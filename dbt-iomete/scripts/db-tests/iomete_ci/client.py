"""IOMETE control-plane client used by the provisioning flow.

One cohesive HTTP client, grouped by the resources it touches: identity, domain
membership, domain roles, namespace bundles, catalogs, compute, and data-security
access policies. Admin calls use the admin Bearer token; calls that must run as
the test user take an explicit ``token``.
"""
from __future__ import annotations

import json
import logging
import time
import urllib.parse
from typing import Optional

import requests

from .config import (
    COMPUTE_ACTIVE_STATUS,
    COMPUTE_PERMS,
    FULL_ACCESS,
    PRIORITY_NORMAL,
    ROLE_PERMISSIONS,
    TOKEN_EXPIRATION_DAYS,
    Config,
)
from .errors import ProvisionError

logger = logging.getLogger(__name__)


class IometeClient:
    """Control-plane client. Admin calls use the admin Bearer token; pass an
    explicit ``token`` for calls that must run as the test user (compute)."""

    def __init__(self, config: Config):
        self.config = config
        self.session = requests.Session()

    def _call(
        self,
        method: str,
        path: str,
        *,
        token: Optional[str] = None,
        json_body=None,
        form: Optional[dict] = None,
        ok=(200, 201, 204),
        accept: str = "application/json",
        tolerate_404: bool = False,
        timeout: int = 60,
    ):
        """One control-plane call. ``token`` defaults to the admin token.

        ``path`` may be a full URL or a path joined onto ``base_url``.
        """
        url = path if path.startswith("http") else f"{self.config.base_url}{path}"
        headers = {"Accept": accept}
        bearer = self.config.token if token is None else token
        if bearer:
            headers["Authorization"] = f"Bearer {bearer}"
        data = None
        if json_body is not None:
            headers["Content-Type"] = "application/json"
            data = json.dumps(json_body)
        elif form is not None:
            headers["Content-Type"] = "application/x-www-form-urlencoded"
            data = urllib.parse.urlencode(form)
        response = self.session.request(method, url, headers=headers, data=data, timeout=timeout)
        if tolerate_404 and response.status_code == 404:
            return None
        if response.status_code not in ok:
            raise ProvisionError(f"{method} {url} -> {response.status_code}: {response.text[:500]}")
        if not response.content:
            return None
        try:
            return response.json()
        except ValueError:
            return response.text

    # --- identity ----------------------------------------------------------

    def create_user(self, username: str) -> str:
        body = self._call(
            "POST",
            "/api/v1/admin/identity/users",
            json_body={
                "username": username,
                "email": f"{username}@example.com",
                "firstName": "dbt",
                "lastName": "CI",
            },
        ) or {}
        password = body.get("temporaryPassword") or body.get("temporary_password")
        if not password:
            raise ProvisionError(f"create_user({username!r}) returned no temporary password: {body}")
        return password

    def login_user(self, username: str, password: str) -> str:
        """Exchange the temp password for the user's own token (unauthenticated, form-encoded)."""
        body = self._call(
            "POST",
            "/api/v1/identity/auth/token",
            token="",  # the credentials are the body; no bearer header
            form={
                "username": username,
                "password": password,
                "grant_type": "password",
                "client_id": "iomete-cli",
            },
        ) or {}
        token = body.get("access_token")
        if not token:
            raise ProvisionError(f"login_user({username!r}) returned no access_token.")
        return token

    def create_access_token(self, user_token: str, name: str) -> str:
        """Mint a personal access token for the test user (run as that user).

        The OAuth token from ``login_user`` authorises control-plane calls but is
        not accepted by the compute's thrift gateway; queries authenticate with a
        personal access token instead (the same token kind the dbt adapter uses).
        """
        body = self._call(
            "POST",
            f"/api/v1/domains/{self.config.domain}/auth/tokens",
            token=user_token,
            json_body={"name": name, "expirationDays": TOKEN_EXPIRATION_DAYS},
        ) or {}
        token = body.get("token")
        if not token:
            raise ProvisionError(f"create_access_token({name!r}) returned no token: {body}")
        return token

    def delete_user(self, username: str) -> None:
        # SCIM delete: no body, scim+json Accept, 404-tolerant.
        self._call(
            "DELETE",
            f"/api/v1/admin/identity/scim/v2/Users/{username}",
            accept="application/scim+json",
            ok=(200, 204),
            tolerate_404=True,
        )

    # --- domain membership -------------------------------------------------

    def add_domain_member(self, username: str) -> Optional[str]:
        self._call(
            "POST",
            f"/api/v1/admin/domains/{self.config.domain}/members",
            json_body=[{"type": "USER", "value": username}],
        )
        query = urllib.parse.urlencode({"searchText": username, "type": "USER", "size": 50})
        page = self._call(
            "GET", f"/api/v1/admin/domains/{self.config.domain}/members?{query}"
        ) or {}
        items = page.get("items") if isinstance(page, dict) else page
        for member in items or []:
            if (member.get("identity") or {}).get("value") == username:
                return str(member.get("id"))
        return None

    def remove_domain_member(self, membership_id: str) -> None:
        self._call(
            "DELETE",
            f"/api/v1/admin/domains/{self.config.domain}/members/{membership_id}",
            ok=(200, 204),
            tolerate_404=True,
        )

    # --- domain role (create-compute right) --------------------------------

    def grant_create_role(self, username: str, role_name: str) -> None:
        self._call(
            "POST",
            f"/api/v1/domains/{self.config.domain}/roles",
            json_body={
                "name": role_name,
                "description": "dbt-iomete CI: let the test user create compute and tokens",
                "permissions": ROLE_PERMISSIONS,
            },
        )
        self._call(
            "POST",
            f"/api/v1/domains/{self.config.domain}/members/USER/{username}/roles",
            json_body=[role_name],
        )

    def revoke_create_role(self, username: str, role_name: str) -> None:
        self._call(
            "DELETE",
            f"/api/v1/domains/{self.config.domain}/members/USER/{username}/roles",
            json_body=[role_name],
            ok=(200, 204),
            tolerate_404=True,
        )
        self._call(
            "DELETE",
            f"/api/v1/domains/{self.config.domain}/roles/{role_name}",
            ok=(200, 204),
            tolerate_404=True,
        )

    # --- namespace bundle + operate grant ----------------------------------

    def resolve_namespace_bundle(self) -> str:
        body = self._call("GET", f"/api/v2/domains/{self.config.domain}/namespaces/list") or []
        items = body if isinstance(body, list) else body.get("items", [])
        match = next((n for n in items if n.get("name") == self.config.namespace), None)
        if not match:
            valid = [n.get("name") for n in items]
            raise ProvisionError(
                f"Namespace {self.config.namespace!r} not found in domain "
                f"{self.config.domain!r}; visible namespaces: {valid}"
            )
        bundle_id = (match.get("bundle") or {}).get("id")
        if not bundle_id:
            raise ProvisionError(f"Namespace {self.config.namespace!r} has no bundle id.")
        return bundle_id

    def grant_compute_perms(self, username: str, ns_bundle_id: str) -> None:
        self._call(
            "POST",
            f"/api/v1/bundles/{ns_bundle_id}/permissions",
            json_body={
                "COMPUTE": {"users": {username: list(COMPUTE_PERMS)}, "groups": {}},
                "NAMESPACE": {"users": {username: ["USE"]}, "groups": {}},
            },
        )

    def revoke_bundle_actor(self, username: str, ns_bundle_id: str) -> None:
        self._call(
            "DELETE",
            f"/api/v1/bundles/{ns_bundle_id}/permissions",
            json_body={"actorType": "USER", "actorId": username},
            ok=(200, 204),
            tolerate_404=True,
        )

    # --- catalogs ----------------------------------------------------------

    def list_catalog_names(self) -> set:
        # Global admin list is authoritative: the domain-scoped list only shows
        # catalogs already granted to the domain, which breaks create-if-missing.
        data = self._call("GET", "/api/v1/admin/spark/settings/catalogs") or {}
        items = data.get("items") if isinstance(data, dict) else data
        return {c.get("name") for c in (items or [])}

    def create_catalog(self, name: str) -> None:
        logger.info("Creating catalog %r", name)
        self._call(
            "POST",
            "/api/v1/admin/spark/settings/catalogs",
            json_body={
                "name": name,
                "type": "iceberg",
                "lakehouseDir": f"{self.config.lakehouse_dir_prefix}/{name}",
            },
        )

    def ensure_catalogs(self) -> None:
        existing = self.list_catalog_names()
        for name in self.config.catalogs:
            if name in existing:
                logger.info("Catalog %r already exists.", name)
                continue
            self.create_catalog(name)
        logger.info("All required catalogs present: %s", ", ".join(self.config.catalogs))

    # --- compute (v2, created as the test user) ----------------------------

    def create_compute(self, name: str, user_token: str, ns_bundle_id: str) -> str:
        payload = {
            "name": name,
            "namespace": self.config.namespace,
            "bundleId": ns_bundle_id,  # required, or the API returns 400
            "nodeTypes": {
                "driver": self.config.driver_node_type,
                "executor": self.config.executor_node_type,
                "minExecutors": 0,
                "maxExecutors": self.config.max_executors,
                "singleNodeDeployment": False,
            },
            "autoScale": {"enabled": True, "idleTimeoutInSeconds": 1800},
            "sparkConfig": {
                "arguments": [],
                "deps": {"jars": [], "files": [], "pyFiles": [], "packages": []},
            },
        }
        logger.info("Creating compute %r as the test user", name)
        self._call(
            "POST", f"/api/v2/domains/{self.config.domain}/compute", token=user_token, json_body=payload
        )
        # Create can return an empty body and indexes asynchronously; resolve the
        # id by listing (also confirms the user can see its own compute).
        for _ in range(30):
            listing = self._call(
                "GET", f"/api/v2/domains/{self.config.domain}/compute", token=user_token
            ) or {}
            items = listing.get("items") if isinstance(listing, dict) else listing
            match = next((c for c in (items or []) if c.get("name") == name), None)
            if match:
                return match["id"]
            time.sleep(1)
        raise ProvisionError(f"Compute {name!r} did not appear after creation.")

    def start_compute(self, compute_id: str, user_token: str) -> None:
        # Create auto-starts, so a redundant start returns 409 (already running).
        self._call(
            "POST",
            f"/api/v2/domains/{self.config.domain}/compute/{compute_id}/start",
            token=user_token,
            ok=(200, 201, 204, 409),
        )

    def get_compute_status(self, compute_id: str, token: str) -> str:
        data = self._call(
            "GET", f"/api/v2/domains/{self.config.domain}/compute/{compute_id}", token=token
        ) or {}
        item = data.get("item") if isinstance(data, dict) and "item" in data else data
        return (item or {}).get("driverStatus", "UNKNOWN")

    def wait_compute_active(self, compute_id: str, user_token: str) -> None:
        deadline = time.time() + self.config.active_timeout
        while True:
            status = self.get_compute_status(compute_id, user_token)
            if status == COMPUTE_ACTIVE_STATUS:
                logger.info("Compute is ACTIVE.")
                return
            if time.time() >= deadline:
                raise ProvisionError(
                    f"Compute did not become {COMPUTE_ACTIVE_STATUS} within "
                    f"{self.config.active_timeout}s (last status: {status})."
                )
            logger.info("Compute status=%s; waiting ...", status)
            time.sleep(self.config.poll_interval)

    def delete_compute(self, compute_id: str) -> None:
        self._call(
            "DELETE",
            f"/api/v2/domains/{self.config.domain}/compute/{compute_id}",
            ok=(200, 204),
            tolerate_404=True,
        )

    # --- data-security access policy ---------------------------------------

    @staticmethod
    def _catalog_resource(catalog: str) -> dict:
        # The catalog is encoded into the database name as "<catalog>.*"; there is
        # no dedicated catalog field (mirrors the platform's built-in policy).
        return {
            "databases": [f"{catalog}.*"],
            "databasesInclusionType": "INCLUDE",
            "tables": ["*"],
            "tablesInclusionType": "INCLUDE",
            "columns": ["*"],
            "columnsInclusionType": "INCLUDE",
        }

    def create_access_policy(self, name: str, username: str, catalogs) -> int:
        body = {
            "name": name,
            "priority": PRIORITY_NORMAL,
            "allowPolicyItems": [
                {"users": [username], "groups": [], "roles": [], "accesses": [FULL_ACCESS]}
            ],
            "denyPolicyItems": None,
            "resources": [self._catalog_resource(c) for c in catalogs],
        }
        logger.info("Creating access policy %r granting %r %s on %s", name, username, FULL_ACCESS, list(catalogs))
        created = self._call("POST", "/api/v1/admin/data-security/access/policy", json_body=body)
        if isinstance(created, dict) and created.get("id") is not None:
            return int(created["id"])
        policy_id = self.find_policy_id(name)
        if policy_id is None:
            raise ProvisionError(f"Access policy {name!r} not found after creation.")
        return policy_id

    def find_policy_id(self, name: str) -> Optional[int]:
        for policy in self._call("GET", "/api/v1/admin/data-security/access/policy") or []:
            if policy.get("name") == name:
                return int(policy["id"])
        return None

    def delete_access_policy(self, policy_id: int) -> None:
        self._call(
            "DELETE",
            f"/api/v1/admin/data-security/access/policy/{policy_id}",
            ok=(200, 204),
            tolerate_404=True,
        )
