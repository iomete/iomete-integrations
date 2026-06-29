"""
IOMETE control-plane client used by the provisioning flow.
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
    """Small wrapper around IOMETE control-plane APIs.

    Calls use the admin token by default. Pass ``token`` when an API request
    must run as the temporary test user, such as compute or PAT operations.
    """

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

        response = self.session.request(
            method, url, headers=headers, data=data, timeout=timeout
        )

        if tolerate_404 and response.status_code == 404:
            return None
        if response.status_code not in ok:
            raise ProvisionError(
                f"{method} {url} -> {response.status_code}: {response.text[:500]}"
            )
        if not response.content:
            return None
        try:
            return response.json()
        except ValueError:
            return response.text

    # --- identity ----------------------------------------------------------

    def create_user(self, username: str) -> str:
        body = (
            self._call(
                "POST",
                "/api/v1/admin/identity/users",
                json_body={
                    "username": username,
                    "email": f"{username}@example.com",
                    "firstName": "DBT",
                    "lastName": "CI",
                },
            )
            or {}
        )

        password = body.get("temporaryPassword")

        if not password:
            raise ProvisionError(
                f"create_user({username!r}) returned no temporary password: {body}"
            )

        return password

    def login_user(self, username: str, password: str) -> str:
        body = (
            self._call(
                "POST",
                "/api/v1/identity/auth/token",
                token="",  # the credentials are the body; no bearer header
                form={
                    "username": username,
                    "password": password,
                    "grant_type": "password",
                    "client_id": "iomete-cli",
                },
            )
            or {}
        )

        token = body.get("access_token")

        if not token:
            raise ProvisionError(f"login_user({username!r}) returned no access_token.")

        return token

    def create_access_token(self, user_token: str, name: str) -> str:
        body = (
            self._call(
                "POST",
                f"/api/v1/domains/{self.config.domain}/auth/tokens",
                token=user_token,
                json_body={"name": name, "expirationDays": TOKEN_EXPIRATION_DAYS},
            )
            or {}
        )
        token = body.get("token")
        if not token:
            raise ProvisionError(
                f"create_access_token({name!r}) returned no token: {body}"
            )
        return token

    def delete_user(self, username: str) -> None:
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

        # Get membership id
        query = urllib.parse.urlencode(
            {"searchText": username, "type": "USER", "size": 50}
        )
        page = (
            self._call(
                "GET", f"/api/v1/admin/domains/{self.config.domain}/members?{query}"
            )
            or {}
        )

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
        body = (
            self._call("GET", f"/api/v2/domains/{self.config.domain}/namespaces/list")
            or []
        )
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
            raise ProvisionError(
                f"Namespace {self.config.namespace!r} has no bundle id."
            )

        return bundle_id

    def grant_bundle_perms(self, username: str, ns_bundle_id: str) -> None:
        self._call(
            "POST",
            f"/api/v1/bundles/{ns_bundle_id}/permissions",
            json_body={
                "COMPUTE": {"users": {username: list(COMPUTE_PERMS)}, "groups": {}},
                "NAMESPACE": {"users": {username: ["USE"]}, "groups": {}},
            },
        )

    def revoke_bundle_perms(self, username: str, ns_bundle_id: str) -> None:
        self._call(
            "DELETE",
            f"/api/v1/bundles/{ns_bundle_id}/permissions",
            json_body={"actorType": "USER", "actorId": username},
            ok=(200, 204),
            tolerate_404=True,
        )

    # --- catalogs ----------------------------------------------------------

    def catalog_exists(self, name: str) -> bool:
        data = self._call(
            "GET",
            f"/api/v1/admin/spark/settings/catalogs/{name}",
            tolerate_404=True,
        )

        return bool(data)

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

        if not self.catalog_exists(name):
            raise ProvisionError(f"Catalog {name!r} did not appear after creation.")

        self.attach_catalog_to_domain(name)

    def attach_catalog_to_domain(self, name: str) -> None:
        domain = self.config.domain
        logger.info("Attaching catalog %r to domain %r", name, domain)

        self._call(
            "POST",
            "/api/v1/admin/spark/settings/catalogs/permissions/bulk",
            json_body=[{"catalogName": name, "domains": [domain]}],
        )

        granted = (
            self._call(
                "GET",
                f"/api/v1/admin/spark/settings/catalogs/{name}/permissions",
            )
            or []
        )

        if domain not in granted:
            raise ProvisionError(
                f"Catalog {name!r} not attached to domain {domain!r} after grant; "
                f"current domains: {granted}"
            )

    def delete_catalog(self, name: str) -> None:
        logger.info("Deleting catalog %r", name)

        self._call(
            "DELETE",
            f"/api/v1/admin/spark/settings/catalogs/{name}",
            ok=(200, 204),
            tolerate_404=True,
        )

    # --- compute (v2) ----------------------------------------------------

    def create_compute(self, name: str, user_token: str, ns_bundle_id: str) -> str:
        payload = {
            "name": name,
            "namespace": self.config.namespace,
            "bundleId": ns_bundle_id,
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

        logger.info("Creating compute cluster - %r ", name)

        self._call(
            "POST",
            f"/api/v2/domains/{self.config.domain}/compute",
            token=user_token,
            json_body=payload,
        )

        # Create can return an empty body and indexes asynchronously; resolve the
        # id by listing (also confirms the user can see its own compute).
        for _ in range(30):
            listing = (
                self._call(
                    "GET",
                    f"/api/v2/domains/{self.config.domain}/compute",
                    token=user_token,
                )
                or {}
            )

            items = listing.get("items") if isinstance(listing, dict) else listing
            match = next((c for c in (items or []) if c.get("name") == name), None)

            if match:
                return match["id"]

            time.sleep(1)

        raise ProvisionError(f"Compute {name!r} did not appear after creation.")

    def get_compute_status(self, compute_id: str, token: str) -> str:
        data = (
            self._call(
                "GET",
                f"/api/v2/domains/{self.config.domain}/compute/{compute_id}",
                token=token,
            )
            or {}
        )
        item = data.get("item") if isinstance(data, dict) and "item" in data else data
        return (item or {}).get("driverStatus", "UNKNOWN")

    def wait_compute_active(self, compute_id: str, user_token: str) -> None:
        deadline = time.time() + self.config.active_timeout_seconds

        while True:
            status = self.get_compute_status(compute_id, user_token)

            if status == COMPUTE_ACTIVE_STATUS:
                logger.info("Compute is ACTIVE.")
                return

            if time.time() >= deadline:
                raise ProvisionError(
                    f"Compute did not become {COMPUTE_ACTIVE_STATUS} within "
                    f"{self.config.active_timeout_seconds}s (last status: {status})."
                )

            logger.info("Compute status=%s; waiting ...", status)

            time.sleep(self.config.poll_interval_seconds)

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
        return {
            "databases": [f"{catalog}.*"],
            "databasesInclusionType": "INCLUDE",
            "tables": ["*"],
            "tablesInclusionType": "INCLUDE",
            "columns": ["*"],
            "columnsInclusionType": "INCLUDE",
        }

    def create_full_access_policy(self, name: str, username: str, catalogs) -> int:
        body = {
            "name": name,
            "priority": PRIORITY_NORMAL,
            "allowPolicyItems": [
                {
                    "users": [username],
                    "groups": [],
                    "roles": [],
                    "accesses": [FULL_ACCESS],
                }
            ],
            "denyPolicyItems": None,
            "resources": [self._catalog_resource(c) for c in catalogs],
        }

        logger.info(
            "Creating access policy %r granting %r %s on %s",
            name,
            username,
            FULL_ACCESS,
            list(catalogs),
        )

        created = self._call(
            "POST", "/api/v1/admin/data-security/access/policy", json_body=body
        )

        if isinstance(created, dict) and created.get("id") is not None:
            return int(created["id"])

        raise ProvisionError(f"Access policy {name!r} not found after creation.")

    def delete_access_policy(self, policy_id: int) -> None:
        self._call(
            "DELETE",
            f"/api/v1/admin/data-security/access/policy/{policy_id}",
            ok=(200, 204),
            tolerate_404=True,
        )
