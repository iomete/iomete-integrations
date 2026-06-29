#!/usr/bin/env python
"""Provision the isolated IOMETE resources the dbt-iomete test suites need.

The integration and functional suites run as a short-lived *test user* against a
freshly created compute, with full query access to the catalogs the tests use.
This script creates that environment with an admin token and records everything
it creates to a state file so ``iomete_teardown.py`` can remove it afterwards.

The flow (all against the existing ``DBT_IOMETE_DOMAIN`` domain):

1. create a temp user and log it in (its token is what the suites connect with);
2. add the user to the domain;
3. ensure the required catalogs exist (create-if-missing, never deleted);
4. grant the user a domain role that allows creating compute, plus operate
   permissions on the data-plane namespace bundle;
5. create the compute *as the user*, start it, and wait until it is ACTIVE;
6. create a data-security access policy granting the user full access to the
   catalogs;
7. preflight ``SELECT 1`` as the user.

State is written incrementally, so a crash mid-provision still leaves a state
file teardown can act on. The admin token is never written to disk.

Auth: control-plane calls use ``Authorization: Bearer`` with the admin token,
except the compute create/start/status calls, which use the *test user's* token
to prove the grant (not admin privilege) is what authorises them.

Usage::

    python scripts/iomete_provision.py provision   # create resources + write state
    python scripts/iomete_provision.py preflight    # SELECT 1 as the test user
    python scripts/iomete_provision.py all          # provision then preflight (default)

    # --state-file PATH  (default: dbt-iomete/.provision-state.json)
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import secrets
import sys
import time
import urllib.parse
from dataclasses import dataclass
from typing import Optional

import requests

logger = logging.getLogger("iomete_provision")

# Catalogs the suites query. spark_catalog is the built-in default catalog (never
# created or deleted); the second is exercised by the multi-catalog snapshot tests.
REQUIRED_CATALOGS = ("spark_catalog", "test_dbt_multi_catalog")

COMPUTE_ACTIVE_STATUS = "ACTIVE"

# COMPUTE asset-type permissions that let a user operate a specific compute, and
# the NAMESPACE:USE that lets them use the data-plane it runs in. Both are granted
# on the namespace bundle (GET /api/v1/bundles/asset-types/permissions).
COMPUTE_PERMS = ("VIEW", "UPDATE", "DELETE", "EXECUTE", "CONSUME")

# Domain-level rights release exposes as role permissions (not bundle asset
# types): creating a compute (service=lakehouse) and minting a personal access
# token (service=access_token). The test user needs both.
_ROLE_ACTIONS = [{"action": a, "resources": []} for a in ("list", "create", "view", "manage")]
ROLE_PERMISSIONS = [
    {"service": "lakehouse", "actions": _ROLE_ACTIONS},
    {"service": "access_token", "actions": _ROLE_ACTIONS},
]

FULL_ACCESS = "ALL"
PRIORITY_NORMAL = "NORMAL"

# Lifetime of the test user's personal access token. It only needs to outlive a
# single test run; the token is also removed when the user is deleted at teardown.
TOKEN_EXPIRATION_DAYS = 1

DEFAULT_STATE_FILE = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".provision-state.json"
)


class ProvisionError(RuntimeError):
    """A resource could not be ensured and tests would fail without it."""


def _load_dotenv() -> None:
    """Best-effort load of dbt-iomete/.env for standalone runs (CI sets env directly).

    Only fills variables that are not already set, so an explicit environment always
    wins. The bash runner does the same; this keeps direct ``python`` invocation usable.
    """
    env_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env")
    if not os.path.isfile(env_path):
        return
    with open(env_path) as handle:
        for line in handle:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            os.environ.setdefault(key.strip(), value.strip())


@dataclass
class Config:
    """Connection + provisioning settings, sourced from ``DBT_IOMETE_*`` env vars."""

    host: str
    token: str  # admin token, used as Bearer for control-plane calls
    domain: str
    namespace: str  # a.k.a. dataplane, e.g. "spark-resources-1"
    port: int
    https: bool
    catalogs: tuple = REQUIRED_CATALOGS

    # Compute-create knobs.
    driver_node_type: str = "driver-x-small"
    executor_node_type: str = "exec-x-small"
    max_executors: int = 2
    lakehouse_dir_prefix: str = "s3://lakehouse"

    # Wait/timeout tuning.
    active_timeout: int = 600
    poll_interval: int = 10

    @property
    def scheme(self) -> str:
        return "https" if self.https else "http"

    @property
    def base_url(self) -> str:
        return f"{self.scheme}://{self.host}"

    @classmethod
    def from_env(cls) -> "Config":
        def required(name: str) -> str:
            value = os.getenv(name)
            if not value:
                raise ProvisionError(
                    f"Environment variable {name} is not set. Populate dbt-iomete/.env "
                    f"or export it (see tests/README.md)."
                )
            return value

        return cls(
            host=required("DBT_IOMETE_HOST"),
            token=required("DBT_IOMETE_TOKEN"),
            domain=os.getenv("DBT_IOMETE_DOMAIN", "default"),
            namespace=required("DBT_IOMETE_DATAPLANE"),
            port=int(os.getenv("DBT_IOMETE_PORT", "443")),
            https=os.getenv("DBT_IOMETE_HTTPS", "true").lower() == "true",
            driver_node_type=os.getenv("DBT_IOMETE_DRIVER_NODE_TYPE", "driver-x-small"),
            executor_node_type=os.getenv("DBT_IOMETE_EXECUTOR_NODE_TYPE", "exec-x-small"),
            max_executors=int(os.getenv("DBT_IOMETE_MAX_EXECUTORS", "2")),
            lakehouse_dir_prefix=os.getenv("DBT_IOMETE_LAKEHOUSE_DIR_PREFIX", "s3://lakehouse"),
            active_timeout=int(os.getenv("DBT_IOMETE_ACTIVE_TIMEOUT", "600")),
            poll_interval=int(os.getenv("DBT_IOMETE_POLL_INTERVAL", "10")),
        )


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


class ProvisionState:
    """The set of resources a provision run created, persisted incrementally.

    Written after every successful create so an interrupted run still leaves a
    file ``iomete_teardown.py`` can act on. The admin token is never stored; the
    test user's token is, since the suites need it (the file is gitignored).
    """

    def __init__(self, path: str, domain: str):
        self.path = path
        self.data = {"domain": domain, "created": {}, "test_env": {}}
        self.flush()

    def set_created(self, **kwargs) -> None:
        self.data["created"].update({k: v for k, v in kwargs.items() if v is not None})
        self.flush()

    def set_test_env(self, **kwargs) -> None:
        self.data["test_env"].update({k: v for k, v in kwargs.items() if v is not None})
        self.flush()

    def flush(self) -> None:
        tmp = f"{self.path}.tmp"
        with open(tmp, "w") as handle:
            json.dump(self.data, handle, indent=2)
        os.replace(tmp, self.path)


def provision(config: Config, state_path: str) -> ProvisionState:
    client = IometeClient(config)
    state = ProvisionState(state_path, config.domain)

    suffix = secrets.token_hex(4)
    username = f"dbtci-{suffix}"
    compute_name = f"dbtci-{suffix}"
    role_name = f"dbtci-role-{suffix}"
    policy_name = f"dbtci-grant-{suffix}"

    password = client.create_user(username)
    state.set_created(username=username)
    user_token = client.login_user(username, password)
    logger.info("Created test user %r", username)

    membership_id = client.add_domain_member(username)
    state.set_created(membership_id=membership_id)

    client.ensure_catalogs()

    # Grant the domain role (create compute + mint tokens) and namespace operate
    # rights, then let the permission caches settle before the user acts on them.
    client.grant_create_role(username, role_name)
    state.set_created(role_name=role_name)

    ns_bundle_id = client.resolve_namespace_bundle()
    client.grant_compute_perms(username, ns_bundle_id)
    state.set_created(ns_bundle_id=ns_bundle_id)
    time.sleep(5)

    # The OAuth token drove the control-plane calls above; the suites and the
    # preflight authenticate to the compute's thrift gateway with a PAT instead.
    pat = client.create_access_token(user_token, f"dbtci-pat-{suffix}")
    state.set_test_env(
        DBT_IOMETE_TOKEN=pat,
        DBT_IOMETE_USER_NAME=username,
        DBT_IOMETE_DOMAIN=config.domain,
    )

    compute_id = client.create_compute(compute_name, user_token, ns_bundle_id)
    state.set_created(compute_id=compute_id, compute_name=compute_name)
    state.set_test_env(DBT_IOMETE_LAKEHOUSE=compute_name)
    client.start_compute(compute_id, user_token)
    client.wait_compute_active(compute_id, user_token)

    policy_id = client.create_access_policy(policy_name, username, config.catalogs)
    state.set_created(policy_id=policy_id)

    logger.info("Provisioning complete. State written to %s", state_path)
    return state


def _read_state(state_path: str) -> dict:
    if not os.path.isfile(state_path):
        raise ProvisionError(
            f"State file {state_path} not found. Run `provision` before `preflight`."
        )
    with open(state_path) as handle:
        return json.load(handle)


def preflight(config: Config, state_path: str) -> None:
    """Open a real connection as the test user and run ``SELECT 1``.

    This is the closest check to what the suites do: it proves the test user can
    reach the compute and query through the same thrift path the dbt adapter uses.
    """
    state = _read_state(state_path)
    env = state.get("test_env", {})
    username = env.get("DBT_IOMETE_USER_NAME")
    token = env.get("DBT_IOMETE_TOKEN")  # the test user's personal access token
    compute = env.get("DBT_IOMETE_LAKEHOUSE")
    if not (username and token and compute):
        raise ProvisionError(f"State file {state_path} is missing test-user connection details.")

    try:
        from pyhive import hive
    except ImportError:
        logger.warning("pyhive not importable; falling back to a control-plane reachability check.")
        IometeClient(config).list_catalog_names()
        logger.info("Control-plane reachable; skipping thrift preflight.")
        return

    logger.info("Preflight: connecting to compute %r as %r and running SELECT 1", compute, username)

    def attempt() -> None:
        conn = hive.connect(
            scheme=config.scheme,
            host=config.host,
            port=config.port,
            lakehouse=compute,
            database=config.catalogs[0],
            username=username,
            password=token,
            data_plane=config.namespace,
        )
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            result = cursor.fetchall()
            if not result or result[0][0] != 1:
                raise ProvisionError(f"Preflight SELECT 1 returned unexpected result: {result!r}")
        finally:
            conn.close()

    # driverStatus can report ACTIVE a little before the thrift gateway accepts
    # sessions, so give the warmup a few tries before declaring failure.
    deadline = time.time() + config.active_timeout
    while True:
        try:
            attempt()
            logger.info("Preflight OK: the test user can query the compute.")
            return
        except Exception as exc:  # noqa: BLE001 - retry any connect/query error during warmup
            if time.time() >= deadline:
                raise ProvisionError(f"Preflight failed after warmup retries: {exc}") from exc
            logger.info("Preflight not ready yet (%s); retrying ...", type(exc).__name__)
            time.sleep(config.poll_interval)


def main(argv: Optional[list] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "command",
        nargs="?",
        default="all",
        choices=["provision", "preflight", "all"],
        help="provision: create resources; preflight: SELECT 1 as the test user; all: both (default)",
    )
    parser.add_argument(
        "--state-file",
        default=DEFAULT_STATE_FILE,
        help=f"where to read/write the provision state (default: {DEFAULT_STATE_FILE})",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s [provision] %(levelname)s %(message)s"
    )

    try:
        _load_dotenv()
        config = Config.from_env()
        if args.command in ("provision", "all"):
            provision(config, args.state_file)
        if args.command in ("preflight", "all"):
            preflight(config, args.state_file)
    except ProvisionError as exc:
        logger.error("Provisioning failed: %s", exc)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
