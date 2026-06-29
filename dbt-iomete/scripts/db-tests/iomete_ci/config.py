"""Provisioning constants, connection config, and filesystem paths.

Everything here is sourced from ``DBT_IOMETE_*`` environment variables (see
tests/README.md). Paths are resolved relative to this package so the scripts work
regardless of the caller's working directory.
"""
from __future__ import annotations

import os
from dataclasses import dataclass

from .errors import ProvisionError

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

# scripts/db-tests/ — one level above this package.
_DB_TESTS_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# dbt-iomete/ — two levels above scripts/db-tests/ (where .env lives).
_DBT_DIR = os.path.dirname(os.path.dirname(_DB_TESTS_DIR))

# The state file lives alongside the scripts, in scripts/db-tests/.
DEFAULT_STATE_FILE = os.path.join(_DB_TESTS_DIR, ".provision-state.json")


def load_dotenv() -> None:
    """Best-effort load of dbt-iomete/.env for standalone runs (CI sets env directly).

    Only fills variables that are not already set, so an explicit environment always
    wins. The bash runner does the same; this keeps direct ``python`` invocation usable.
    """
    env_path = os.path.join(_DBT_DIR, ".env")
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
