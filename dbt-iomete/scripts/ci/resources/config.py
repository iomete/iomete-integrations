"""Provisioning constants, connection config, and filesystem paths.

Everything here is sourced from ``DBT_IOMETE_*`` environment variables (see tests/README.md).
"""

from __future__ import annotations

import os
from dataclasses import dataclass

from .errors import ProvisionError

# spark_catalog is the built-in default catalog (never created or deleted).
DEFAULT_CATALOG = "spark_catalog"

COMPUTE_ACTIVE_STATUS = "ACTIVE"

# COMPUTE asset-type permissions that let a user operate a specific compute, and
# the NAMESPACE:USE that lets them use the data-plane it runs in. Both are granted
# on the namespace bundle.
COMPUTE_PERMS = ("VIEW", "UPDATE", "DELETE", "EXECUTE", "CONSUME")

# Domain-level rights release exposes as role permissions: creating a compute
# (service=lakehouse) and minting a personal access token (service=access_token).
# The test user needs both.
_ROLE_ACTIONS = [
    {"action": a, "resources": []} for a in ("list", "create", "view", "manage")
]
ROLE_PERMISSIONS = [
    {"service": "lakehouse", "actions": _ROLE_ACTIONS},
    {"service": "access_token", "actions": _ROLE_ACTIONS},
]

FULL_ACCESS = "ALL"
PRIORITY_NORMAL = "NORMAL"

# Lifetime of the test user's personal access token. It only needs to outlive a
# single test run; the token is also removed when the user is deleted at teardown.
TOKEN_EXPIRATION_DAYS = 1

# scripts/ci/ — one level above this package.
_DB_TESTS_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# dbt-iomete/ — two levels above scripts/ci/ (where .env lives).
_DBT_DIR = os.path.dirname(os.path.dirname(_DB_TESTS_DIR))

# The state file lives alongside the scripts, in scripts/ci/.
DEFAULT_STATE_FILE = os.path.join(_DB_TESTS_DIR, ".provision-state.json")

# Per-run test-user credentials loaded by pytest-dotenv.
DEFAULT_TEST_ENV_FILE = os.path.join(_DBT_DIR, ".env.test")

_DOTENV_FILE = os.path.join(_DBT_DIR, ".env")


def read_env_file(path: str) -> dict:
    """Parse a ``KEY=value`` dotenv file into a dict (empty if it does not exist)."""
    values = {}

    if not os.path.isfile(path):
        return values

    with open(path) as handle:
        for line in handle:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, _, value = line.partition("=")
            values[key.strip()] = value.strip()

    return values


def write_env_file(path: str, values: dict) -> None:
    """Write ``values`` as a dotenv file, replacing any existing file atomically."""
    tmp = f"{path}.tmp"

    with open(tmp, "w") as handle:
        handle.write("".join(f"{key}={value}\n" for key, value in values.items()))

    os.replace(tmp, path)


def load_dotenv() -> None:
    """Best-effort load of dbt-iomete/.env for standalone runs (CI sets env directly).

    Only fills variables that are not already set, so an explicit environment always
    wins. The bash runner does the same; this keeps direct ``python`` invocation usable.
    """
    for key, value in read_env_file(_DOTENV_FILE).items():
        os.environ.setdefault(key, value)


@dataclass
class Config:
    """Connection + provisioning settings, sourced from ``DBT_IOMETE_*`` env vars."""

    host: str
    admin_token: str
    domain: str
    namespace: str  # a.k.a. dataplane, e.g. "spark-resources-1"
    port: int
    https: bool

    # Compute-create config.
    driver_node_type: str = "driver-x-small"
    executor_node_type: str = "exec-x-small"
    max_executors: int = 2
    lakehouse_dir_prefix: str = "s3://lakehouse"

    # Wait/timeout tuning.
    active_timeout_seconds: int = 120
    poll_interval_seconds: int = 10

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
            admin_token=required("DBT_IOMETE_ADMIN_TOKEN"),
            domain=required("DBT_IOMETE_DOMAIN"),
            namespace=required("DBT_IOMETE_DATAPLANE"),
            port=int(required("DBT_IOMETE_PORT")),
            https=required("DBT_IOMETE_HTTPS").lower() == "true",
            driver_node_type=os.getenv(
                "DBT_IOMETE_DRIVER_NODE_TYPE", cls.driver_node_type
            ),
            executor_node_type=os.getenv(
                "DBT_IOMETE_EXECUTOR_NODE_TYPE", cls.executor_node_type
            ),
            max_executors=int(
                os.getenv("DBT_IOMETE_MAX_EXECUTORS", str(cls.max_executors))
            ),
            lakehouse_dir_prefix=os.getenv(
                "DBT_IOMETE_LAKEHOUSE_DIR_PREFIX", cls.lakehouse_dir_prefix
            ),
            active_timeout_seconds=int(
                os.getenv(
                    "DBT_IOMETE_ACTIVE_TIMEOUT_SECONDS", str(cls.active_timeout_seconds)
                )
            ),
            poll_interval_seconds=int(
                os.getenv(
                    "DBT_IOMETE_POLL_INTERVAL_SECONDS", str(cls.poll_interval_seconds)
                )
            ),
        )
