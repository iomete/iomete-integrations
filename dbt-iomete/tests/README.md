# Testing dbt-iomete


## Prerequisites

The integration and functional suites run against live IOMETE infrastructure. Rather than
requiring you to set this up by hand, `scripts/ci/run-integration-tests.sh` provisions an isolated test user,
a fresh compute, and full catalog access for the run, then tears it all down afterwards. The suites
connect as that provisioned test user, not as the admin token you supply.

All you need to provide is an **admin token** (`DBT_IOMETE_ADMIN_TOKEN`) for the target environment
(`release.iomete.cloud` by default) and the connection variables below. The admin token must have
the permissions the provisioning step needs (create users, domain roles, catalogs, grant catalogs
to the domain, compute grants, and access policies). The multi-catalog snapshot tests get a
uniquely-named catalog created per run, granted to the domain so the compute can query it, and torn
down afterwards; the built-in `spark_catalog` default is used as-is.

The provisioned test-user credentials (`DBT_IOMETE_TOKEN`, `DBT_IOMETE_USER_NAME`,
`DBT_IOMETE_LAKEHOUSE`, `DBT_IOMETE_ALT_CATALOG`) are written by provisioning to a
`.env.test` file and loaded automatically by `pytest-dotenv` — you never set those yourself.

For the full prerequisite checklist — the exact admin permissions, the platform infrastructure that
must already exist, and what the scripts create and remove — see
[scripts/ci/README.md](../scripts/ci/README.md).

## Set credentials

### Option A: `.env` file (recommended)

Create a `.env` file in the `dbt-iomete/` directory. `pytest-dotenv` loads it automatically

```dotenv
DBT_IOMETE_HOST=release.iomete.cloud
DBT_IOMETE_PORT=443
DBT_IOMETE_HTTPS=true
DBT_IOMETE_DOMAIN=default
DBT_IOMETE_DATAPLANE=spark-resources-1
DBT_IOMETE_ADMIN_TOKEN=<admin-token>
```

### Option B: export environment variables

```bash
export DBT_IOMETE_HOST=release.iomete.cloud
export DBT_IOMETE_PORT=443
export DBT_IOMETE_HTTPS=true
export DBT_IOMETE_DOMAIN=default
export DBT_IOMETE_DATAPLANE=spark-resources-1
export DBT_IOMETE_ADMIN_TOKEN=<admin-token>
```

## Run all DB tests (recommended)

This ensures the required resources exist and the compute is healthy, then runs both the
integration and functional suites, reporting a single aggregated status. It is the same
entrypoint CI uses.

```shell
scripts/ci/run-integration-tests.sh
```

Useful overrides:

```shell
SUITES=integration scripts/ci/run-integration-tests.sh   # run only one suite (integration | functional)
SKIP_PROVISION=1 scripts/ci/run-integration-tests.sh      # skip resource provisioning
```

To only ensure resources (without running tests):

```shell
python scripts/ci/provision.py provision   # create compute + catalog, start, wait
python scripts/ci/provision.py healthcheck  # SELECT 1 against the compute
```

### Iterating on a single test locally

Provision once, then run individual tests directly — no manual env export.
Provisioning writes the test-user credentials to `.env.test`, which `pytest-dotenv`
loads automatically (see `tox.ini`):

```shell
python scripts/ci/provision.py provision
pytest tests/integration/snapshot_validations/test_snapshot.py -k test_snapshot_diff_catalog_schema
python scripts/ci/teardown.py              # when finished
```

## Running without the provisioning scripts (not recommended)

You can point the suites at a pre-existing user, compute, and catalog instead of
provisioning. This skips the isolation and automatic teardown the scripts give
you — the resources are shared and long-lived, so a failed test can leave dirty
state, and you are responsible for access and cleanup. Prefer the provisioned
flow above; use this only when you cannot run provisioning.

Because nothing writes `.env.test`, you supply **all** the suite variables
yourself — including the ones provisioning normally generates — in `.env` (no
admin token is needed):

```dotenv
DBT_IOMETE_HOST=release.iomete.cloud
DBT_IOMETE_PORT=443
DBT_IOMETE_HTTPS=true
DBT_IOMETE_DOMAIN=default
DBT_IOMETE_DATAPLANE=spark-resources-1
DBT_IOMETE_USER_NAME=<existing-user>
DBT_IOMETE_TOKEN=<that-user's-token>
DBT_IOMETE_LAKEHOUSE=<existing-compute>
DBT_IOMETE_ALT_CATALOG=<existing-catalog>   # for the multi-catalog snapshot tests
```

The user must already have full access to both `spark_catalog` and the catalog
named by `DBT_IOMETE_ALT_CATALOG`, and that catalog must exist **and be granted
to `DBT_IOMETE_DOMAIN`** (a catalog the domain has not been granted is invisible
to the compute). The snapshot tests default to `test_dbt_multi_catalog` if the
variable is unset. Then run the suites directly, skipping provisioning and
teardown:

```shell
SKIP_PROVISION=1 scripts/ci/run-integration-tests.sh   # both suites
tox -e integration-iomete                           # or a single suite directly
```

### Run a single suite

Once credentials are in place — either provisioned (`.env.test`) or supplied
manually as above — run one suite directly by its tox environment:

```shell
tox -e integration-iomete   # integration suite
tox -e functional           # functional suite (uses the dbt adapter test framework)
```

## Run unit tests

Unit tests need no IOMETE resources or credentials, so they run on their own:

```shell
tox -e unit
```
