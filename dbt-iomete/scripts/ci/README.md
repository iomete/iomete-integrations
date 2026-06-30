# dbt-iomete test provisioning

The integration and functional suites need live IOMETE infrastructure: a running
compute and the catalogs to tests query. The scripts in this directory create
that infrastructure on demand, run the tests against it as an isolated test user,
and then remove everything the run created.

Use `run-integration-tests.sh` for normal runs. It provisions the test
infrastructure, runs the suites, and tears everything down even if a test fails.
If the resources are already provisioned and you only want to run the suites,
set `SKIP_PROVISION=1`. To keep the resources after the run, for example to
debug against the live compute, set `KEEP_RESOURCES=1`.

Under the hood that's a three-step lifecycle:

1. **Provision** (`provision.py`) — create the test user and resources,
   then emit the temporary credentials the test run needs.
2. **Run the suites** — run tox as the temporary user against the
   provisioned compute.
3. **Tear down** (`teardown.py`) — delete everything the provision step
   created. This runs even when the tests fail.

This document describes what must already exist before you run the scripts. If
you are the person preparing an environment (or a token) for CI, this is your
checklist.

## Who runs this, and as whom

Provisioning runs with an **admin token**. It does not run the tests as the
admin. Instead it creates a short-lived test user, grants that user exactly the
permissions the suites need, and the **suites connect as the test user**. This
keeps the admin token out of the test path and makes each CI run self-contained.

A subtle but important point about compute: the admin token is only used to
**grant the test user permission to create and use compute**. The compute itself
is then created and owned by the test user. The admin never owns test compute.

## Prerequisites

### 1. An admin token (`DBT_IOMETE_ADMIN_TOKEN`)

The token must belong to a principal with administrative rights in the target
domain. Concretely, it must be able to:

| Area          | Operations the token must be allowed to perform                       |
| ------------- | -------------------------------------------------------------------- |
| Identity      | Create a user, and delete a user (SCIM)                              |
| Domain        | Add and remove domain members                                       |
| Domain roles  | Create, assign, and delete domain roles (this is how the test user is granted the rights to create compute and mint a token) |
| Bundles       | Grant and revoke namespace-bundle permissions (compute + namespace use) |
| Spark settings| Create and delete a catalog, and grant a catalog to the domain      |
| Data security | Create and delete access policies                                   |

If any of these are missing the provision step fails fast with the specific
operation that was denied.

### 2. Platform infrastructure that must already exist

The scripts assume the environment itself is set up. They do **not** create any
of the following — these are infrastructure concerns owned by whoever operates
the IOMETE deployment:

- The IOMETE environment, reachable at `DBT_IOMETE_HOST` on the configured port.
- The **domain** named by `DBT_IOMETE_DOMAIN` (default `default`).
- The **dataplane / namespace** named by `DBT_IOMETE_DATAPLANE` (default
  `spark-resources-1`), running and attached to that domain. The scripts grant
  the test user `USE` on this namespace; they cannot attach a namespace to a
  domain.
- Backing **object storage** for catalog data (the location referenced by the
  catalog's `lakehouseDir`) must be writable.
- Valid **compute node types** in that dataplane (the defaults are
  `driver-x-small` and `exec-x-small`; override them if your dataplane uses
  different names — see the optional variables below).
- The built-in `spark_catalog` default catalog. It always exists; the scripts
  never create or delete it, they only grant the test user access to it.

### 3. Required configuration

Set these as environment variables (CI) or in `dbt-iomete/.env` (local):

| Variable             | Example               | Notes                                  |
| -------------------- | --------------------- | -------------------------------------- |
| `DBT_IOMETE_HOST`    | `release.iomete.cloud`| Control-plane host                     |
| `DBT_IOMETE_ADMIN_TOKEN`| _(secret)_         | Admin token (see permissions above)    |
| `DBT_IOMETE_DOMAIN`  | `default`             | Domain to operate in                   |
| `DBT_IOMETE_DATAPLANE`| `spark-resources-1`  | Namespace the compute runs in          |
| `DBT_IOMETE_PORT`    | `443`                 |                                        |
| `DBT_IOMETE_HTTPS`   | `true`                |                                        |

Optional overrides (sensible defaults are built in):

| Variable                       | Default        | Purpose                          |
| ------------------------------ | -------------- | -------------------------------- |
| `DBT_IOMETE_DRIVER_NODE_TYPE`  | `driver-x-small`| Compute driver node type         |
| `DBT_IOMETE_EXECUTOR_NODE_TYPE`| `exec-x-small` | Compute executor node type       |
| `DBT_IOMETE_MAX_EXECUTORS`     | `2`            | Compute autoscale ceiling        |
| `DBT_IOMETE_LAKEHOUSE_DIR_PREFIX`| `s3://lakehouse`| Prefix for a created catalog's data |
| `DBT_IOMETE_VOLUME_ID`         | _(unset)_      | Optional volume for the compute  |
| `DBT_IOMETE_ACTIVE_TIMEOUT`    | `600`          | Seconds to wait for compute ACTIVE |
| `DBT_IOMETE_POLL_INTERVAL`     | `10`           | Seconds between status polls      |

> The token behind `DBT_IOMETE_ADMIN_TOKEN` authenticates against the control
> plane. The temporary test user receives its own token at provision time, written
> to `.env.test` as `DBT_IOMETE_TOKEN`; the suites use that token, not the admin one.

### 4. Local tooling

- Python 3 with `requests` (and `pyhive` + thrift for the healthcheck query; the
  healthcheck degrades to a metadata check if `pyhive` is unavailable).
- `tox` and the dbt test dependencies, for the suites themselves.
- Network egress to the host on the configured port.

## What the scripts create and remove

Created at provision time, removed at teardown:

- A temporary **test user**, with an OAuth login token plus a **personal access
  token** (the PAT is what the suites use to query the compute; it is removed
  with the user).
- The test user's **domain membership**.
- A temporary **domain role** granting the user the right to create compute and
  mint a token, assigned to the user.
- **Namespace-bundle permission grants** giving the user compute and namespace `USE`.
- A temporary **compute** (uniquely named per run, created by the test user).
- A **catalog** for the multi-catalog snapshot tests (uniquely named per run,
  `dbt_multi_catalog_<suffix>`), **granted to the domain** so the test
  compute can query it. Its name is written to
  `.env.test` as `DBT_IOMETE_ALT_CATALOG` so the suite targets it.
- An **access policy** granting the user full access to `spark_catalog` and the
  per-run catalog.
- A **`.env.test` file** (at `dbt-iomete/.env.test`) holding the test-user
  credentials (`DBT_IOMETE_TOKEN`, `DBT_IOMETE_USER_NAME`, `DBT_IOMETE_LAKEHOUSE`,
  `DBT_IOMETE_ALT_CATALOG`); loaded by `pytest-dotenv` and removed at teardown.

Never touched:

- `spark_catalog` (the default catalog) and the dataplane/namespace.

## Usage

```shell
# 1. Provision (writes temp credentials for the run)
python scripts/ci/provision.py

# 2. Run the suites (provisions, runs, and tears down for you)
scripts/ci/run-integration-tests.sh

# 3. Tear down explicitly, if you provisioned by hand
python scripts/ci/teardown.py
```

See [tests/README.md](../../tests/README.md) for the suite-level test instructions.
