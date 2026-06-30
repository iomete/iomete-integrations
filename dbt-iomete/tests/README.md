# Testing dbt-iomete


## Prerequisites

Before running the tests, the following resources must exist in the target IOMETE environment
(the `release` env, i.e. `release.iomete.cloud`, unless overridden via the variables below):

- **Compute**: a running compute named `<DBT_IOMETE_LAKEHOUSE>` (default `dbt`) in the
  `<DBT_IOMETE_DATAPLANE>` namespace (default `spark-resources-1`). The compute must be up and
  reachable so dbt can submit queries against it.
- **Catalogs**: both `spark_catalog` and `test_dbt_multi_catalog` must exist and be accessible.
  `spark_catalog` is the default catalog used by the tests; `test_dbt_multi_catalog` is the
  alternate catalog exercised by the multi-catalog snapshot tests.
- **Permissions**: the user behind `DBT_IOMETE_TOKEN` (issued in the `<DBT_IOMETE_DOMAIN>` domain,
  default `default`) must have access to run queries on the compute above and full access to run
  queries on both catalogs.

> Note: The values in angle brackets (e.g. `<DBT_IOMETE_LAKEHOUSE>`, `<DBT_IOMETE_DATAPLANE>`,
> `<DBT_IOMETE_DOMAIN>`) refer to the corresponding environment variables set below.

> TODO: We should add a setup script to auto-create these resources (compute, catalogs, data
> policies) if they don't already exist, so this prep is no longer manual. The token used by the
> tests is expected to have permission to create/edit/consume compute, create catalogs, and create
> data policies — the same script can later be reused by CI to provision the required resources.

## Set credentials

### Option A: `.env` file (recommended)

Create a `.env` file in the `dbt-iomete/` directory. `pytest-dotenv` loads it automatically

```dotenv
DBT_IOMETE_HOST=release.iomete.cloud
DBT_IOMETE_PORT=443
DBT_IOMETE_HTTPS=true
DBT_IOMETE_LAKEHOUSE=dbt
DBT_IOMETE_USER_NAME=admin
DBT_IOMETE_DOMAIN=default
DBT_IOMETE_TOKEN=<dbt-token>
DBT_IOMETE_DATAPLANE=spark-resources-1
```

### Option B: export environment variables

```bash
export DBT_IOMETE_HOST=release.iomete.cloud
export DBT_IOMETE_PORT=443
export DBT_IOMETE_HTTPS=true
export DBT_IOMETE_LAKEHOUSE=dbt
export DBT_IOMETE_USER_NAME=admin
export DBT_IOMETE_DOMAIN=default
export DBT_IOMETE_TOKEN=<dbt-token>
export DBT_IOMETE_DATAPLANE=spark-resources-1
```

## Run integration test

```shell
tox -e integration-iomete
```

## Run functional test (Using DBT Adaptor Tests)

```shell
tox -e functional
```

## Run unit tests
```shell
tox -e unit
```
