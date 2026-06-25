# Testing dbt-iomete


## Set credentials

### Option A: `.env` file (recommended)

Create a `.env` file in the `dbt-iomete/` directory. `pytest-dotenv` loads it
automatically (already-set environment variables take precedence, so CI is
unaffected).

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