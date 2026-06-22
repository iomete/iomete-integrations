![INTERROGATE](https://github.com/iomete/iomete-python-monorepo/blob/main/iomete-airflow-plugin/interrogate_badge.svg?raw=true)

# IOMETE Airflow Plugin

This Plugin helps to trigger/run Spark Jobs created in IOMETE platform.

## Requirements

- Apache Airflow `>=2.10.5, <4.0.0`
- Python `3.10` – `3.13`

## Installation

This plugin could be easily installed with `pip`. Just run the following in your aiflow server:
```bash
pip install iomete-airflow-plugin
```
Restart you server and check the plugins page on Airflow's admin panel to make sure plugin loaded successfully.

## Usage

Pass your IOMETE connection details directly to `IometeOperator`. A single Airflow instance can trigger jobs against multiple IOMETE clusters by passing different `host`/`domain`/`access_token` values per task.

```python
from airflow import DAG
from iomete_airflow_plugin.iomete_operator import IometeOperator

dag = DAG(dag_id="...", default_args={}, schedule=None)

task = IometeOperator(
    task_id="random_task_id",
    job_id="1b0fc29b-5491-4c0a-94ea-48e304c3c72e",  # Spark Job ID or Name in IOMETE platform.
    host="https://YOUR.iomete.host",
    domain="YOUR_DOMAIN",
    access_token="YOUR_ACCESS_TOKEN",
    dag=dag,
)
```

To avoid repeating credentials on every task, put them in the DAG's `default_args`:

```python
dag = DAG(
    dag_id="...",
    default_args={
        "host": "https://YOUR.iomete.host",
        "domain": "YOUR_DOMAIN",
        "access_token": "YOUR_ACCESS_TOKEN",
    },
    schedule=None,
)
```

### Loading the access token from an Airflow Variable

If you store tokens as Airflow Variables (one per target IOMETE cluster), pass the Variable name via `access_token_variable` instead of the raw token. The plugin calls `Variable.get` at task-execute time, so different tasks can target different IOMETE clusters by naming different Variables. We recommend prefixing your Variable names with `iomete_` to keep them grouped in Admin → Variables.

```python
# Define an Airflow Variable named "iomete_prod_token" in Admin → Variables, then:
task = IometeOperator(
    task_id="run_on_prod",
    job_id="...",
    host="https://prod.iomete.com",
    domain="prod",
    access_token_variable="iomete_prod_token",
    dag=dag,
)
```

Specify either `access_token` **or** `access_token_variable`, never both. If the resolved Variable is unset or empty at execute time the task fails with a clear error.

### Parameters

| Parameter               | Type   | Required               | Default     | Description                                                                                                  |
| ----------------------- | ------ | ---------------------- | ----------- | ------------------------------------------------------------------------------------------------------------ |
| `job_id`                | `str`  | yes                    | —           | Spark Job ID or name in the IOMETE platform.                                                                 |
| `host`                  | `str`  | yes                    | —           | IOMETE platform host URL.                                                                                    |
| `domain`                | `str`  | yes                    | —           | IOMETE domain identifier.                                                                                    |
| `access_token`          | `str`  | one of these two       | —           | Personal access token, passed as a raw string.                                                               |
| `access_token_variable` | `str`  | one of these two       | —           | Name of an Airflow Variable holding the token. Resolved via `Variable.get` at execute time.                  |
| `host_verify`           | `bool` | no                     | `True`      | Verify the TLS certificate of the IOMETE host.                                                               |

`host`, `domain`, and `access_token_variable` are Jinja-templatable. `access_token` is **not** templatable: Airflow persists rendered template fields to its metadata database and exposes them in the UI, so templating raw tokens would leak them. Use `access_token_variable` to template the lookup name instead.

## Migrating from 2.x

The 2.x plugin read connection details from four Airflow Variables (`iomete_host`, `iomete_access_token`, `iomete_domain`, `iomete_host_verify`) and exposed a `variable_prefix` parameter to namespace them. Both are gone in 3.0.0. Pass `host`, `domain`, and either `access_token` or `access_token_variable` (and optionally `host_verify`) directly to `IometeOperator`. If you previously used `variable_prefix`, set `access_token_variable` to the full Airflow Variable name instead (e.g. `access_token_variable="iomete_prod_token"`).

## Resources
For more information check:
1. [Github repository](https://github.com/iomete/iomete-airflow-plugin)
2. [IOMETE Docs](https://iomete.com/docs)
