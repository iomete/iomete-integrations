![INTERROGATE](https://github.com/iomete/iomete-python-monorepo/blob/main/iomete-airflow-plugin/interrogate_badge.svg?raw=true)

# IOMETE Airflow Plugin

This Plugin helps to trigger/run Spark Jobs created in IOMETE platform.

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

dag = DAG(dag_id="...", default_args={}, schedule_interval=None)

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
    schedule_interval=None,
)
```

### Loading the access token from an Airflow Variable

If you store tokens as Airflow Variables (one per target IOMETE cluster), pass the Variable's **suffix** via `access_token_variable` instead of the raw token. The plugin prepends `variable_prefix` (default `"iomete_"`) and calls `Variable.get` at task-execute time, so different tasks can target different IOMETE clusters by naming different Variables.

```python
# Define an Airflow Variable named "iomete_prod_token" in Admin → Variables, then:
task = IometeOperator(
    task_id="run_on_prod",
    job_id="...",
    host="https://prod.iomete.com",
    domain="prod",
    access_token_variable="prod_token",  # resolves to Variable "iomete_prod_token" at execute time
    dag=dag,
)
```

Override the prefix with `variable_prefix` if you namespace iomete Variables differently:

```python
access_token_variable="prod_token",
variable_prefix="myorg_",  # resolves to Variable "myorg_prod_token"
```

Specify either `access_token` **or** `access_token_variable`, never both. If the resolved Variable is unset or empty at execute time the task fails with a clear error.

### Parameters

| Parameter               | Type   | Required               | Default     | Description                                                                                                  |
| ----------------------- | ------ | ---------------------- | ----------- | ------------------------------------------------------------------------------------------------------------ |
| `job_id`                | `str`  | yes                    | —           | Spark Job ID or name in the IOMETE platform.                                                                 |
| `host`                  | `str`  | yes                    | —           | IOMETE platform host URL.                                                                                    |
| `domain`                | `str`  | yes                    | —           | IOMETE domain identifier.                                                                                    |
| `access_token`          | `str`  | one of these two       | —           | Personal access token, passed as a raw string.                                                               |
| `access_token_variable` | `str`  | one of these two       | —           | Suffix of an Airflow Variable name. Resolved as `variable_prefix + access_token_variable` at execute time.   |
| `variable_prefix`       | `str`  | no                     | `"iomete_"` | Prefix prepended to `access_token_variable` when looking up the Airflow Variable.                            |
| `host_verify`           | `bool` | no                     | `True`      | Verify the TLS certificate of the IOMETE host.                                                               |

`host`, `domain`, `access_token`, and `access_token_variable` are Jinja-templatable.

## Migrating from 2.x

The 2.x plugin read connection details from four Airflow Variables (`iomete_host`, `iomete_access_token`, `iomete_domain`, `iomete_host_verify`). Those Variables are no longer read. Pass `host`, `domain`, `access_token` (and optionally `host_verify`) directly to `IometeOperator`. The `variable_prefix` parameter has been removed.

## Resources
For more information check:
1. [Github repository](https://github.com/iomete/iomete-airflow-plugin)
2. [IOMETE Docs](https://iomete.com/docs)
