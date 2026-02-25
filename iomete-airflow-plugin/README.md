![INTERROGATE](https://github.com/iomete/iomete-python-monorepo/blob/main/iomete-airflow-plugin/interrogate_badge.svg?raw=true)

# IOMETE Airflow Plugin

This Plugin helps to trigger/run Spark Jobs created in IOMETE platform.

## Installation

This plugin could be easily installed with `pip`. Just run the following in your aiflow server:
```bash
pip install iomete-airflow-plugin
```
Restart you server and check the plugins page on Airflow's admin panel to make sure plugin loaded successfully.

## Configuration

You need to add the following keys to airflow's Variables page:

- `iomete_access_token` - Personal access token. Check our documentation page on how to generate one.
- `iomete_host` - IOMETE platform host URL.
- `iomete_domain` - IOMETE domain identifier (required since v2.0.0).

## Usage

Here are sample DAG:
```python
from airflow import DAG
from iomete_airflow_plugin.iomete_operator import IometeOperator

dag = DAG(dag_id="...", default_args={}, schedule_interval=None)

task = IometeOperator(
    task_id="random_task_id",
    job_id="1b0fc29b-5491-4c0a-94ea-48e304c3c72e", # Spark Job ID or Name in IOMETE platform.
    dag=dag,
)

```

## Resources
For more information check: 
1. [Github repository](https://github.com/iomete/iomete-airflow-plugin)
2. [IOMETE Docs](https://iomete.com/docs)
