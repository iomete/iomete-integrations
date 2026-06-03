import pendulum

from airflow import DAG

from iomete_airflow_plugin.iomete_operator import IometeOperator

args = {
    "owner": "airflow",
    "email": ["airflow@example.com"],
    "depends_on_past": False,
    "start_date": pendulum.today("UTC"),
}

dag = DAG(
    dag_id="iomete-task-with-args",
    default_args=args,
    schedule=None,
    params={
        "job_id": "0761a510-3a66-4c72-b06e-9d071f30d85d",
        "config_override": {
            "envVars": {"env1": "value1"},
            "arguments": ["arg1", "arg2"],
        },
    },
)

# Resolves the token from the Airflow Variable "iomete_access_token" at execute time.
task = IometeOperator(
    task_id="iomete-catalog-sync-task-with-config",
    job_id="{{ params.job_id }}",
    config_override="{{ params.config_override }}",
    host="https://YOUR.iomete.host",
    domain="YOUR_DOMAIN",
    access_token_variable="access_token",
    dag=dag,
)
