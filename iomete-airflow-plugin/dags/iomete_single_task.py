import pendulum

from airflow import DAG

from iomete_airflow_plugin.iomete_operator import IometeOperator

args = {
    "owner": "airflow",
    "email": ["airflow@example.com"],
    "depends_on_past": False,
    "start_date": pendulum.today("UTC"),
}

dag = DAG(dag_id="iomete-task", default_args=args, schedule=None)

# Resolves the token from the Airflow Variable named "iomete_access_token" at execute time.
task = IometeOperator(
    task_id="iomete-catalog-sync-task",
    job_id="0761a510-3a66-4c72-b06e-9d071f30d85d",
    host="https://YOUR.iomete.host",
    domain="YOUR_DOMAIN",
    access_token_variable="iomete_access_token",
    dag=dag,
)
