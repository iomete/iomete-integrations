import pendulum

from airflow import DAG

from iomete_airflow_plugin.iomete_operator import IometeOperator

# Shared iomete connection details on the DAG so each task does not repeat them.
# `access_token_variable` is the Airflow Variable name holding the token; resolved at execute time.
args = {
    "owner": "airflow",
    "email": ["airflow@example.com"],
    "depends_on_past": False,
    "start_date": pendulum.today("UTC"),
    "host": "https://YOUR.iomete.host",
    "domain": "YOUR_DOMAIN",
    "access_token_variable": "iomete_access_token",
}

dag = DAG(dag_id="iomete-demo", default_args=args, schedule=None)

catalogTask = IometeOperator(
    task_id="task-01-sync-catalog",
    job_id="iomete-catalog-sync",
    dag=dag,
)

sqlTask = IometeOperator(
    task_id="task-02-run-sql",
    job_id="sql-runner",
    dag=dag,
)

sqlTask >> catalogTask
