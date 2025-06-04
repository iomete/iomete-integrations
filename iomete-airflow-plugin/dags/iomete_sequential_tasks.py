from airflow import DAG, utils

from iomete_airflow_plugin.iomete_operator import IometeOperator

args = {
    'owner': 'airflow',
    'email': ['airflow@example.com'],
    'depends_on_past': False,
    'start_date': utils.dates.days_ago(0, second=1),
}

dag = DAG(dag_id='iomete-demo', default_args=args, schedule_interval=None)

catalogTask = IometeOperator(
    task_id='task-01-sync-catalog',
    job_id='iomete-catalog-sync',
    dag=dag,
    variable_prefix='iomete_',
)

sqlTask = IometeOperator(
    task_id='task-02-run-sql',
    job_id='sql-runner',
    dag=dag,
    variable_prefix='iomete_',
)

sqlTask >> catalogTask
