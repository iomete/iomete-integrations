from airflow import DAG, utils

from iomete_airflow_plugin.iomete_operator import IometeOperator

args = {
    'owner': 'airflow',
    'email': ['airflow@example.com'],
    'depends_on_past': False,
    'start_date': utils.dates.days_ago(0, second=1),
}

dag = DAG(
    dag_id='iomete-task-with-args',
    default_args=args,
    schedule_interval=None,
    params={
        'job_id': '0761a510-3a66-4c72-b06e-9d071f30d85d',
        'config_override': {
            'envVars': {'env1': 'value1'},
            'arguments': ['arg1', 'arg2'],
        },
    },
)

task = IometeOperator(
    task_id='iomete-catalog-sync-task-with-config',
    job_id='{{ params.job_id }}',
    config_override='{{ params.config_override }}',
    dag=dag,
)
