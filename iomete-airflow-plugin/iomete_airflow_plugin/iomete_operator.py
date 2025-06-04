import json
import time
from enum import Enum
from typing import Dict, Optional, Union

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from iomete_airflow_plugin.hook import IometeHook

XCOM_RUN_ID_KEY = 'job_run_id'
XCOM_JOB_ID_KEY = 'job_id'


class IometeOperator(BaseOperator):
    """
    Run Spark job using IOMETE SDK
    """

    # Used in airflow.models.BaseOperator
    template_fields = ('job_id', 'config_override')
    template_ext = ('.json',)

    # IOMETE blue color with white text
    ui_color = '#fff'
    ui_fgcolor = '#0070f3'

    @apply_defaults
    def __init__(
        self,
        job_id: Optional[str] = None,
        config_override: Optional[Union[Dict, str]] = None,
        polling_period_seconds: int = 10,
        do_xcom_push: bool = False,
        variable_prefix: str = 'iomete_',
        **kwargs,
    ):
        """
        Creates a new ``IometeOperator``.
        """
        super().__init__(**kwargs)

        self.do_xcom_push = do_xcom_push
        self.payload = {}

        self.run_id = ''
        self.job_id = job_id
        self.polling_period_seconds = polling_period_seconds

        self.variable_prefix = variable_prefix

        if not config_override:
            self.config_override = {}
        else:
            self.config_override = config_override

        if self.job_id is None:
            raise AirflowException(
                'Parameter `job_id` should be specified. '
                'You can also specify the name of the IOMETE job in `job_id` field.'
            )

    def execute(self, context):
        self.log.info('Submitting IOMETE Job')
        hook = IometeHook(
            variable_prefix=self.variable_prefix,
        )
        dict_data = serialize_to_dict(self.config_override)
        self.run_id = hook.submit_job_run(self.job_id, dict_data)['id']
        self.log.info(f'IOMETE Job submitted. Run ID {self.run_id}')
        self._monitor_app(hook, context)

    def on_kill(self):
        hook = IometeHook(
            variable_prefix=self.variable_prefix,
        )
        hook.cancel_job_run(self.job_id, self.run_id)
        self.log.info(
            'Task: %s with job id: %s was requested to be cancelled.',
            self.task_id,
            self.job_id,
        )

    def _monitor_app(self, hook, context):

        if self.do_xcom_push:
            context['ti'].xcom_push(key=XCOM_JOB_ID_KEY, value=self.job_id)
        self.log.info('Spark job submitted with job_id: %s', self.job_id)
        if self.do_xcom_push:
            context['ti'].xcom_push(key=XCOM_RUN_ID_KEY, value=self.run_id)

        while True:
            app = hook.get_job_run(self.job_id, self.run_id)
            app_state = _get_state_from_app(app)
            if app_state.is_final:
                if app_state.is_successful:
                    self.log.info('%s completed successfully.', self.task_id)
                    return
                else:
                    error_message = '{t} failed with terminal state: {s}'.format(t=self.task_id, s=app_state.value)
                    raise AirflowException(error_message)
            else:
                self.log.info('%s in app state: %s', self.task_id, app_state.value)
                self.log.info('Sleeping for %s seconds.', self.polling_period_seconds)
                time.sleep(self.polling_period_seconds)


def serialize_to_dict(config_override: Optional[Union[Dict, str]] = None) -> Dict:
    if config_override is None:
        return {}

    if isinstance(config_override, dict):
        return config_override

    # Try parsing the string as JSON
    try:
        return json.loads(config_override)
    except json.JSONDecodeError:
        pass

    # If not valid JSON, assume it's a Python dict string and try to convert to a dict
    try:
        # Convert string representation of dict to actual dict
        return eval(config_override)
    except:
        raise ValueError(f'Unsupported format for config_override: {config_override}')


def _get_state_from_app(app):
    return ApplicationStateType(app.get('driverStatus', ''))


class ApplicationStateType(Enum):
    EmptyState = 'ENQUEUED'
    DeployingState = 'SUBMITTED'  # Usually takes ~1 min
    RunningState = 'RUNNING'
    CompletedState = 'COMPLETED'
    FailedState = 'FAILED'
    AbortedState = 'ABORTED'
    AbortingState = 'ABORTING'
    ExecutorState = map(
        {'RUNNING': 1},
        {'PENDING': 1},  # Take ~1 min to scale executor
    )

    @property
    def is_final(self) -> bool:
        return self in [
            ApplicationStateType.CompletedState,
            ApplicationStateType.FailedState,
            ApplicationStateType.AbortedState,
        ]

    @property
    def is_successful(self) -> bool:
        return self == ApplicationStateType.CompletedState
