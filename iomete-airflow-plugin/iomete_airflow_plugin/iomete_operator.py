import json
import time
from enum import Enum
from typing import Dict, Optional, Union

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator, Variable

from iomete_airflow_plugin.hook import IometeHook

XCOM_RUN_ID_KEY = "job_run_id"
XCOM_JOB_ID_KEY = "job_id"


class IometeOperator(BaseOperator):
    """
    Run Spark job using IOMETE SDK
    """

    # Used in airflow.models.BaseOperator
    template_fields = (
        "job_id",
        "config_override",
        "host",
        "domain",
        "access_token",
        "access_token_variable",
    )
    template_ext = (".json",)

    # IOMETE blue color with white text
    ui_color = "#fff"
    ui_fgcolor = "#0070f3"

    def __init__(
        self,
        job_id: Optional[str] = None,
        host: Optional[str] = None,
        domain: Optional[str] = None,
        access_token: Optional[str] = None,
        access_token_variable: Optional[str] = None,
        variable_prefix: str = "iomete_",
        host_verify: bool = True,
        config_override: Optional[Union[Dict, str]] = None,
        polling_period_seconds: int = 10,
        do_xcom_push: bool = False,
        **kwargs,
    ):
        """
        Creates a new ``IometeOperator``.
        """
        super().__init__(**kwargs)

        self.do_xcom_push = do_xcom_push
        self.payload = {}

        self.run_id = ""
        self.job_id = job_id
        self.host = host
        self.domain = domain
        self.access_token = access_token
        self.access_token_variable = access_token_variable
        self.variable_prefix = variable_prefix
        self.host_verify = host_verify
        self.polling_period_seconds = polling_period_seconds

        if not config_override:
            self.config_override = {}
        else:
            self.config_override = config_override

        if not self.job_id:
            raise AirflowException(
                "Parameter `job_id` should be specified. "
                "You can also specify the name of the IOMETE job in `job_id` field."
            )
        if not self.host:
            raise AirflowException("Parameter `host` should be specified.")
        if not self.domain:
            raise AirflowException("Parameter `domain` should be specified.")
        if self.access_token and self.access_token_variable:
            raise AirflowException(
                "Specify only one of `access_token` or `access_token_variable`, not both."
            )
        if not self.access_token and not self.access_token_variable:
            raise AirflowException(
                "Either `access_token` or `access_token_variable` must be specified."
            )

    def _resolve_access_token(self) -> str:
        if self.access_token_variable:
            name = self.variable_prefix + self.access_token_variable
            token = Variable.get(name, default_var=None)
            if not token:
                raise AirflowException(f"Airflow Variable `{name}` is not set or empty.")
            return token
        return self.access_token

    def _build_hook(self) -> IometeHook:
        return IometeHook(
            host=self.host,
            domain=self.domain,
            access_token=self._resolve_access_token(),
            host_verify=self.host_verify,
        )

    def execute(self, context):
        self.log.info("Submitting IOMETE Job")
        hook = self._build_hook()
        dict_data = serialize_to_dict(self.config_override)
        self.run_id = hook.submit_job_run(self.job_id, dict_data)["id"]
        self.log.info(f"IOMETE Job submitted. Run ID {self.run_id}")
        time.sleep(5)
        self._monitor_app(hook, context)

    def on_kill(self):
        hook = self._build_hook()
        hook.cancel_job_run(self.job_id, self.run_id)
        self.log.info(
            "Task: %s with job id: %s was requested to be cancelled.",
            self.task_id,
            self.job_id,
        )

    def _monitor_app(self, hook, context):

        if self.do_xcom_push:
            context["ti"].xcom_push(key=XCOM_JOB_ID_KEY, value=self.job_id)
        self.log.info("Spark job submitted with job_id: %s", self.job_id)
        if self.do_xcom_push:
            context["ti"].xcom_push(key=XCOM_RUN_ID_KEY, value=self.run_id)

        while True:
            app = hook.get_job_run(self.job_id, self.run_id)
            app_state = _get_state_from_app(app)
            if app_state.is_final:
                if app_state.is_successful:
                    self.log.info("%s completed successfully.", self.task_id)
                    return
                else:
                    error_message = "{t} failed with terminal state: {s}".format(t=self.task_id, s=app_state.value)
                    raise AirflowException(error_message)
            else:
                self.log.info("%s in app state: %s", self.task_id, app_state.value)
                self.log.info("Sleeping for %s seconds.", self.polling_period_seconds)
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
        raise ValueError(f"Unsupported format for config_override: {config_override}")


def _get_state_from_app(app):
    return ApplicationStateType(app.get("driverStatus", ""))


class ApplicationStateType(Enum):
    EmptyState = "ENQUEUED"
    DeployingState = "SUBMITTED"  # Usually takes ~1 min
    RunningState = "RUNNING"
    CompletedState = "COMPLETED"
    FailedState = "FAILED"
    AbortedState = "ABORTED"
    AbortingState = "ABORTING"
    ExecutorState = map(
        {"RUNNING": 1},
        {"PENDING": 1},  # Take ~1 min to scale executor
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
