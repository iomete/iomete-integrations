import unittest
from datetime import datetime
from unittest.mock import MagicMock, patch

import requests
from airflow import DAG
from airflow.exceptions import AirflowException
from iomete_sdk.api_utils import ClientError

from iomete_airflow_plugin.hook import IometeHook
from iomete_airflow_plugin.iomete_operator import (
    XCOM_JOB_ID_KEY,
    XCOM_RUN_ID_KEY,
    ApplicationStateType,
    IometeOperator,
    serialize_to_dict,
)


class TestIometeOperator(unittest.TestCase):
    def setUp(self):
        self.job_id = "test_job_id"
        self.host = "https://test.iomete.com"
        self.domain = "test_domain"
        self.access_token = "test_token"
        self.host_verify = False
        self.config_override = {"param1": "value1"}
        self.polling_period_seconds = 5
        self.do_xcom_push = True
        self.task_id = "test_task"
        self.dag = DAG(dag_id="test_dag", start_date=datetime.now())

    def _operator(self, **overrides):
        kwargs = dict(
            job_id=self.job_id,
            host=self.host,
            domain=self.domain,
            access_token=self.access_token,
            task_id=self.task_id,
        )
        kwargs.update(overrides)
        return IometeOperator(**kwargs)

    def test_init_with_all_params(self):
        operator = IometeOperator(
            job_id=self.job_id,
            host=self.host,
            domain=self.domain,
            access_token=self.access_token,
            host_verify=self.host_verify,
            config_override=self.config_override,
            polling_period_seconds=self.polling_period_seconds,
            do_xcom_push=self.do_xcom_push,
            task_id=self.task_id,
            dag=self.dag,
        )
        self.assertEqual(operator.job_id, self.job_id)
        self.assertEqual(operator.host, self.host)
        self.assertEqual(operator.domain, self.domain)
        self.assertEqual(operator.access_token, self.access_token)
        self.assertEqual(operator.host_verify, self.host_verify)
        self.assertEqual(operator.config_override, self.config_override)
        self.assertEqual(operator.polling_period_seconds, self.polling_period_seconds)
        self.assertEqual(operator.do_xcom_push, self.do_xcom_push)

    def test_host_verify_defaults_to_true(self):
        operator = self._operator()
        self.assertTrue(operator.host_verify)

    def test_init_without_job_id_raises(self):
        with self.assertRaises(AirflowException) as ctx:
            self._operator(job_id=None)
        self.assertIn("job_id", str(ctx.exception))

    def test_init_without_host_raises(self):
        with self.assertRaises(AirflowException) as ctx:
            self._operator(host=None)
        self.assertIn("host", str(ctx.exception))

    def test_init_without_domain_raises(self):
        with self.assertRaises(AirflowException) as ctx:
            self._operator(domain=None)
        self.assertIn("domain", str(ctx.exception))

    def test_init_without_access_token_raises(self):
        with self.assertRaises(AirflowException) as ctx:
            self._operator(access_token=None)
        self.assertIn("Either `access_token` or `access_token_variable`", str(ctx.exception))

    def test_init_rejects_both_access_token_and_variable(self):
        with self.assertRaises(AirflowException) as ctx:
            self._operator(access_token=self.access_token, access_token_variable="X")
        self.assertIn("only one of", str(ctx.exception))

    def test_init_rejects_neither_access_token_nor_variable(self):
        with self.assertRaises(AirflowException) as ctx:
            self._operator(access_token=None, access_token_variable=None)
        self.assertIn("Either `access_token` or `access_token_variable`", str(ctx.exception))

    @patch("iomete_airflow_plugin.iomete_operator.Variable")
    @patch("iomete_airflow_plugin.iomete_operator.IometeHook")
    def test_execute_resolves_access_token_from_variable(self, mock_hook_class, mock_variable):
        mock_hook = mock_hook_class.return_value
        mock_hook.submit_job_run.return_value = {"id": "run"}
        mock_hook.get_job_run.return_value = {"driverStatus": "COMPLETED"}
        mock_variable.get.return_value = "resolved_token_value"

        operator = self._operator(access_token=None, access_token_variable="iomete_prod_token")

        with patch("time.sleep", return_value=None):
            operator.execute({"ti": MagicMock()})

        mock_variable.get.assert_called_with("iomete_prod_token", default_var=None)
        mock_hook_class.assert_called_with(
            host=self.host,
            domain=self.domain,
            access_token="resolved_token_value",
            host_verify=True,
        )

    @patch("iomete_airflow_plugin.iomete_operator.Variable")
    def test_execute_raises_when_variable_missing(self, mock_variable):
        mock_variable.get.return_value = None
        operator = self._operator(access_token=None, access_token_variable="iomete_missing_token")
        with self.assertRaises(AirflowException) as ctx:
            operator._build_hook()
        self.assertIn("iomete_missing_token", str(ctx.exception))

    @patch("iomete_airflow_plugin.iomete_operator.Variable")
    def test_execute_raises_when_variable_empty(self, mock_variable):
        mock_variable.get.return_value = ""
        operator = self._operator(access_token=None, access_token_variable="iomete_empty_token")
        with self.assertRaises(AirflowException) as ctx:
            operator._build_hook()
        self.assertIn("iomete_empty_token", str(ctx.exception))

    @patch("iomete_airflow_plugin.iomete_operator.IometeHook")
    def test_execute_successful_job(self, mock_hook_class):
        mock_hook = mock_hook_class.return_value
        mock_hook.submit_job_run.return_value = {"id": "test_run_id"}
        mock_hook.get_job_run.side_effect = [
            {"driverStatus": "ENQUEUED"},
            {"driverStatus": "SUBMITTED"},
            {"driverStatus": "RUNNING"},
            {"driverStatus": "COMPLETED"},
        ]

        operator = self._operator(do_xcom_push=True)

        context = {"ti": MagicMock()}

        with patch("time.sleep", return_value=None):
            operator.execute(context)

        mock_hook_class.assert_called_with(
            host=self.host,
            domain=self.domain,
            access_token=self.access_token,
            host_verify=True,
        )
        mock_hook.submit_job_run.assert_called_once_with(self.job_id, {})
        self.assertEqual(operator.run_id, "test_run_id")
        context["ti"].xcom_push.assert_any_call(key=XCOM_JOB_ID_KEY, value=self.job_id)
        context["ti"].xcom_push.assert_any_call(key=XCOM_RUN_ID_KEY, value="test_run_id")

    @patch("iomete_airflow_plugin.iomete_operator.IometeHook")
    def test_execute_failed_job(self, mock_hook_class):
        mock_hook = mock_hook_class.return_value
        mock_hook.submit_job_run.return_value = {"id": "test_run_id"}
        mock_hook.get_job_run.side_effect = [
            {"driverStatus": "ENQUEUED"},
            {"driverStatus": "SUBMITTED"},
            {"driverStatus": "FAILED"},
        ]

        operator = self._operator()

        with patch("time.sleep", return_value=None):
            with self.assertRaises(AirflowException) as context_exc:
                operator.execute({"ti": MagicMock()})

        self.assertIn("failed with terminal state: FAILED", str(context_exc.exception))

    @patch("iomete_airflow_plugin.iomete_operator.IometeHook")
    def test_on_kill(self, mock_hook_class):
        mock_hook = mock_hook_class.return_value

        operator = self._operator()
        operator.run_id = "test_run_id"
        operator.on_kill()

        mock_hook_class.assert_called_with(
            host=self.host,
            domain=self.domain,
            access_token=self.access_token,
            host_verify=True,
        )
        mock_hook.cancel_job_run.assert_called_once_with(self.job_id, "test_run_id")


class TestMonitorAppRetry(unittest.TestCase):
    def setUp(self):
        self.job_id = "test_job_id"
        self.host = "https://test.iomete.com"
        self.domain = "test_domain"
        self.access_token = "test_token"
        self.task_id = "test_task"

    def _operator(self, **overrides):
        kwargs = dict(
            job_id=self.job_id,
            host=self.host,
            domain=self.domain,
            access_token=self.access_token,
            task_id=self.task_id,
        )
        kwargs.update(overrides)
        return IometeOperator(**kwargs)

    @patch("iomete_airflow_plugin.iomete_operator.IometeHook")
    def test_retries_on_500_then_succeeds(self, mock_hook_class):
        mock_hook = mock_hook_class.return_value
        mock_hook.submit_job_run.return_value = {"id": "run1"}
        mock_hook.get_job_run.side_effect = [
            ClientError(status=500, content={"error": "Internal Server Error"}),
            ClientError(status=502, content={"error": "Bad Gateway"}),
            {"driverStatus": "COMPLETED"},
        ]

        operator = self._operator()
        context = {"ti": MagicMock()}

        with patch("time.sleep", return_value=None):
            operator.execute(context)

        self.assertEqual(mock_hook.get_job_run.call_count, 3)

    @patch("iomete_airflow_plugin.iomete_operator.IometeHook")
    def test_retries_on_429_then_succeeds(self, mock_hook_class):
        mock_hook = mock_hook_class.return_value
        mock_hook.submit_job_run.return_value = {"id": "run1"}
        mock_hook.get_job_run.side_effect = [
            ClientError(status=429, content={"error": "Too Many Requests"}),
            ClientError(status=429, content={"error": "Too Many Requests"}),
            {"driverStatus": "COMPLETED"},
        ]

        operator = self._operator()
        context = {"ti": MagicMock()}

        with patch("time.sleep", return_value=None):
            operator.execute(context)

        self.assertEqual(mock_hook.get_job_run.call_count, 3)

    @patch("iomete_airflow_plugin.iomete_operator.IometeHook")
    def test_fails_immediately_on_4xx(self, mock_hook_class):
        mock_hook = mock_hook_class.return_value
        mock_hook.submit_job_run.return_value = {"id": "run1"}
        mock_hook.get_job_run.side_effect = ClientError(status=404, content={"error": "Not Found"})

        operator = self._operator()
        context = {"ti": MagicMock()}

        with patch("time.sleep", return_value=None):
            with self.assertRaises(AirflowException) as ctx:
                operator.execute(context)

        self.assertIn("Non-retryable API error (HTTP 404)", str(ctx.exception))
        self.assertEqual(mock_hook.get_job_run.call_count, 1)

    @patch("iomete_airflow_plugin.iomete_operator.IometeHook")
    def test_fails_immediately_on_401(self, mock_hook_class):
        mock_hook = mock_hook_class.return_value
        mock_hook.submit_job_run.return_value = {"id": "run1"}
        mock_hook.get_job_run.side_effect = ClientError(status=401, content={"error": "Unauthorized"})

        operator = self._operator()
        context = {"ti": MagicMock()}

        with patch("time.sleep", return_value=None):
            with self.assertRaises(AirflowException) as ctx:
                operator.execute(context)

        self.assertIn("Non-retryable API error (HTTP 401)", str(ctx.exception))
        self.assertEqual(mock_hook.get_job_run.call_count, 1)

    @patch("iomete_airflow_plugin.iomete_operator.IometeHook")
    def test_fails_after_max_consecutive_500_errors(self, mock_hook_class):
        mock_hook = mock_hook_class.return_value
        mock_hook.submit_job_run.return_value = {"id": "run1"}
        mock_hook.get_job_run.side_effect = [
            ClientError(status=500, content={"error": "Internal Server Error"})
            for _ in range(5)
        ]

        operator = self._operator()
        context = {"ti": MagicMock()}

        with patch("time.sleep", return_value=None):
            with self.assertRaises(AirflowException) as ctx:
                operator.execute(context)

        self.assertIn("Failed to get job status after 5 consecutive attempts", str(ctx.exception))
        self.assertEqual(mock_hook.get_job_run.call_count, 5)

    @patch("iomete_airflow_plugin.iomete_operator.IometeHook")
    def test_retries_on_network_error_then_succeeds(self, mock_hook_class):
        mock_hook = mock_hook_class.return_value
        mock_hook.submit_job_run.return_value = {"id": "run1"}
        mock_hook.get_job_run.side_effect = [
            requests.exceptions.ConnectionError("Connection refused"),
            requests.exceptions.Timeout("Read timed out"),
            {"driverStatus": "COMPLETED"},
        ]

        operator = self._operator()
        context = {"ti": MagicMock()}

        with patch("time.sleep", return_value=None):
            operator.execute(context)

        self.assertEqual(mock_hook.get_job_run.call_count, 3)

    @patch("iomete_airflow_plugin.iomete_operator.IometeHook")
    def test_fails_after_max_consecutive_network_errors(self, mock_hook_class):
        mock_hook = mock_hook_class.return_value
        mock_hook.submit_job_run.return_value = {"id": "run1"}
        mock_hook.get_job_run.side_effect = [
            requests.exceptions.ConnectionError("Connection refused")
            for _ in range(5)
        ]

        operator = self._operator()
        context = {"ti": MagicMock()}

        with patch("time.sleep", return_value=None):
            with self.assertRaises(AirflowException) as ctx:
                operator.execute(context)

        self.assertIn("Failed to get job status after 5 consecutive attempts", str(ctx.exception))
        self.assertEqual(mock_hook.get_job_run.call_count, 5)

    @patch("iomete_airflow_plugin.iomete_operator.IometeHook")
    def test_consecutive_error_counter_resets_on_success(self, mock_hook_class):
        mock_hook = mock_hook_class.return_value
        mock_hook.submit_job_run.return_value = {"id": "run1"}
        mock_hook.get_job_run.side_effect = [
            # 4 errors, then a success (resets counter), then 4 more errors, then complete
            ClientError(status=500, content={}),
            ClientError(status=500, content={}),
            ClientError(status=500, content={}),
            ClientError(status=500, content={}),
            {"driverStatus": "RUNNING"},  # resets counter
            ClientError(status=500, content={}),
            ClientError(status=500, content={}),
            ClientError(status=500, content={}),
            ClientError(status=500, content={}),
            {"driverStatus": "COMPLETED"},
        ]

        operator = self._operator()
        context = {"ti": MagicMock()}

        with patch("time.sleep", return_value=None):
            operator.execute(context)

        self.assertEqual(mock_hook.get_job_run.call_count, 10)

    @patch("iomete_airflow_plugin.iomete_operator.IometeHook")
    def test_mixed_500_and_network_errors_share_counter(self, mock_hook_class):
        mock_hook = mock_hook_class.return_value
        mock_hook.submit_job_run.return_value = {"id": "run1"}
        mock_hook.get_job_run.side_effect = [
            ClientError(status=500, content={}),
            requests.exceptions.ConnectionError("Connection refused"),
            ClientError(status=503, content={}),
            requests.exceptions.Timeout("Read timed out"),
            ClientError(status=500, content={}),
        ]

        operator = self._operator()
        context = {"ti": MagicMock()}

        with patch("time.sleep", return_value=None):
            with self.assertRaises(AirflowException) as ctx:
                operator.execute(context)

        self.assertIn("Failed to get job status after 5 consecutive attempts", str(ctx.exception))
        self.assertEqual(mock_hook.get_job_run.call_count, 5)


class TestSerializeToDict(unittest.TestCase):
    def test_serialize_none(self):
        self.assertEqual(serialize_to_dict(None), {})

    def test_serialize_dict(self):
        config = {"key": "value"}
        self.assertEqual(serialize_to_dict(config), config)

    def test_serialize_json_string(self):
        config_json = '{"key": "value"}'
        self.assertEqual(serialize_to_dict(config_json), {"key": "value"})

    def test_serialize_python_dict_string(self):
        config_str = "{'key': 'value'}"
        self.assertEqual(serialize_to_dict(config_str), {"key": "value"})

    def test_serialize_invalid_format(self):
        config_str = "invalid string"
        with self.assertRaises(ValueError):
            serialize_to_dict(config_str)


class TestApplicationStateType(unittest.TestCase):
    def test_application_state_type_is_final(self):
        self.assertTrue(ApplicationStateType.CompletedState.is_final)
        self.assertTrue(ApplicationStateType.FailedState.is_final)
        self.assertTrue(ApplicationStateType.AbortedState.is_final)
        self.assertFalse(ApplicationStateType.RunningState.is_final)

    def test_application_state_type_is_successful(self):
        self.assertTrue(ApplicationStateType.CompletedState.is_successful)
        self.assertFalse(ApplicationStateType.FailedState.is_successful)
        self.assertFalse(ApplicationStateType.AbortedState.is_successful)


class TestIometeHook(unittest.TestCase):
    @patch("iomete_airflow_plugin.hook.SparkJobApiClient")
    def test_hook_passes_params_to_client(self, mock_client_class):
        IometeHook(
            host="https://test.iomete.com",
            domain="test_domain",
            access_token="test_token",
        )

        mock_client_class.assert_called_once_with(
            host="https://test.iomete.com",
            api_key="test_token",
            domain="test_domain",
            verify=True,
        )

    @patch("iomete_airflow_plugin.hook.SparkJobApiClient")
    def test_hook_respects_host_verify_false(self, mock_client_class):
        IometeHook(
            host="https://test.iomete.com",
            domain="test_domain",
            access_token="test_token",
            host_verify=False,
        )

        mock_client_class.assert_called_once_with(
            host="https://test.iomete.com",
            api_key="test_token",
            domain="test_domain",
            verify=False,
        )


if __name__ == "__main__":
    unittest.main()
