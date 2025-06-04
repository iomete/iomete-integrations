import unittest
from datetime import datetime
from unittest.mock import MagicMock, patch

from airflow import DAG
from airflow.exceptions import AirflowException

from iomete_airflow_plugin.iomete_operator import (
    XCOM_JOB_ID_KEY,
    XCOM_RUN_ID_KEY,
    ApplicationStateType,
    IometeOperator,
    serialize_to_dict,
)


class TestIometeOperator(unittest.TestCase):
    def setUp(self):
        self.job_id = 'test_job_id'
        self.config_override = {'param1': 'value1'}
        self.polling_period_seconds = 5
        self.do_xcom_push = True
        self.variable_prefix = 'iomete_test_'
        self.task_id = 'test_task'
        self.dag = DAG(dag_id='test_dag', start_date=datetime.now())

    def test_init_with_job_id(self):
        operator = IometeOperator(
            job_id=self.job_id,
            config_override=self.config_override,
            polling_period_seconds=self.polling_period_seconds,
            do_xcom_push=self.do_xcom_push,
            variable_prefix=self.variable_prefix,
            task_id=self.task_id,
            dag=self.dag,
        )
        self.assertEqual(operator.job_id, self.job_id)
        self.assertEqual(operator.config_override, self.config_override)
        self.assertEqual(operator.polling_period_seconds, self.polling_period_seconds)
        self.assertEqual(operator.do_xcom_push, self.do_xcom_push)
        self.assertEqual(operator.variable_prefix, self.variable_prefix)

    def test_init_without_job_id(self):
        with self.assertRaises(AirflowException):
            IometeOperator(job_id=None, task_id=self.task_id)

    @patch('iomete_airflow_plugin.iomete_operator.IometeHook')
    def test_execute_successful_job(self, mock_hook_class):
        mock_hook = mock_hook_class.return_value
        mock_hook.submit_job_run.return_value = {'id': 'test_run_id'}
        mock_hook.get_job_run.side_effect = [
            {'driverStatus': 'ENQUEUED'},
            {'driverStatus': 'SUBMITTED'},
            {'driverStatus': 'RUNNING'},
            {'driverStatus': 'COMPLETED'},
        ]

        operator = IometeOperator(job_id=self.job_id, task_id=self.task_id, do_xcom_push=True)

        context = {'ti': MagicMock()}

        with patch('time.sleep', return_value=None):
            operator.execute(context)

        mock_hook.submit_job_run.assert_called_once_with(self.job_id, {})
        self.assertEqual(operator.run_id, 'test_run_id')
        context['ti'].xcom_push.assert_any_call(key=XCOM_JOB_ID_KEY, value=self.job_id)
        context['ti'].xcom_push.assert_any_call(key=XCOM_RUN_ID_KEY, value='test_run_id')

    @patch('iomete_airflow_plugin.iomete_operator.IometeHook')
    def test_execute_failed_job(self, mock_hook_class):
        mock_hook = mock_hook_class.return_value
        mock_hook.submit_job_run.return_value = {'id': 'test_run_id'}
        mock_hook.get_job_run.side_effect = [
            {'driverStatus': 'ENQUEUED'},
            {'driverStatus': 'SUBMITTED'},
            {'driverStatus': 'FAILED'},
        ]

        operator = IometeOperator(job_id=self.job_id, task_id=self.task_id)

        with patch('time.sleep', return_value=None):
            with self.assertRaises(AirflowException) as context_exc:
                operator.execute({'ti': MagicMock()})

        self.assertIn('failed with terminal state: FAILED', str(context_exc.exception))

    @patch('iomete_airflow_plugin.iomete_operator.IometeHook')
    def test_on_kill(self, mock_hook_class):
        mock_hook = mock_hook_class.return_value

        operator = IometeOperator(job_id=self.job_id, task_id=self.task_id)
        operator.run_id = 'test_run_id'
        operator.on_kill()

        mock_hook.cancel_job_run.assert_called_once_with(self.job_id, 'test_run_id')


class TestSerializeToDict(unittest.TestCase):
    def test_serialize_none(self):
        self.assertEqual(serialize_to_dict(None), {})

    def test_serialize_dict(self):
        config = {'key': 'value'}
        self.assertEqual(serialize_to_dict(config), config)

    def test_serialize_json_string(self):
        config_json = '{"key": "value"}'
        self.assertEqual(serialize_to_dict(config_json), {'key': 'value'})

    def test_serialize_python_dict_string(self):
        config_str = "{'key': 'value'}"
        self.assertEqual(serialize_to_dict(config_str), {'key': 'value'})

    def test_serialize_invalid_format(self):
        config_str = 'invalid string'
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


if __name__ == '__main__':
    unittest.main()
