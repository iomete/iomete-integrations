import unittest
from unittest import mock

from dbt_common.exceptions import CompilationError
from dbt.adapters.iomete.schema_service import (
    SchemaService,
    DEFAULT_SCHEMA_TIMEOUT_SECONDS,
    SCHEMA_TIMEOUT_ENV_VAR,
)


def _credentials():
    creds = mock.Mock()
    creds.scheme = "https"
    creds.host = "iomete.com"
    creds.port = 443
    creds.domain = "default"
    creds.token = "abc123"
    return creds


class TestSchemaServiceTimeout(unittest.TestCase):

    def test_defaults_to_120_when_env_unset(self):
        with mock.patch.dict("os.environ", {}, clear=True):
            service = SchemaService(_credentials())
        self.assertEqual(service.timeout, DEFAULT_SCHEMA_TIMEOUT_SECONDS)
        self.assertEqual(service.timeout, 120)

    def test_env_var_overrides_default(self):
        with mock.patch.dict("os.environ", {SCHEMA_TIMEOUT_ENV_VAR: "45"}, clear=True):
            service = SchemaService(_credentials())
        self.assertEqual(service.timeout, 45)

    def test_invalid_env_var_raises(self):
        with mock.patch.dict("os.environ", {SCHEMA_TIMEOUT_ENV_VAR: "not-a-number"}, clear=True):
            with self.assertRaises(CompilationError):
                SchemaService(_credentials())

    def test_non_positive_env_var_raises(self):
        with mock.patch.dict("os.environ", {SCHEMA_TIMEOUT_ENV_VAR: "0"}, clear=True):
            with self.assertRaises(CompilationError):
                SchemaService(_credentials())

    def test_timeout_is_passed_to_request(self):
        with mock.patch.dict("os.environ", {}, clear=True):
            service = SchemaService(_credentials())

        response = mock.Mock(status_code=200, text="{}")
        with mock.patch.object(service.session, "get", return_value=response) as get:
            service.get_table("db", "analytics", "orders")

        self.assertEqual(get.call_args.kwargs["timeout"], DEFAULT_SCHEMA_TIMEOUT_SECONDS)
