import multiprocessing
import unittest
from unittest import mock

from dbt.exceptions import DbtProfileError
from dbt_common.exceptions import DbtRuntimeError
from dbt.adapters.iomete import SparkAdapter
from dbt.adapters.contracts.connection import ConnectionState
from dbt.adapters.iomete.connections import (
    IometeSqlAlchemyConnectionWrapper,
    SparkConnectionManager,
)
from .utils import config_from_parts_or_dicts


class TestSparkAdapter(unittest.TestCase):

    def setUp(self):

        self.project_cfg = {
            'name': 'X',
            'version': '0.1',
            'profile': 'test',
            'project-root': '/tmp/dbt/does-not-exist',
            'quoting': {
                'identifier': False,
                'schema': False,
            },
            'config-version': 2
        }

    def _get_target_http(self, project):
        return config_from_parts_or_dicts(project, {
            'outputs': {
                'test': {
                    'type': 'iomete',
                    'host': 'iomete.com',
                    'dataplane': 'spark-resource',
                    'domain': 'default',
                    'cluster': 'dbt',
                    'user': 'user1',
                    'token': 'abc123',
                    'port': 443,
                    'schema': 'analytics'
                }
            },
            'target': 'test'
        })

    def test_relation_with_database(self):
        config = self._get_target_http(self.project_cfg)
        adapter = SparkAdapter(config, multiprocessing.get_context("spawn"))

        adapter.Relation.create(schema='different', identifier='table')
        relation = adapter.Relation.create(
            database='something',
            schema='different',
            identifier='table',
        )
        self.assertIsNotNone(relation)

    def test_relation_without_database(self):
        config = self._get_target_http(self.project_cfg)
        adapter = SparkAdapter(config, multiprocessing.get_context("spawn"))

        relation = adapter.Relation.create(
            schema='different',
            identifier='table',
        )
        self.assertIsNotNone(relation)

    def test_profile_with_database_keyword(self):
        profile = {
            'outputs': {
                'test': {
                    'type': 'iomete',
                    'database': 'demo_catalog',
                    'schema': 'analytics',
                    'host': 'myorg.sparkhost.com',
                    'port': 443,
                    'token': 'abc123',
                    'cluster': '01234-23423-coffeetime',
                }
            },
            'target': 'test'
        }
        config = config_from_parts_or_dicts(self.project_cfg, profile)
        adapter = SparkAdapter(config, multiprocessing.get_context("spawn"))

        self.assertEqual(adapter.config.credentials.database, 'demo_catalog')

    def test_profile_with_data_plane_keyword(self):
        profile = {
            'outputs': {
                'test': {
                    'type': 'iomete',
                    'database': 'demo_catalog',
                    'schema': 'analytics',
                    'host': 'myorg.sparkhost.com',
                    'port': 443,
                    'token': 'abc123',
                    'cluster': '01234-23423-coffeetime',
                    'data_plane': 'spark-resource',
                }
            },
            'target': 'test'
        }
        config = config_from_parts_or_dicts(self.project_cfg, profile)
        adapter = SparkAdapter(config, multiprocessing.get_context("spawn"))

        self.assertEqual(
            adapter.config.credentials.data_plane_name,
            'spark-resource',
        )

    def test_profile_keeps_lakehouse_and_dataplane_compatibility(self):
        profile = {
            'outputs': {
                'test': {
                    'type': 'iomete',
                    'database': 'demo_catalog',
                    'schema': 'analytics',
                    'host': 'myorg.sparkhost.com',
                    'port': 443,
                    'token': 'abc123',
                    'lakehouse': 'legacy-lakehouse',
                    'dataplane': 'legacy-plane',
                }
            },
            'target': 'test'
        }
        config = config_from_parts_or_dicts(self.project_cfg, profile)
        adapter = SparkAdapter(config, multiprocessing.get_context("spawn"))

        self.assertEqual(
            adapter.config.credentials.cluster_name,
            'legacy-lakehouse',
        )
        self.assertEqual(
            adapter.config.credentials.data_plane_name,
            'legacy-plane',
        )

    def test_open_uses_iomete_sqlalchemy_url(self):
        config = self._get_target_http(self.project_cfg)
        connection = mock.MagicMock()
        connection.state = ConnectionState.CLOSED
        connection.credentials = config.credentials
        raw_connection = mock.MagicMock()
        engine = mock.MagicMock()
        engine.raw_connection.return_value = raw_connection

        with mock.patch(
            'dbt.adapters.iomete.connections.create_engine',
            return_value=engine,
        ) as create_engine:
            opened = SparkConnectionManager.open(connection)

        url = create_engine.call_args.args[0]
        self.assertEqual(url.drivername, 'iomete')
        self.assertEqual(url.username, 'user1')
        self.assertEqual(url.password, 'abc123')
        self.assertEqual(url.host, 'iomete.com')
        self.assertEqual(url.port, 443)
        self.assertEqual(url.database, 'spark_catalog/analytics')
        self.assertEqual(url.query['cluster'], 'dbt')
        self.assertEqual(url.query['data_plane'], 'spark-resource')
        self.assertEqual(url.query['tls'], 'true')
        self.assertEqual(opened.handle.handle, raw_connection)
        self.assertEqual(opened.state, ConnectionState.OPEN)

    def test_wrapper_uses_executemany_for_parameterized_writes(self):
        dbapi_connection = mock.MagicMock()
        cursor = dbapi_connection.cursor.return_value
        cursor.fetchall.side_effect = RuntimeError("no rows")
        wrapper = IometeSqlAlchemyConnectionWrapper(dbapi_connection).cursor()

        wrapper.execute("insert into table values (?)", [1])

        cursor.executemany.assert_called_once_with(
            "insert into table values (?)",
            [(1.0,)],
        )
        cursor.execute.assert_not_called()

    def test_wrapper_preserves_result_sets_for_reads(self):
        dbapi_connection = mock.MagicMock()
        cursor = dbapi_connection.cursor.return_value
        wrapper = IometeSqlAlchemyConnectionWrapper(dbapi_connection).cursor()

        wrapper.execute("select * from table where id = ?", [1])

        cursor.execute.assert_called_once_with(
            "select * from table where id = ?",
            (1.0,),
        )
        cursor.fetchall.assert_not_called()

    def test_profile_with_catalog_keyword(self):
        profile = {
            'outputs': {
                'test': {
                    'type': 'iomete',
                    'catalog': 'demo_catalog',
                    'schema': 'analytics',
                    'host': 'myorg.sparkhost.com',
                    'port': 443,
                    'token': 'abc123',
                    'cluster': '01234-23423-coffeetime',
                }
            },
            'target': 'test'
        }
        config = config_from_parts_or_dicts(self.project_cfg, profile)
        adapter = SparkAdapter(config, multiprocessing.get_context("spawn"))

        self.assertEqual(adapter.config.credentials.database, 'demo_catalog')

    def test_profile_with_both_database_and_catalog(self):
        profile = {
            'outputs': {
                'test': {
                    'type': 'iomete',
                    'database': 'demo_catalog',
                    'catalog': 'demo_catalog',
                    'schema': 'analytics',
                    'host': 'myorg.sparkhost.com',
                    'port': 443,
                    'token': 'abc123',
                    'cluster': '01234-23423-coffeetime',
                }
            },
            'target': 'test'
        }

        with self.assertRaises(DbtProfileError):
            config_from_parts_or_dicts(self.project_cfg, profile)

    def test_profile_with_empty_database(self):
        profile = {
            'outputs': {
                'test': {
                    'type': 'iomete',
                    'database': '',
                    'schema': 'analytics',
                    'host': 'myorg.sparkhost.com',
                    'port': 443,
                    'token': 'abc123',
                    'cluster': '01234-23423-coffeetime',
                }
            },
            'target': 'test'
        }

        with self.assertRaises(DbtRuntimeError):
            config_from_parts_or_dicts(self.project_cfg, profile)

    def test_profile_with_schema_containing_catalog_dot_notation(self):
        profile = {
            'outputs': {
                'test': {
                    'type': 'iomete',
                    'database': 'demo_catalog',
                    'schema': 'demo_catalog.analytics',
                    'host': 'myorg.sparkhost.com',
                    'port': 443,
                    'token': 'abc123',
                    'cluster': '01234-23423-coffeetime',
                }
            },
            'target': 'test'
        }

        with self.assertRaises(DbtRuntimeError):
            config_from_parts_or_dicts(self.project_cfg, profile)
