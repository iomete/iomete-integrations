import multiprocessing
import unittest
from unittest import mock

from dbt.adapters.contracts.relation import RelationType
from dbt.exceptions import DbtProfileError
from dbt_common.exceptions import DbtRuntimeError
from dbt_common.context import set_invocation_context
from dbt_common.utils.executor import MultiThreadedExecutor, SingleThreadedExecutor
from dbt.adapters.iomete import SparkAdapter
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

    def _get_target_http(self, project, extra_output=None):
        output = {
            'type': 'iomete',
            'host': 'iomete.com',
            'dataplane': 'spark-resource',
            'domain': 'default',
            'lakehouse': 'dbt',
            'user': 'user1',
            'token': 'abc123',
            'port': 443,
            'schema': 'analytics'
        }
        if extra_output:
            output.update(extra_output)
        return config_from_parts_or_dicts(project, {
            'outputs': {'test': output},
            'target': 'test'
        })

    def test_relation_with_database(self):
        config = self._get_target_http(self.project_cfg)
        adapter = SparkAdapter(config, multiprocessing.get_context("spawn"))

        adapter.Relation.create(schema='different', identifier='table')
        relation = adapter.Relation.create(database='something', schema='different', identifier='table')
        self.assertIsNotNone(relation)

    def test_relation_without_database(self):
        config = self._get_target_http(self.project_cfg)
        adapter = SparkAdapter(config, multiprocessing.get_context("spawn"))

        relation = adapter.Relation.create(schema='different', identifier='table')
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

    def test_profile_with_incorrect_schema_containing_catalog_using_dot_notation(self):
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

    def test_list_relations_threads_defaults_to_100(self):
        # Listing is decoupled from dbt `threads` and defaults to 100.
        set_invocation_context({})
        config = self._get_target_http(self.project_cfg, {'threads': 1})
        adapter = SparkAdapter(config, multiprocessing.get_context("spawn"))
        self.assertEqual(config.credentials.list_relations_threads, 100)
        with adapter._list_relations_executor() as tpe:
            self.assertIsInstance(tpe, MultiThreadedExecutor)
            self.assertEqual(tpe._max_workers, 100)

    def test_list_relations_threads_can_be_overridden(self):
        set_invocation_context({})
        config = self._get_target_http(
            self.project_cfg, {'threads': 1, 'list_relations_threads': 16}
        )
        adapter = SparkAdapter(config, multiprocessing.get_context("spawn"))
        with adapter._list_relations_executor() as tpe:
            self.assertIsInstance(tpe, MultiThreadedExecutor)
            self.assertEqual(tpe._max_workers, 16)

    def test_list_relations_threads_respects_single_threaded_flag(self):
        set_invocation_context({})
        config = self._get_target_http(
            self.project_cfg, {'list_relations_threads': 16}
        )
        config.args.single_threaded = True
        adapter = SparkAdapter(config, multiprocessing.get_context("spawn"))
        with adapter._list_relations_executor() as tpe:
            self.assertIsInstance(tpe, SingleThreadedExecutor)

    def test_list_relations_threads_rejects_non_positive(self):
        with self.assertRaises(DbtRuntimeError):
            self._get_target_http(self.project_cfg, {'list_relations_threads': 0})

    def test_list_relations_skips_relations_that_fail_to_describe(self):
        # A single relation failing to describe (e.g. a broken view or a table
        # dropped mid-listing) must not fail the whole schema listing.
        set_invocation_context({})
        config = self._get_target_http(self.project_cfg)
        adapter = SparkAdapter(config, multiprocessing.get_context("spawn"))
        # Run the fan-out synchronously so the test needs no live connection.
        config.args.single_threaded = True

        schema_relation = adapter.Relation.create(schema='analytics')

        def fake_execute_macro(macro_name, kwargs=None):
            if macro_name == 'list_tables':
                return [
                    {'tableName': 'good_table', 'isTemporary': False},
                    {'tableName': 'bad_table', 'isTemporary': False},
                ]
            if macro_name == 'list_views':
                return []
            raise AssertionError(f"unexpected macro {macro_name}")

        def fake_build(schema_relation, identifier, rel_type):
            if identifier == 'bad_table':
                raise DbtRuntimeError("describe extended failed")
            return adapter.Relation.create(
                schema=schema_relation.schema, identifier=identifier, type=rel_type
            )

        with mock.patch.object(adapter, 'execute_macro', side_effect=fake_execute_macro), \
                mock.patch.object(adapter, '_build_relation_with_columns', side_effect=fake_build):
            relations = adapter.list_relations_without_caching(schema_relation)

        identifiers = {relation.identifier for relation in relations}
        self.assertEqual(identifiers, {'good_table'})
