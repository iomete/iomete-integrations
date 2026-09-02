import unittest
from unittest import mock
import re
from jinja2 import Environment, FileSystemLoader


class TestSparkMacros(unittest.TestCase):

    def setUp(self):
        self.jinja_env = Environment(loader=FileSystemLoader('dbt/include/iomete/macros'),
                                     extensions=['jinja2.ext.do', ])

        self.config = {}
        self.default_context = {
            'validation': mock.Mock(),
            'model': mock.Mock(),
            'exceptions': mock.Mock(),
            'config': mock.Mock()
        }
        self.default_context['config'].get = lambda key, default=None, **kwargs: self.config.get(key, default)

    def __get_template(self, template_filename):
        return self.jinja_env.get_template(template_filename, globals=self.default_context)

    def __run_macro(self, template, name, temporary, relation, sql):
        self.default_context['model'].alias = relation
        value = getattr(template.module, name)(temporary, relation, sql)
        return re.sub(r'\s\s+', ' ', value)

    def test_macros_load(self):
        self.jinja_env.get_template('adapters.sql')

    def _render_list_macros(self, relation):
        """Render the list_tables/list_views macros against `relation` and return
        the SQL each hands to `statement`, keyed by statement name. Reproduces how
        the adapter calls these macros (the schema Relation is passed straight
        through and interpolated into `show tables|views in {{ ... }}`)."""
        captured = {}

        def fake_statement(name, **kwargs):
            # `{% call statement(...) %}<sql>{% endcall %}` passes the block body
            # to the callable as the `caller` kwarg.
            captured[name] = re.sub(r"\s+", " ", kwargs["caller"]()).strip()
            return ""

        self.default_context["statement"] = fake_statement
        self.default_context["load_result"] = lambda name: mock.Mock()
        self.default_context["return"] = lambda value: value

        template = self.__get_template("adapters.sql")
        template.module.iomete__list_tables(relation)
        template.module.iomete__list_views(relation)
        return captured

    def test_list_tables_views_interpolate_schema_relation(self):
        # PR review concern: list_tables/list_views receive the schema Relation
        # and interpolate it directly into `show tables|views in {{ ... }}`.
        # Reproduce it for a schema-level relation (database + schema, no
        # identifier, as dbt passes to list_relations_without_caching) and check
        # the resulting namespace is valid Spark and not quoted / relation-formatted.
        from dbt.adapters.iomete import SparkRelation

        schema_relation = SparkRelation.create(database="my_catalog", schema="my_schema")

        captured = self._render_list_macros(schema_relation)

        self.assertEqual(captured["list_tables"], "show tables in my_catalog.my_schema")
        self.assertEqual(captured["list_views"], "show views in my_catalog.my_schema")

    def test_list_tables_views_without_catalog(self):
        # When no catalog is set the namespace is just the schema.
        from dbt.adapters.iomete import SparkRelation

        schema_relation = SparkRelation.create(schema="analytics")

        captured = self._render_list_macros(schema_relation)

        self.assertEqual(captured["list_tables"], "show tables in analytics")
        self.assertEqual(captured["list_views"], "show views in analytics")

    def test_macros_create_table_as(self):
        template = self.__get_template('adapters.sql')
        sql = self.__run_macro(template, 'iomete__create_table_as', False, 'my_table', 'select 1').strip()

        self.assertEqual(sql, "create or replace table my_table as select 1")

    def test_macros_create_table_as_file_format(self):
        template = self.__get_template('adapters.sql')

        self.config['file_format'] = 'iceberg'
        sql = self.__run_macro(template, 'iomete__create_table_as', False, 'my_table', 'select 1').strip()
        self.assertEqual(sql, "create or replace table my_table using iceberg as select 1")

    def test_macros_create_table_as_options(self):
        template = self.__get_template('adapters.sql')

        self.config['file_format'] = 'iceberg'
        self.config['options'] = {"compression": "gzip"}
        sql = self.__run_macro(template, 'iomete__create_table_as', False, 'my_table', 'select 1').strip()
        self.assertEqual(sql, 'create or replace table my_table using iceberg options (compression "gzip" ) as select 1')

    def test_macros_create_table_as_partition(self):
        template = self.__get_template('adapters.sql')

        self.config['partition_by'] = 'partition_1'
        sql = self.__run_macro(template, 'iomete__create_table_as', False, 'my_table', 'select 1').strip()
        self.assertEqual(sql, "create or replace table my_table partitioned by (partition_1) as select 1")

    def test_macros_create_table_as_partitions(self):
        template = self.__get_template('adapters.sql')

        self.config['partition_by'] = ['partition_1', 'partition_2']
        sql = self.__run_macro(template, 'iomete__create_table_as', False, 'my_table', 'select 1').strip()
        self.assertEqual(sql,
                         "create or replace table my_table partitioned by (partition_1,partition_2) as select 1")

    def test_macros_create_table_as_cluster(self):
        template = self.__get_template('adapters.sql')
        # TODO: Should raise error if clustered by used in iceberg tables
        self.config['file_format'] = 'parquet'
        self.config['clustered_by'] = 'cluster_1'
        self.config['buckets'] = '1'
        sql = self.__run_macro(template, 'iomete__create_table_as', False, 'my_table', 'select 1').strip()
        self.assertEqual(sql, "create table my_table using parquet clustered by (cluster_1) into 1 buckets as select 1")

    def test_macros_create_table_as_clusters(self):
        template = self.__get_template('adapters.sql')

        # TODO: Should raise error if clustered by used in iceberg tables
        self.config['file_format'] = 'parquet'
        self.config['clustered_by'] = ['cluster_1', 'cluster_2']
        self.config['buckets'] = '1'
        sql = self.__run_macro(template, 'iomete__create_table_as', False, 'my_table', 'select 1').strip()
        self.assertEqual(sql, "create table my_table using parquet clustered by (cluster_1,cluster_2) into 1 buckets as select 1")

    def test_macros_create_table_as_location(self):
        template = self.__get_template('adapters.sql')

        self.config['file_format'] = 'parquet'
        self.config['location_root'] = '/mnt/root'
        sql = self.__run_macro(template, 'iomete__create_table_as', False, 'my_table', 'select 1').strip()
        self.assertEqual(sql, "create table my_table using parquet location '/mnt/root/my_table' as select 1")

    def test_macros_create_table_as_comment(self):
        template = self.__get_template('adapters.sql')

        self.config['persist_docs'] = {'relation': True}
        self.default_context['model'].description = 'Description Test'
        sql = self.__run_macro(template, 'iomete__create_table_as', False, 'my_table', 'select 1').strip()
        self.assertEqual(sql, "create or replace table my_table comment 'Description Test' as select 1")

    def test_macros_create_table_as_all(self):
        template = self.__get_template('adapters.sql')

        self.config['file_format'] = 'parquet'
        self.config['location_root'] = '/mnt/root'
        self.config['partition_by'] = ['partition_1', 'partition_2']
        self.config['clustered_by'] = ['cluster_1', 'cluster_2']
        self.config['buckets'] = '1'
        self.config['persist_docs'] = {'relation': True}
        self.default_context['model'].description = 'Description Test'

        sql = self.__run_macro(template, 'iomete__create_table_as', False, 'my_table', 'select 1').strip()
        self.assertEqual(
            sql,
            "create table my_table using parquet partitioned by (partition_1,partition_2) clustered by (cluster_1,cluster_2) into 1 buckets location '/mnt/root/my_table' comment 'Description Test' as select 1"
        )

        self.config['file_format'] = 'iceberg'
        sql = self.__run_macro(template, 'iomete__create_table_as', False, 'my_table', 'select 1').strip()
        self.assertEqual(
            sql,
            "create or replace table my_table using iceberg partitioned by (partition_1,partition_2) clustered by (cluster_1,cluster_2) into 1 buckets location '/mnt/root/my_table' comment 'Description Test' as select 1"
        )


class _MacroReturn(Exception):
    def __init__(self, value):
        self.value = value


class _Relation(str):
    """Stands in for a dbt Relation: renders as its name and answers `include()`."""

    def include(self, **kwargs):
        return self


def _squash(sql):
    return re.sub(r'\s+', ' ', sql).strip()


class TestIncrementalStrategyMacros(unittest.TestCase):
    """Render the strategy macros without a cluster and check the Spark SQL they emit."""

    source = _Relation('my_source')
    target = _Relation('my_target')

    def setUp(self):
        self.jinja_env = Environment(loader=FileSystemLoader('dbt/include/iomete/macros'),
                                     extensions=['jinja2.ext.do', ])

        columns = {self.target: ['id', 'country', 'msg', 'only_in_target'],
                   self.source: ['id', 'country', 'msg']}

        def get_columns_in_relation(relation):
            # SparkColumn.quoted backticks the name; the macros interpolate that form.
            return [mock.Mock(quoted='`{}`'.format(name)) for name in columns[relation]]

        adapter = mock.Mock()
        adapter.get_columns_in_relation = get_columns_in_relation

        def _return(value):
            raise _MacroReturn(value)

        self.default_context = {
            'adapter': adapter,
            'config': mock.Mock(),
            'exceptions': mock.Mock(),
            'return': _return,
        }

    def _get_incremental_sql(self, strategy, unique_key=None, incremental_predicates=None):
        template = self.jinja_env.get_template(
            'materializations/incremental/strategies.sql', globals=self.default_context)
        try:
            rendered = template.module.dbt_iomete_get_incremental_sql(
                strategy, self.source, self.target, unique_key, incremental_predicates)
        except _MacroReturn as macro_return:
            return [_squash(sql) for sql in macro_return.value]
        return _squash(rendered)

    @property
    def expected_insert(self):
        return ('insert into table my_target (`id`, `country`, `msg`, `only_in_target`) '
                'select `id`, `country`, `msg`, NULL AS `only_in_target` from my_source')

    def test_delete_insert_unique_key(self):
        statements = self._get_incremental_sql('delete+insert', unique_key='id')

        self.assertEqual(statements, [
            'delete from my_target as DBT_INTERNAL_DEST where exists ( '
            'select 1 from my_source as DBT_INTERNAL_SOURCE '
            'where DBT_INTERNAL_DEST.id <=> DBT_INTERNAL_SOURCE.id )',
            self.expected_insert,
        ])

    def test_delete_insert_composite_unique_key(self):
        statements = self._get_incremental_sql('delete+insert', unique_key=['id', 'country'])

        self.assertEqual(statements[0],
                         'delete from my_target as DBT_INTERNAL_DEST where exists ( '
                         'select 1 from my_source as DBT_INTERNAL_SOURCE '
                         'where DBT_INTERNAL_DEST.id <=> DBT_INTERNAL_SOURCE.id '
                         'and DBT_INTERNAL_DEST.country <=> DBT_INTERNAL_SOURCE.country )')

    def test_delete_insert_without_unique_key_only_inserts(self):
        statements = self._get_incremental_sql('delete+insert')

        self.assertEqual(statements, [self.expected_insert])

    def test_delete_insert_predicates_apply_to_delete_only(self):
        statements = self._get_incremental_sql(
            'delete+insert', unique_key='id',
            incremental_predicates=["DBT_INTERNAL_DEST.day >= '2024-01-01'"])

        self.assertTrue(statements[0].endswith("and DBT_INTERNAL_DEST.day >= '2024-01-01'"),
                        statements[0])
        self.assertEqual(statements[1], self.expected_insert)

    def test_append_returns_a_single_statement(self):
        self.assertEqual(self._get_incremental_sql('append'), self.expected_insert)


class TestIncrementalStrategyValidation(unittest.TestCase):
    """The compile-time gate: which strategies are accepted, and what the error names."""

    def setUp(self):
        self.jinja_env = Environment(loader=FileSystemLoader('dbt/include/iomete/macros'),
                                     extensions=['jinja2.ext.do', ])
        self.exceptions = mock.Mock()
        self.template = self.jinja_env.get_template(
            'materializations/incremental/validate.sql',
            globals={'exceptions': self.exceptions, 'return': lambda value: value})

    def _validate(self, raw_strategy, file_format='iceberg'):
        self.template.module.dbt_iomete_validate_get_incremental_strategy(raw_strategy, file_format)
        if not self.exceptions.raise_compiler_error.called:
            return None
        return _squash(self.exceptions.raise_compiler_error.call_args[0][0])

    def test_accepted_strategies_do_not_raise(self):
        for strategy in ['append', 'merge', 'delete+insert']:
            with self.subTest(strategy=strategy):
                self.exceptions.reset_mock()
                self.assertIsNone(self._validate(strategy))

    def test_unknown_strategy_error_lists_the_supported_strategies(self):
        message = self._validate('something_else')

        self.assertIn("Invalid incremental strategy provided: something_else", message)
        self.assertIn("Expected one of: 'append', 'merge', 'delete+insert'", message)

    def test_unknown_strategy_error_does_not_offer_insert_overwrite(self):
        self.assertNotIn('insert_overwrite', self._validate('something_else'))

    def test_insert_overwrite_is_still_rejected_for_iceberg(self):
        self.assertIn('You cannot use this strategy when file_format is set to',
                      self._validate('insert_overwrite'))
