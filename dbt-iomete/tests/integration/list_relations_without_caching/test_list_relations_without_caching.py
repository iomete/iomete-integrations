from tests.integration.base import DBTIntegrationTest


class TestListRelationsWithoutCaching(DBTIntegrationTest):
    @property
    def schema(self):
        return "list_relations_without_caching"

    @property
    def models(self):
        return "models"

    def test_empty_list_for_nonexistent_schema(self):
        # `list_relations_without_caching` must return an empty list (not raise)
        # when the schema does not exist. dbt relies on this while caching
        # schemas it is about to create.
        schema_relation = self.adapter.Relation.create(
            database=self.default_database,
            schema="nonexistent_schema_do_not_create",
            identifier="",
            quote_policy=self.config.quoting,
        ).without_identifier()

        with self.get_connection():
            relations = self.adapter.list_relations_without_caching(schema_relation)

        self.assertEqual(relations, [])