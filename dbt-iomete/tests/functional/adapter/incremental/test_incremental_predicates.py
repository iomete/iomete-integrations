import pytest
from dbt.tests.adapter.incremental.test_incremental_predicates import BaseIncrementalPredicates


class BaseIcebergPredicates(BaseIncrementalPredicates):
    strategy: str
    config_key: str

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+{}".format(self.config_key): ["dbt_internal_dest.id != 2"],
                "+incremental_strategy": self.strategy,
                "+file_format": "iceberg"
            }
        }


class TestIncrementalPredicatesMerge(BaseIcebergPredicates):
    strategy = "merge"
    config_key = "incremental_predicates"


class TestPredicatesMerge(BaseIcebergPredicates):
    strategy = "merge"
    config_key = "predicates"


class TestIncrementalPredicatesDeleteInsert(BaseIcebergPredicates):
    strategy = "delete+insert"
    config_key = "incremental_predicates"


class TestPredicatesDeleteInsert(BaseIcebergPredicates):
    strategy = "delete+insert"
    config_key = "predicates"
