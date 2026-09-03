import pytest
from dbt.tests.adapter.incremental.test_incremental_unique_id import BaseIncrementalUniqueKey
from tests.functional.adapter.incremental import fixtures


class BaseIcebergUniqueKey(BaseIncrementalUniqueKey):
    strategy: str

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+file_format": "iceberg",
                "+incremental_strategy": self.strategy,
            }
        }

    # Override seeds fixture to handle Iceberg's strict type conversion (String to DATE not allowed)
    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "duplicate_insert.sql": fixtures.seeds__duplicate_insert_sql,
            "seed.csv": fixtures.seeds__seed_csv,
            "add_new_rows.sql": fixtures.seeds__add_new_rows_sql,
        }


class TestIncrementalUniqueKey(BaseIcebergUniqueKey):
    strategy = "merge"


class TestDeleteInsertUniqueKey(BaseIcebergUniqueKey):
    """Covers the composite-key case against data: the trinary-key tests fail if the
    delete matches key values across different source rows instead of per row."""

    strategy = "delete+insert"
