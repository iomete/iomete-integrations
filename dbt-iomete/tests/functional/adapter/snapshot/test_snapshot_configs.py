"""Live tests for the snapshot configs added in dbt 1.9: snapshot_meta_column_names,
dbt_valid_to_current, a list unique_key and hard_deletes: new_record.

They follow dbt.tests.adapter.simple_snapshot, with Spark SQL fixtures. Where the upstream
class reads its SQL from fixtures, it is subclassed; otherwise its test is reproduced here.
"""
import pytest
from dbt.tests.adapter.simple_snapshot.new_record_dbt_valid_to_current import (
    BaseSnapshotNewRecordDbtValidToCurrent,
)
from dbt.tests.adapter.simple_snapshot.new_record_timestamp_mode import (
    BaseSnapshotNewRecordTimestampMode,
)
from dbt.tests.util import check_relations_equal, run_dbt, run_dbt_and_capture, update_config_file

from tests.functional.adapter.snapshot import fixtures

_custom_column_names = {
    "dbt_valid_to": "test_valid_to",
    "dbt_valid_from": "test_valid_from",
    "dbt_scd_id": "test_scd_id",
    "dbt_updated_at": "test_updated_at",
}


def _snapshot_and_update(project, invalidate_sql_statements, update_sql):
    results = run_dbt(["snapshot"])
    assert len(results) == 1

    for sql in invalidate_sql_statements:
        project.run_sql(sql)
    project.run_sql(update_sql)

    results = run_dbt(["snapshot"])
    assert len(results) == 1


class BaseSnapshotProject:
    @pytest.fixture(scope="class")
    def snapshots(self):
        return {"snapshot.sql": fixtures.snapshot_actual_sql}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "snapshots.yml": fixtures.snapshots_yml,
            "ref_snapshot.sql": fixtures.ref_snapshot_sql,
        }


class ColumnNamesFromDbtProject:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "snapshots.yml": fixtures.snapshots_no_column_names_yml,
            "ref_snapshot.sql": fixtures.ref_snapshot_sql,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"snapshots": {"test": {"+snapshot_meta_column_names": _custom_column_names}}}


class TestSnapshotColumnNames(BaseSnapshotProject):
    def test_snapshot_column_names(self, project):
        project.run_sql(fixtures.create_seed_sql)
        project.run_sql(fixtures.create_snapshot_expected_sql)
        project.run_sql(fixtures.seed_insert_sql)
        project.run_sql(fixtures.populate_snapshot_expected_sql)

        _snapshot_and_update(project, fixtures.invalidate_sql_statements, fixtures.update_sql)

        check_relations_equal(project.adapter, ["snapshot_actual", "snapshot_expected"])


class TestSnapshotColumnNamesFromDbtProject(ColumnNamesFromDbtProject, TestSnapshotColumnNames):
    pass


class TestSnapshotInvalidColumnNames(ColumnNamesFromDbtProject, BaseSnapshotProject):
    def test_snapshot_invalid_column_names(self, project):
        project.run_sql(fixtures.create_seed_sql)
        project.run_sql(fixtures.seed_insert_sql)

        results = run_dbt(["snapshot"])
        assert len(results) == 1

        # dbt_valid_from and dbt_scd_id fall back to their default names, which the
        # existing snapshot table does not have
        different_columns = {
            "snapshots": {
                "test": {
                    "+snapshot_meta_column_names": {
                        "dbt_valid_to": "test_valid_to",
                        "dbt_updated_at": "test_updated_at",
                    }
                }
            }
        }
        update_config_file(different_columns, "dbt_project.yml")

        results, log_output = run_dbt_and_capture(["snapshot"], expect_pass=False)
        assert len(results) == 1
        assert "Compilation Error in snapshot snapshot_actual" in log_output
        assert "Snapshot target is missing configured columns" in log_output


class TestSnapshotDbtValidToCurrent(BaseSnapshotProject):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "snapshots.yml": fixtures.snapshots_valid_to_current_yml,
            "ref_snapshot.sql": fixtures.ref_snapshot_sql,
        }

    @staticmethod
    def _valid_to_by_id(project):
        # Spark returns rows in no particular order, so sort by id and version
        rows = project.run_sql(
            "select id, cast(TEST_VALID_TO as string) from {schema}.snapshot_actual"
            " order by id, test_valid_from",
            fetch="all",
        )
        valid_to = {}
        for id_, value in rows:
            valid_to.setdefault(id_, []).append(value)
        return valid_to

    def test_valid_to_current(self, project):
        current = "2099-12-31 00:00:00"
        project.run_sql(fixtures.create_seed_sql)
        project.run_sql(fixtures.create_snapshot_expected_sql)
        project.run_sql(fixtures.seed_insert_sql)
        project.run_sql(fixtures.populate_snapshot_expected_valid_to_current_sql)

        results = run_dbt(["snapshot"])
        assert len(results) == 1

        valid_to = self._valid_to_by_id(project)
        assert len(valid_to) == 20
        assert all(versions == [current] for versions in valid_to.values())

        for sql in fixtures.invalidate_sql_statements:
            project.run_sql(sql)
        project.run_sql(fixtures.update_with_current_sql)

        results = run_dbt(["snapshot"])
        assert len(results) == 1

        valid_to = self._valid_to_by_id(project)
        assert valid_to[1] == [current]
        # The replaced version of row 10 is closed at its new updated_at, and the new one is current
        assert valid_to[10] == ["2016-08-20 16:44:49", current]

        check_relations_equal(project.adapter, ["snapshot_actual", "snapshot_expected"])


# Also covers snapshot_meta_column_names and a yaml-only snapshot definition
class TestSnapshotMultiUniqueKey:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "seed.sql": fixtures.model_seed_sql,
            "snapshots.yml": fixtures.snapshots_multi_key_yml,
            "ref_snapshot.sql": fixtures.ref_snapshot_sql,
        }

    def test_multi_column_unique_key(self, project):
        project.run_sql(fixtures.create_multi_key_seed_sql)
        project.run_sql(fixtures.create_multi_key_snapshot_expected_sql)
        project.run_sql(fixtures.seed_multi_key_insert_sql)
        project.run_sql(fixtures.populate_multi_key_snapshot_expected_sql)

        _snapshot_and_update(
            project, fixtures.invalidate_multi_key_sql_statements, fixtures.update_multi_key_sql
        )

        check_relations_equal(project.adapter, ["snapshot_actual", "snapshot_expected"])


class TestSnapshotNewRecordTimestampMode(BaseSnapshotNewRecordTimestampMode):
    @pytest.fixture(scope="class")
    def snapshots(self):
        return {"snapshot.sql": fixtures.snapshot_actual_sql}

    @pytest.fixture(scope="class")
    def seed_new_record_mode_statements(self):
        return fixtures.new_record_seed_statements

    @pytest.fixture(scope="class")
    def invalidate_sql_statements(self):
        return fixtures.new_record_invalidate_sql_statements

    @pytest.fixture(scope="class")
    def update_sql(self):
        return fixtures.new_record_update_sql

    @pytest.fixture(scope="class")
    def delete_sql(self):
        return fixtures.new_record_delete_sql

    @pytest.fixture(scope="class")
    def reinsert_sql(self):
        return fixtures.new_record_reinsert_sql


class TestSnapshotNewRecordDbtValidToCurrent(BaseSnapshotNewRecordDbtValidToCurrent):
    @pytest.fixture(scope="class")
    def seed_new_record_mode_statements(self):
        return fixtures.new_record_valid_to_current_seed_statements

    @pytest.fixture(scope="class")
    def delete_sql(self):
        return fixtures.new_record_delete_sql
