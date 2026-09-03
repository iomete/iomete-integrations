import pytest
from dbt.tests.adapter.incremental.test_incremental_on_schema_change import (
    BaseIncrementalOnSchemaChange
)
from dbt.tests.adapter.incremental.fixtures import (
    _MODELS__INCREMENTAL_SYNC_REMOVE_ONLY,
    _MODELS__INCREMENTAL_IGNORE,
    _MODELS__INCREMENTAL_SYNC_REMOVE_ONLY_TARGET,
    _MODELS__INCREMENTAL_IGNORE_TARGET,
    _MODELS__INCREMENTAL_FAIL,
    _MODELS__INCREMENTAL_SYNC_ALL_COLUMNS,
    _MODELS__INCREMENTAL_APPEND_NEW_COLUMNS_REMOVE_ONE,
    _MODELS__A,
    _MODELS__INCREMENTAL_APPEND_NEW_COLUMNS_TARGET,
    _MODELS__INCREMENTAL_APPEND_NEW_COLUMNS,
    _MODELS__INCREMENTAL_SYNC_ALL_COLUMNS_TARGET,
    _MODELS__INCREMENTAL_APPEND_NEW_COLUMNS_REMOVE_ONE_TARGET
)
from dbt.tests.util import (
    check_relations_equal,
    run_dbt
)


class TestIncrementalMergeOnSchemaChange(BaseIncrementalOnSchemaChange):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+incremental_strategy": "merge",
                "+file_format": "iceberg",
            }
        }


# Model names carry a per-strategy prefix so the classes do not collide under xdist.
class BaseRenamedOnSchemaChange:
    prefix: str
    strategy: str

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+incremental_strategy": self.strategy,
                "+file_format": "iceberg",
            }
        }

    @pytest.fixture(scope="class")
    def models(self):
        p = self.prefix
        return {
            f"{p}_sync_remove_only.sql": _MODELS__INCREMENTAL_SYNC_REMOVE_ONLY,
            f"{p}_ignore.sql": _MODELS__INCREMENTAL_IGNORE,
            f"{p}_sync_remove_only_target.sql": _MODELS__INCREMENTAL_SYNC_REMOVE_ONLY_TARGET,
            f"{p}_ignore_target.sql": _MODELS__INCREMENTAL_IGNORE_TARGET,
            f"{p}_fail.sql": _MODELS__INCREMENTAL_FAIL,
            f"{p}_sync_all_columns.sql": _MODELS__INCREMENTAL_SYNC_ALL_COLUMNS,
            f"{p}_append_new_columns_remove_one.sql": _MODELS__INCREMENTAL_APPEND_NEW_COLUMNS_REMOVE_ONE,
            "model_a.sql": _MODELS__A,
            f"{p}_append_new_columns_target.sql": _MODELS__INCREMENTAL_APPEND_NEW_COLUMNS_TARGET,
            f"{p}_append_new_columns.sql": _MODELS__INCREMENTAL_APPEND_NEW_COLUMNS,
            f"{p}_sync_all_columns_target.sql": _MODELS__INCREMENTAL_SYNC_ALL_COLUMNS_TARGET,
            f"{p}_append_new_columns_remove_one_target.sql": _MODELS__INCREMENTAL_APPEND_NEW_COLUMNS_REMOVE_ONE_TARGET,
        }

    def run_twice_and_assert(self, include, compare_source, compare_target, project):
        # dbt run (twice)
        run_args = ["run"]
        if include:
            run_args.extend(("--select", include))
        results_one = run_dbt(run_args)
        assert len(results_one) == 3

        results_two = run_dbt(run_args)
        assert len(results_two) == 3

        check_relations_equal(project.adapter, [compare_source, compare_target])

    def run_case(self, case, project):
        model = f"{self.prefix}_{case}"
        target = f"{model}_target"
        self.run_twice_and_assert(f"model_a {model} {target}", model, target, project)

    def test_run_incremental_ignore(self, project):
        self.run_case("ignore", project)

    def test_run_incremental_append_new_columns(self, project):
        self.run_case("append_new_columns", project)
        self.run_case("append_new_columns_remove_one", project)

    def test_run_incremental_sync_all_columns(self, project):
        self.run_case("sync_all_columns", project)
        self.run_case("sync_remove_only", project)

    def test_run_incremental_fail_on_schema_change(self, project):
        select = f"model_a {self.prefix}_fail"
        run_dbt(["run", "--models", select, "--full-refresh"])
        results_two = run_dbt(["run", "--models", select], expect_pass=False)
        assert "Compilation Error" in results_two[1].message


class TestIncrementalAppendOnSchemaChange(BaseRenamedOnSchemaChange):
    prefix = "incremental_append"
    strategy = "append"


class TestIncrementalDeleteInsertOnSchemaChange(BaseRenamedOnSchemaChange):
    prefix = "incremental_delete_insert"
    strategy = "delete+insert"
