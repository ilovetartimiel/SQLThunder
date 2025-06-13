### --- Third-party imports --- ###
import pandas as pd
import pytest

### --- Internal package imports --- ###
from SQLThunder.exceptions.execution import (
    BadArgumentsBulk,
    UnsupportedDuplicateHandling,
    UnsupportedMultiThreadedDatabase,
)

### --- Test Insert_many --- ###


class TestInsertMany:

    @pytest.mark.parametrize(
        "db_client",
        [
            {"db": "sqlite"},
            {"db": "mysql"},
            {"db": "postgres"},
        ],
        indirect=True,
    )
    def test_basic_insert(self, db_client, setup_test_table, truncate_test_table):
        df = pd.DataFrame(
            [
                {"id": 1, "name": "Alice", "value": 10.5, "created_at": "2024-01-01"},
                {"id": 2, "name": "Bob", "value": 20.2, "created_at": "2024-01-02"},
            ]
        )
        res = db_client.insert_many(df, setup_test_table)
        assert res[0].empty

        out = db_client.query(f"SELECT * FROM {setup_test_table}")
        assert len(out) == 2
        assert sorted(out["name"].tolist()) == ["Alice", "Bob"]

    @pytest.mark.parametrize(
        "db_client",
        [
            {"db": "sqlite"},
            {"db": "mysql"},
            {"db": "postgres"},
        ],
        indirect=True,
    )
    @pytest.mark.parametrize("mode", ["ignore", "replace"])
    def test_on_duplicate_modes(
        self, db_client, setup_test_table, truncate_test_table, mode
    ):
        df1 = pd.DataFrame(
            [{"id": 1, "name": "X", "value": 1.0, "created_at": "2024-01-01"}]
        )
        df2 = pd.DataFrame(
            [{"id": 1, "name": "Y", "value": 2.0, "created_at": "2024-01-02"}]
        )
        db_client.insert_many(df1, setup_test_table)

        if db_client._db_type == "postgresql" and mode == "replace":
            with pytest.raises(UnsupportedDuplicateHandling):
                db_client.insert_batch(df2, setup_test_table, on_duplicate=mode)
            return

        res = db_client.insert_many(df2, setup_test_table, on_duplicate=mode)
        assert res[0].empty
        out = db_client.query(f"SELECT * FROM {setup_test_table}")
        assert len(out) == 1
        assert out.iloc[0]["name"] == ("X" if mode == "ignore" else "Y")

    @pytest.mark.parametrize(
        "db_client",
        [
            {"db": "sqlite"},
            {"db": "mysql"},
            {"db": "postgres"},
        ],
        indirect=True,
    )
    def test_duplicate_fails_without_on_duplicate(
        self, db_client, setup_test_table, truncate_test_table
    ):
        df1 = pd.DataFrame(
            [{"id": 1, "name": "A", "value": 1.0, "created_at": "2024-01-01"}]
        )
        df2 = pd.DataFrame(
            [{"id": 1, "name": "B", "value": 2.0, "created_at": "2024-01-02"}]
        )
        db_client.insert_many(df1, setup_test_table)
        failures, _ = db_client.insert_many(df2, setup_test_table, return_failures=True)

        assert isinstance(failures, pd.DataFrame)
        assert len(failures) == 1
        assert failures.iloc[0]["id"] == 1
        assert "error_message" in failures.columns

    @pytest.mark.parametrize(
        "db_client",
        [
            {"db": "sqlite"},
            {"db": "mysql"},
            {"db": "postgres"},
        ],
        indirect=True,
    )
    def test_empty_dataframe_raises(
        self, db_client, setup_test_table, truncate_test_table
    ):
        empty_df = pd.DataFrame(columns=["id", "name", "value", "created_at"])
        with pytest.raises(BadArgumentsBulk):
            db_client.insert_many(empty_df, setup_test_table)

    @pytest.mark.parametrize(
        "db_client",
        [
            {"db": "sqlite"},
            {"db": "mysql"},
            {"db": "postgres"},
        ],
        indirect=True,
    )
    def test_malformed_dataframe_returns_error(
        self, db_client, setup_test_table, truncate_test_table
    ):
        malformed_df = pd.DataFrame([{"wrong_column": 1}])
        res = db_client.insert_many(malformed_df, setup_test_table)
        assert len(res[0]) == 1


### --- Test Insert_batch --- ###


class TestInsertBatch:

    @pytest.mark.parametrize(
        "db_client",
        [
            {"db": "mysql"},
            {"db": "postgres"},
        ],
        indirect=True,
    )
    def test_large_dataset_success(
        self, db_client, setup_test_table, truncate_test_table, large_dataframe
    ):
        failures, success = db_client.insert_batch(
            large_dataframe,
            table_name=setup_test_table,
            chunk_size=5000,
            return_failures=True,
            return_status=True,
        )
        assert success is True
        assert failures is None or failures.empty

        count = db_client.query(
            f"SELECT COUNT(*) as count FROM {setup_test_table}", return_type="list"
        )[0]["count"]
        assert count == 100_000

    @pytest.mark.parametrize(
        "db_client",
        [
            {"db": "mysql"},
            {"db": "postgres"},
        ],
        indirect=True,
    )
    @pytest.mark.parametrize("mode", ["ignore", "replace"])
    def test_on_duplicate_modes(
        self, db_client, setup_test_table, truncate_test_table, mode
    ):
        df1 = pd.DataFrame(
            [{"id": 10, "name": "Foo", "value": 5.0, "created_at": "2024-01-01"}]
        )
        df2 = pd.DataFrame(
            [{"id": 10, "name": "Bar", "value": 8.8, "created_at": "2024-01-02"}]
        )
        db_client.insert_batch(df1, setup_test_table)

        if db_client._db_type == "postgresql" and mode == "replace":
            with pytest.raises(UnsupportedDuplicateHandling):
                db_client.insert_batch(df2, setup_test_table, on_duplicate=mode)
            return

        failures, success = db_client.insert_batch(
            df2,
            setup_test_table,
            on_duplicate=mode,
            return_failures=True,
            return_status=True,
        )
        assert success is True
        assert failures is None or failures.empty

        out = db_client.query(
            f"SELECT * FROM {setup_test_table} WHERE id = 10", return_type="list"
        )[0]
        assert out["name"] == ("Foo" if mode == "ignore" else "Bar")

    @pytest.mark.parametrize(
        "db_client",
        [
            {"db": "mysql"},
            {"db": "postgres"},
        ],
        indirect=True,
    )
    def test_duplicate_fails_without_on_duplicate(
        self, db_client, setup_test_table, truncate_test_table
    ):
        df1 = pd.DataFrame(
            [{"id": 999, "name": "A", "value": 1.0, "created_at": "2024-01-01"}]
        )
        df2 = pd.DataFrame(
            [{"id": 999, "name": "B", "value": 2.0, "created_at": "2024-01-02"}]
        )
        db_client.insert_batch(df1, setup_test_table)
        failures, success = db_client.insert_batch(
            df2, setup_test_table, return_failures=True, return_status=True
        )

        assert success is False
        assert isinstance(failures, pd.DataFrame)
        assert len(failures) == 1
        assert failures.iloc[0]["id"] == 999
        assert "error_message" in failures.columns

    @pytest.mark.parametrize("db_client", [{"db": "sqlite"}], indirect=True)
    def test_raises_on_sqlite(
        self, db_client, setup_test_table, truncate_test_table, large_dataframe
    ):
        with pytest.raises(UnsupportedMultiThreadedDatabase):
            db_client.insert_batch(large_dataframe, setup_test_table)
