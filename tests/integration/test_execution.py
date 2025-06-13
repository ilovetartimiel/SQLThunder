### --- Third-party imports --- ###
import pandas as pd
import pytest

### --- Internal package imports --- ###
from SQLThunder.exceptions.execution import (
    BadArgumentsBulk,
    UnsupportedMultiThreadedDatabase,
)

### --- Test Execute --- ###


class TestExecute:

    @pytest.mark.parametrize(
        "db_client",
        [{"db": "sqlite"}, {"db": "mysql"}, {"db": "postgres"}],
        indirect=True,
    )
    def test_execute_insert_single_row(
        self, db_client, setup_test_table, truncate_test_table
    ):
        sql = f"INSERT INTO {setup_test_table} (id, name, value, created_at) VALUES (:id, :name, :value, :created_at)"
        row = {"id": 1, "name": "Test", "value": 1.23, "created_at": "2024-01-01"}
        res = db_client.execute(sql, args=row)
        assert res[0].empty
        out = db_client.query(
            f"SELECT * FROM {setup_test_table} WHERE id = 1", return_type="list"
        )[0]
        assert out["name"] == "Test"

    @pytest.mark.parametrize(
        "db_client",
        [{"db": "sqlite"}, {"db": "mysql"}, {"db": "postgres"}],
        indirect=True,
    )
    def test_execute_duplicate_ignore(
        self, db_client, setup_test_table, truncate_test_table
    ):
        sql = f"INSERT INTO {setup_test_table} VALUES (:id, :name, :value, :created_at)"
        row = {"id": 1, "name": "A", "value": 1.0, "created_at": "2024-01-01"}
        db_client.execute(sql, args=row)
        db_client.execute(sql, args=row | {"name": "B"}, on_duplicate="ignore")
        out = db_client.query(f"SELECT * FROM {setup_test_table}", return_type="list")
        assert len(out) == 1
        assert out[0]["name"] == "A"

    @pytest.mark.parametrize(
        "db_client",
        [{"db": "sqlite"}, {"db": "mysql"}, {"db": "postgres"}],
        indirect=True,
    )
    def test_execute_malformed_sql(self, db_client, setup_test_table):
        sql = f"INSER INTO {setup_test_table} VALUES (:id, :name, :value, :created_at)"
        res = db_client.execute(
            sql, args={"id": 1, "name": "A", "value": 1, "created_at": "2024-01-01"}
        )
        assert len(res[0]) == 1


### --- Test Execute_many --- ###


class TestExecuteMany:

    @pytest.mark.parametrize(
        "db_client",
        [{"db": "sqlite"}, {"db": "mysql"}, {"db": "postgres"}],
        indirect=True,
    )
    def test_execute_many_success(
        self, db_client, setup_test_table, truncate_test_table
    ):
        sql = f"INSERT INTO {setup_test_table} VALUES (:id, :name, :value, :created_at)"
        rows = [
            {"id": 10, "name": "A", "value": 100, "created_at": "2024-01-01"},
            {"id": 11, "name": "B", "value": 200, "created_at": "2024-01-02"},
        ]
        res = db_client.execute_many(sql, args=rows)
        assert res[0].empty
        count = db_client.query(
            f"SELECT COUNT(*) as count FROM {setup_test_table}", return_type="list"
        )[0]["count"]
        assert count == 2

    @pytest.mark.parametrize(
        "db_client",
        [{"db": "sqlite"}, {"db": "mysql"}, {"db": "postgres"}],
        indirect=True,
    )
    def test_execute_many_duplicates_with_failure_return(
        self, db_client, setup_test_table, truncate_test_table
    ):
        sql = f"INSERT INTO {setup_test_table} VALUES (:id, :name, :value, :created_at)"
        rows = [
            {"id": 20, "name": "X", "value": 1, "created_at": "2024-01-01"},
            {"id": 20, "name": "Y", "value": 2, "created_at": "2024-01-02"},
        ]
        failure_df, _ = db_client.execute_many(sql, args=rows, return_failures=True)
        assert isinstance(failure_df, pd.DataFrame)
        assert "error_message" in failure_df.columns

    @pytest.mark.parametrize(
        "db_client",
        [{"db": "sqlite"}, {"db": "mysql"}, {"db": "postgres"}],
        indirect=True,
    )
    def test_execute_many_empty_args_raises(self, db_client, setup_test_table):
        sql = f"INSERT INTO {setup_test_table} VALUES (:id, :name, :value, :created_at)"
        with pytest.raises(BadArgumentsBulk):
            db_client.execute_many(sql, args=[])


### --- Test Execute_batch --- ###


class TestExecuteBatch:

    @pytest.mark.parametrize(
        "db_client", [{"db": "mysql"}, {"db": "postgres"}], indirect=True
    )
    def test_execute_batch_large_dataset(
        self, db_client, setup_test_table, truncate_test_table, large_dataframe
    ):
        sql = f"INSERT INTO {setup_test_table} (id, name, value, created_at) VALUES (:id, :name, :value, :created_at)"
        failures, success = db_client.execute_batch(
            sql,
            args=large_dataframe.to_dict(orient="records"),
            chunk_size=5000,
            return_failures=True,
            return_status=True,
        )
        assert success is True
        assert failures is None or failures.empty
        count = db_client.query(
            f"SELECT COUNT(*) as count FROM {setup_test_table}", return_type="list"
        )[0]["count"]
        assert count == len(large_dataframe)

    @pytest.mark.parametrize(
        "db_client", [{"db": "mysql"}, {"db": "postgres"}], indirect=True
    )
    def test_execute_batch_large_dataset_ignore_duplicates(
        self, db_client, setup_test_table, truncate_test_table, large_dataframe
    ):
        df = large_dataframe.iloc[:10000].copy()
        db_client.insert_batch(df, setup_test_table, chunk_size=1000)
        sql = f"INSERT INTO {setup_test_table} (id, name, value, created_at) VALUES (:id, :name, :value, :created_at)"
        args = df.to_dict(orient="records")
        failures, success = db_client.execute_batch(
            sql,
            args=args,
            chunk_size=1000,
            on_duplicate="ignore",
            return_failures=True,
            return_status=True,
        )
        assert success is True
        assert failures is None or failures.empty
        count = db_client.query(
            f"SELECT COUNT(*) as count FROM {setup_test_table}", return_type="list"
        )[0]["count"]
        assert count == 10000

    @pytest.mark.parametrize(
        "db_client", [{"db": "mysql"}, {"db": "postgres"}], indirect=True
    )
    def test_execute_batch_large_dataset_with_duplicates(
        self, db_client, setup_test_table, truncate_test_table, large_dataframe
    ):
        df = large_dataframe.iloc[:10000].copy()
        duplicates = df.iloc[:5].copy()
        df_with_dupes = pd.concat([df, duplicates], ignore_index=True)
        sql = f"INSERT INTO {setup_test_table} (id, name, value, created_at) VALUES (:id, :name, :value, :created_at)"
        failures, success = db_client.execute_batch(
            sql,
            args=df_with_dupes.to_dict(orient="records"),
            chunk_size=1000,
            return_failures=True,
            return_status=True,
        )
        assert success is False
        assert isinstance(failures, pd.DataFrame)
        assert "error_message" in failures.columns
        assert len(failures) >= 5

    @pytest.mark.parametrize("db_client", [{"db": "sqlite"}], indirect=True)
    def test_execute_batch_raises_on_sqlite(self, db_client, setup_test_table):
        sql = f"INSERT INTO {setup_test_table} VALUES (:id, :name, :value, :created_at)"
        rows = [{"id": 1, "name": "X", "value": 10, "created_at": "2024-01-01"}]
        with pytest.raises(UnsupportedMultiThreadedDatabase):
            db_client.execute_batch(sql, args=rows)
