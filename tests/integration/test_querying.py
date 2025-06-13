### --- Third-party imports --- ###
import pandas as pd
import pytest

### --- Internal package imports --- ###
from SQLThunder.exceptions.execution import (
    QueryDisallowedClauseError,
    QueryResultFormatError,
    QuerySelectOnlyError,
    UnsupportedMultiThreadedDatabase,
)

### --- Fixtures --- ###


@pytest.fixture(scope="module", autouse=True)
def populate_test_table_once(db_client, setup_test_table, large_dataframe):
    """
    Populate the shared test table once for all query tests, then truncate or delete it after.
    """
    db_client.insert_many(large_dataframe, setup_test_table)
    yield
    db_type = db_client._db_type
    if db_type in ("mysql", "postgresql"):
        db_client.execute(f"TRUNCATE TABLE {setup_test_table}")
    else:
        db_client.execute(f"DELETE FROM {setup_test_table}")


### --- Test Query --- ###


class TestQuery:

    @pytest.mark.parametrize(
        "db_client",
        [{"db": db} for db in ("sqlite", "mysql", "postgres")],
        indirect=True,
    )
    def test_query_basic_return_df(self, db_client, setup_test_table):
        res = db_client.query(
            f"SELECT * FROM {setup_test_table} WHERE id < 10", return_type="df"
        )
        assert isinstance(res, pd.DataFrame)
        assert len(res) == 10
        assert "name" in res.columns

    @pytest.mark.parametrize(
        "db_client",
        [{"db": db} for db in ("sqlite", "mysql", "postgres")],
        indirect=True,
    )
    def test_query_return_type_list(self, db_client, setup_test_table):
        res = db_client.query(
            f"SELECT * FROM {setup_test_table} WHERE id BETWEEN 10 AND 19",
            return_type="list",
        )
        assert isinstance(res, list)
        assert isinstance(res[0], dict)
        assert len(res) == 10

    @pytest.mark.parametrize(
        "db_client",
        [{"db": db} for db in ("sqlite", "mysql", "postgres")],
        indirect=True,
    )
    def test_query_return_type_raw(self, db_client, setup_test_table):
        res = db_client.query(
            f"SELECT * FROM {setup_test_table} WHERE id BETWEEN 20 AND 29",
            return_type="raw",
        )
        assert isinstance(res, list)
        assert hasattr(res[0], "_mapping")
        assert res[0][0] == 20

    @pytest.mark.parametrize(
        "db_client",
        [{"db": db} for db in ("sqlite", "mysql", "postgres")],
        indirect=True,
    )
    def test_query_print_only(self, db_client, setup_test_table):
        res = db_client.query(
            f"SELECT * FROM {setup_test_table} WHERE id < 5",
            return_type="None",
            print_result=True,
        )
        assert res is None

    @pytest.mark.parametrize(
        "db_client",
        [{"db": db} for db in ("sqlite", "mysql", "postgres")],
        indirect=True,
    )
    def test_query_named_args(self, db_client, setup_test_table):
        res = db_client.query(
            f"SELECT * FROM {setup_test_table} WHERE id = :id",
            args={"id": 123},
            return_type="df",
        )
        assert isinstance(res, pd.DataFrame)
        assert len(res) <= 1

    @pytest.mark.parametrize(
        "db_client",
        [{"db": db} for db in ("sqlite", "mysql", "postgres")],
        indirect=True,
    )
    def test_query_positional_args(self, db_client, setup_test_table):
        res = db_client.query(
            f"SELECT * FROM {setup_test_table} WHERE id = :param1",
            args=(456,),
            return_type="df",
        )
        assert isinstance(res, pd.DataFrame)

    @pytest.mark.parametrize(
        "db_client",
        [{"db": db} for db in ("sqlite", "mysql", "postgres")],
        indirect=True,
    )
    def test_query_invalid_sql_raises(self, db_client):
        with pytest.raises(QuerySelectOnlyError):
            db_client.query("SELEC * FROM non_existing", return_type="df")

    @pytest.mark.parametrize(
        "db_client",
        [{"db": db} for db in ("sqlite", "mysql", "postgres")],
        indirect=True,
    )
    def test_query_non_select_raises(self, db_client):
        with pytest.raises(QuerySelectOnlyError):
            db_client.query("DELETE FROM test_table", return_type="df")

    @pytest.mark.parametrize(
        "db_client",
        [{"db": db} for db in ("sqlite", "mysql", "postgres")],
        indirect=True,
    )
    def test_query_invalid_return_type_raises(self, db_client):
        with pytest.raises(QueryResultFormatError):
            db_client.query("SELECT * FROM test_table", return_type="unsupported")


### --- Test Query_keyed --- ###


class TestQueryKeyed:

    @pytest.mark.parametrize(
        "db_client",
        [{"db": db} for db in ("sqlite", "mysql", "postgres")],
        indirect=True,
    )
    def test_basic_keyed_query(self, db_client, setup_test_table):
        df = db_client.query_keyed(
            sql=f"SELECT * FROM {setup_test_table}",
            key_column="id",
            key_column_type="int",
            chunk_size=10000,
            return_type="df",
        )
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 100_000

    @pytest.mark.parametrize(
        "db_client",
        [{"db": db} for db in ("sqlite", "mysql", "postgres")],
        indirect=True,
    )
    def test_keyed_query_with_start_end_keys(self, db_client, setup_test_table):
        df = db_client.query_keyed(
            sql=f"SELECT * FROM {setup_test_table}",
            key_column="id",
            key_column_type="int",
            start_key=5000,
            end_key=10000,
            chunk_size=2000,
            return_type="df",
        )
        assert isinstance(df, pd.DataFrame)
        assert df["id"].min() == 5000
        assert df["id"].max() == 10000

    @pytest.mark.parametrize(
        "db_client",
        [{"db": db} for db in ("sqlite", "mysql", "postgres")],
        indirect=True,
    )
    def test_keyed_query_return_list(self, db_client, setup_test_table):
        data = db_client.query_keyed(
            sql=f"SELECT * FROM {setup_test_table}",
            key_column="id",
            key_column_type="int",
            chunk_size=5000,
            return_type="list",
        )
        assert isinstance(data, list)
        assert isinstance(data[0], dict)

    @pytest.mark.parametrize(
        "db_client",
        [{"db": db} for db in ("sqlite", "mysql", "postgres")],
        indirect=True,
    )
    def test_keyed_query_return_raw(self, db_client, setup_test_table):
        rows = db_client.query_keyed(
            sql=f"SELECT * FROM {setup_test_table}",
            key_column="id",
            key_column_type="int",
            chunk_size=5000,
            return_type="raw",
        )
        assert hasattr(rows[0], "_mapping")
        assert "id" in rows[0]._mapping

    @pytest.mark.parametrize(
        "db_client",
        [{"db": db} for db in ("sqlite", "mysql", "postgres")],
        indirect=True,
    )
    def test_keyed_query_return_none(self, db_client, setup_test_table):
        res = db_client.query_keyed(
            sql=f"SELECT * FROM {setup_test_table}",
            key_column="id",
            key_column_type="int",
            return_type="none",
            print_result=True,
        )
        assert res is None

    @pytest.mark.parametrize(
        "db_client",
        [{"db": db} for db in ("sqlite", "mysql", "postgres")],
        indirect=True,
    )
    def test_keyed_query_with_status_and_last_key(self, db_client, setup_test_table):
        df, success, last_key = db_client.query_keyed(
            sql=f"SELECT * FROM {setup_test_table}",
            key_column="id",
            key_column_type="int",
            chunk_size=10000,
            return_last_key=True,
            return_status=True,
        )
        assert isinstance(df, pd.DataFrame)
        assert isinstance(last_key, int)
        assert isinstance(success, bool)
        assert last_key == df["id"].max()

    @pytest.mark.parametrize(
        "db_client",
        [{"db": db} for db in ("sqlite", "mysql", "postgres")],
        indirect=True,
    )
    def test_keyed_query_invalid_return_type(self, db_client, setup_test_table):
        with pytest.raises(QueryResultFormatError):
            db_client.query_keyed(
                sql=f"SELECT * FROM {setup_test_table}",
                key_column="id",
                key_column_type="int",
                return_type="unsupported",
            )

    @pytest.mark.parametrize(
        "db_client",
        [{"db": db} for db in ("sqlite", "mysql", "postgres")],
        indirect=True,
    )
    def test_keyed_query_non_select_raises(self, db_client, setup_test_table):
        with pytest.raises(QuerySelectOnlyError):
            db_client.query_keyed(
                sql=f"DELETE FROM {setup_test_table}",
                key_column="id",
                key_column_type="int",
            )

    @pytest.mark.parametrize(
        "db_client",
        [{"db": db} for db in ("sqlite", "mysql", "postgres")],
        indirect=True,
    )
    def test_keyed_query_disallowed_limit_offset(self, db_client, setup_test_table):
        with pytest.raises(QueryDisallowedClauseError):
            db_client.query_keyed(
                sql=f"SELECT * FROM {setup_test_table} LIMIT 100",
                key_column="id",
                key_column_type="int",
            )
        with pytest.raises(QueryDisallowedClauseError):
            db_client.query_keyed(
                sql=f"SELECT * FROM {setup_test_table} OFFSET 50",
                key_column="id",
                key_column_type="int",
            )


### --- Test Query_batch --- ###


class TestQueryBatch:

    @pytest.mark.parametrize(
        "db_client", [{"db": db} for db in ("mysql", "postgres")], indirect=True
    )
    def test_query_batch_full_table(self, db_client, setup_test_table):
        df = db_client.query_batch(
            f"SELECT * FROM {setup_test_table}", chunk_size=5000, return_type="df"
        )
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 100_000

    @pytest.mark.parametrize(
        "db_client", [{"db": db} for db in ("mysql", "postgres")], indirect=True
    )
    def test_query_batch_with_filters(self, db_client, setup_test_table):
        df = db_client.query_batch(
            f"SELECT * FROM {setup_test_table} WHERE id BETWEEN 1000 AND 3000",
            chunk_size=300,
            return_type="df",
        )
        assert df["id"].min() == 1000
        assert df["id"].max() == 3000

    @pytest.mark.parametrize(
        "db_client", [{"db": db} for db in ("mysql", "postgres")], indirect=True
    )
    def test_query_batch_return_list(self, db_client, setup_test_table):
        out = db_client.query_batch(
            f"SELECT * FROM {setup_test_table} WHERE id < 100",
            chunk_size=25,
            return_type="list",
        )
        assert isinstance(out, list)
        assert isinstance(out[0], dict)

    @pytest.mark.parametrize(
        "db_client", [{"db": db} for db in ("mysql", "postgres")], indirect=True
    )
    def test_query_batch_return_raw(self, db_client, setup_test_table):
        out = db_client.query_batch(
            f"SELECT * FROM {setup_test_table} WHERE id < 100",
            chunk_size=30,
            return_type="raw",
        )
        assert hasattr(out[0], "_mapping")

    @pytest.mark.parametrize(
        "db_client", [{"db": db} for db in ("mysql", "postgres")], indirect=True
    )
    def test_query_batch_return_none(self, db_client, setup_test_table):
        out = db_client.query_batch(
            f"SELECT * FROM {setup_test_table} WHERE id < 100",
            chunk_size=50,
            return_type="none",
            print_result=True,
        )
        assert out is None

    @pytest.mark.parametrize(
        "db_client", [{"db": db} for db in ("mysql", "postgres")], indirect=True
    )
    def test_query_batch_status_flag(self, db_client, setup_test_table):
        df, status = db_client.query_batch(
            f"SELECT * FROM {setup_test_table} WHERE id BETWEEN 200 AND 800",
            chunk_size=100,
            return_type="df",
            return_status=True,
        )
        assert isinstance(df, pd.DataFrame)
        assert status is True

    @pytest.mark.parametrize("db_client", [{"db": "sqlite"}], indirect=True)
    def test_query_batch_raises_on_sqlite(self, db_client, setup_test_table):
        with pytest.raises(UnsupportedMultiThreadedDatabase):
            db_client.query_batch(
                f"SELECT * FROM {setup_test_table} WHERE id < 1000", chunk_size=200
            )

    @pytest.mark.parametrize(
        "db_client", [{"db": db} for db in ("mysql", "postgres")], indirect=True
    )
    def test_query_batch_invalid_sql(self, db_client):
        with pytest.raises(QuerySelectOnlyError):
            db_client.query_batch("SELEC * FROM invalid", chunk_size=100)

    @pytest.mark.parametrize(
        "db_client", [{"db": db} for db in ("mysql", "postgres")], indirect=True
    )
    def test_query_batch_invalid_return_type(self, db_client, setup_test_table):
        with pytest.raises(QueryResultFormatError):
            db_client.query_batch(
                f"SELECT * FROM {setup_test_table}", return_type="foo"
            )

    @pytest.mark.parametrize(
        "db_client", [{"db": db} for db in ("mysql", "postgres")], indirect=True
    )
    def test_query_batch_disallowed_limit_offset(self, db_client, setup_test_table):
        with pytest.raises(QueryDisallowedClauseError):
            db_client.query_batch(
                f"SELECT * FROM {setup_test_table} LIMIT 100", chunk_size=1000
            )
        with pytest.raises(QueryDisallowedClauseError):
            db_client.query_batch(
                f"SELECT * FROM {setup_test_table} OFFSET 50", chunk_size=1000
            )
