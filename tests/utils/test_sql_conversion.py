### --- Standard library imports --- ###
import datetime

import pandas as pd

### --- Third-party imports --- ###
import pytest

from SQLThunder.exceptions import (
    BadArgumentsBulk,
    InvalidSQLOperation,
    QueryDisallowedClauseError,
    QuerySelectOnlyError,
    UnsupportedDatabaseType,
    UnsupportedSQLArgsFormat,
)

### --- Internal package imports --- ###
from SQLThunder.utils.sql_conversion import (
    _build_insert_statement,
    _convert_dbapi_to_sqlalchemy_style,
    _parse_datetime_key_based_pagination,
    _quote_identifier,
    _validate_args_for_bulk,
    _validate_select,
    _validate_select_no_limit_offset,
)

### --- Test Convert DBAPI to SQLAchemy Style --- ###


class TestConvertDBAPIToSQLAlchemyStyle:

    def test_no_args(self):
        sql = "SELECT * FROM table"
        args = None
        converted_sql, converted_args = _convert_dbapi_to_sqlalchemy_style(sql, args)
        assert converted_sql == "SELECT * FROM table"
        assert converted_args == args

    def test_named_placeholders_dict(self):
        sql = "SELECT * FROM table WHERE name = %(name)s"
        args = {"name": "Alice"}
        converted_sql, converted_args = _convert_dbapi_to_sqlalchemy_style(sql, args)
        assert converted_sql == "SELECT * FROM table WHERE name = :name"
        assert converted_args == args

    def test_positional_placeholders_tuple(self):
        sql = "INSERT INTO table VALUES (%s, %s)"
        args = ("Alice", 42)
        converted_sql, converted_args = _convert_dbapi_to_sqlalchemy_style(sql, args)
        assert converted_sql == "INSERT INTO table VALUES (:param1, :param2)"
        assert converted_args == {"param1": "Alice", "param2": 42}

    def test_question_mark_placeholders_list_of_tuples(self):
        sql = "INSERT INTO table VALUES (?, ?)"
        args = [("A", 1), ("B", 2)]
        converted_sql, converted_args = _convert_dbapi_to_sqlalchemy_style(sql, args)
        assert converted_sql == "INSERT INTO table VALUES (:param1, :param2)"
        assert converted_args == [
            {"param1": "A", "param2": 1},
            {"param1": "B", "param2": 2},
        ]

    def test_dataframe_conversion(self):
        df = pd.DataFrame(
            {
                "name": ["A", "B"],
                "ts": [
                    pd.Timestamp("2023-01-01 00:00:00"),
                    pd.Timestamp("2023-01-02 00:00:00"),
                ],
                "tsd": [
                    pd.Timestamp("2023-01-01 01:00:00"),
                    pd.Timestamp("2023-01-02 03:00:00"),
                ],
                "dt": [datetime.date(2023, 1, 1), datetime.date(2023, 1, 2)],
            }
        )
        sql = "INSERT INTO table (name, ts, tsd, dt) VALUES (%s, %s, %s, %s)"
        converted_sql, converted_args = _convert_dbapi_to_sqlalchemy_style(sql, df)
        assert (
            converted_sql
            == "INSERT INTO table (name, ts, tsd, dt) VALUES (:param1, :param2, :param3, :param4)"
        )
        assert isinstance(converted_args, list)
        assert all(isinstance(row, dict) for row in converted_args)

    def test_invalid_args_format(self):
        with pytest.raises(UnsupportedSQLArgsFormat):
            _convert_dbapi_to_sqlalchemy_style(
                "SELECT * FROM table WHERE id = ?", set([1, 2, 3])
            )


### --- Test Validate Arguments for Bulk --- ###


class TestValidateArgsForBulk:

    def test_valid_args(self):
        valid_inputs = [[{"a": 1}], {"a": 1}, (1,), pd.DataFrame([{"a": 1}])]
        for args in valid_inputs:
            _validate_args_for_bulk(args)  # Should not raise

    def test_none_raises(self):
        with pytest.raises(BadArgumentsBulk):
            _validate_args_for_bulk(None)

    def test_empty_inputs_raise(self):
        for empty in [[], {}, (), pd.DataFrame()]:
            with pytest.raises(BadArgumentsBulk):
                _validate_args_for_bulk(empty)

    def test_invalid_type_raises(self):
        with pytest.raises(BadArgumentsBulk):
            _validate_args_for_bulk("string")


### --- Test Quote Identifier --- ###


class TestQuoteIdentifier:

    def test_mysql_quoting(self):
        assert _quote_identifier("col", "mysql") == "`col`"

    def test_postgresql_and_sqlite_quoting(self):
        for dialect in ["postgresql", "sqlite"]:
            assert _quote_identifier("col", dialect) == '"col"'

    def test_unsupported_type_raises(self):
        with pytest.raises(UnsupportedDatabaseType):
            _quote_identifier("col", "oracle")


### --- Test Build Insert Statement --- ###


class TestBuildInsertStatement:

    def test_basic_insert_postgres(self):
        stmt = _build_insert_statement("my_table", ["id", "name"], "postgresql")
        assert stmt == 'INSERT INTO "my_table" ("id", "name") VALUES (:id, :name)'

    def test_schema_qualified_mysql(self):
        stmt = _build_insert_statement("schema.table", ["x", "y"], "mysql")
        assert stmt == "INSERT INTO `schema`.`table` (`x`, `y`) VALUES (:x, :y)"

    def test_invalid_db_type(self):
        with pytest.raises(UnsupportedDatabaseType):
            _build_insert_statement("table", ["x"], "oracle")


### --- Test Parse Dates for Query Key --- ###


class TestParseDatetimeKeyPagination:

    def test_valid_datetime_passthrough(self):
        dt = datetime.datetime(2023, 6, 18)
        assert _parse_datetime_key_based_pagination(dt, "start_key", []) is dt

    def test_string_format_parsing(self):
        res = _parse_datetime_key_based_pagination(
            "2023-06-18", "start_key", ["%Y-%m-%d"]
        )
        assert res == datetime.datetime(2023, 6, 18)

    def test_string_format_failure(self):
        with pytest.raises(InvalidSQLOperation):
            _parse_datetime_key_based_pagination(
                "18/06/2023", "start_key", ["%Y-%m-%d"]
            )

    def test_invalid_type(self):
        with pytest.raises(InvalidSQLOperation):
            _parse_datetime_key_based_pagination(12345, "start_key", ["%Y-%m-%d"])


### --- Test Validate Select --- ###


class TestValidateSelect:

    @pytest.mark.parametrize(
        "valid_sql",
        [
            "SELECT * FROM test_table",
            "select id, name from users",
            "SELECT COUNT(*) FROM logs WHERE active = 1",
        ],
    )
    def test_valid_select_statements(self, valid_sql):
        # Should not raise
        _validate_select(valid_sql)

    @pytest.mark.parametrize(
        "invalid_sql",
        [
            "INSERT INTO test_table (id) VALUES (1)",
            "DELETE FROM test_table",
            "UPDATE test_table SET name = 'abc'",
            "",
            "DROP TABLE users"
            "DELETE FROM test_table WHERE name = (SELECT max(user_id) FROM user)",
        ],
    )
    def test_invalid_non_select_statements(self, invalid_sql):
        with pytest.raises(QuerySelectOnlyError):
            _validate_select(invalid_sql)


### --- Test Validate no Limit or Offset --- ###


class TestValidateSelectNoLimitOffset:

    @pytest.mark.parametrize(
        "valid_sql",
        [
            "SELECT * FROM test_table",
            "select id, name from users where age > 30",
            "SELECT id FROM test WHERE value = 10",
        ],
    )
    def test_valid_queries_without_limit_offset(self, valid_sql):
        _validate_select_no_limit_offset(valid_sql)

    @pytest.mark.parametrize(
        "invalid_sql",
        [
            "SELECT * FROM test_table LIMIT 10",
            "SELECT * FROM test_table OFFSET 20",
            "SELECT name FROM test WHERE active = 1 LIMIT 5 OFFSET 10",
        ],
    )
    def test_queries_with_disallowed_clauses(self, invalid_sql):
        with pytest.raises(QueryDisallowedClauseError):
            _validate_select_no_limit_offset(invalid_sql)

    @pytest.mark.parametrize(
        "non_select_sql",
        [
            "INSERT INTO table (id) VALUES (1)",
            "UPDATE table SET name = 'abc'",
            "DELETE FROM table",
        ],
    )
    def test_non_select_statements_raise(self, non_select_sql):
        with pytest.raises(QuerySelectOnlyError):
            _validate_select_no_limit_offset(non_select_sql)
