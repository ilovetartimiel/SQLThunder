### --- Standard library imports --- ###
import datetime
import re
from datetime import datetime as dt
from typing import Any, Optional, Union, cast

import numpy as np

### --- Third-party imports --- ###
import pandas as pd
import sqlparse
from sqlparse.tokens import DML, Keyword

### --- Internal package imports --- ###
from SQLThunder.exceptions import (
    BadArgumentsBulk,
    InvalidSQLOperation,
    QueryDisallowedClauseError,
    QuerySelectOnlyError,
    UnsupportedDatabaseType,
    UnsupportedSQLArgsFormat,
)

### --- Utils --- ###


def _convert_dbapi_to_sqlalchemy_style(
    sql: str,
    args: Optional[
        Union[
            list[tuple[Any, ...]],
            list[dict[str, Any]],
            tuple[Any, ...],
            dict[str, Any],
            pd.DataFrame,
        ]
    ] = None,
) -> tuple[str, Union[list[dict[str, Any]], dict[str, Any], None]]:
    """
    Converts a DBAPI-style SQL query and arguments to SQLAlchemy-compatible format.

    This function replaces parameter placeholders in the SQL string:
        - %(name)s → :name
        - %s or ?  → :param1, :param2, ...

    It also transforms argument formats (tuple, dict, list, DataFrame) into a structure
    compatible with SQLAlchemy's `text()` execution model.

    Args:
        sql (str): SQL query string containing DBAPI-style placeholders.
        args (Optional[Union[list[tuple[Any, ...]], list[dict[str, Any]], tuple[Any, ...], dict[str, Any], pandas.DataFrame]]):
            Optional bound parameters for the SQL statement. Can be a single dict or tuple, a list of those, or a DataFrame.

    Returns:
        tuple[str, Union[list[dict[str, Any]], dict[str, Any], None]]:
            A tuple containing:
            - The SQL string with converted placeholders.
            - The transformed argument set compatible with SQLAlchemy execution, or None if no arguments provided.

    Raises:
        UnsupportedSQLArgsFormat: If the argument structure is unsupported or malformed.
    """
    try:
        # Replace named placeholders: %(param)s → :param
        sql = re.sub(r"%\((\w+)\)s", r":\1", sql)

        # Replace positional placeholders: %s and ? → :param1, :param2, ...
        class ReplacePositional:
            def __init__(self) -> None:
                self.counter = 0

            def __call__(self, _: re.Match[str]) -> str:
                self.counter += 1
                return f":param{self.counter}"

        replacer = ReplacePositional()
        sql = re.sub(r"(%s|\?)", replacer, sql)

        # If no args provided, return just the SQL
        if args is None:
            return sql, None

        # Handle args conversion (dict or list of dict so no need to modify)
        if isinstance(args, dict) or (
            isinstance(args, list) and all(isinstance(a, dict) for a in args)
        ):
            return sql, cast(Union[list[dict[str, Any]], dict[str, Any]], args)

        # Handle df
        if isinstance(args, pd.DataFrame):
            df = args.copy().where(pd.notnull(args), None)

            for col in df.select_dtypes(include=["datetime64[ns]", "datetimetz"]):
                # Check if the column looks like date-only (all timestamps are 00:00:00)
                if df[col].dropna().dt.time.nunique() == 1 and df[
                    col
                ].dropna().dt.time.iloc[0] == datetime.time(0, 0):
                    df[col] = df[col].dt.date  # convert to datetime.date
                else:
                    df[col] = np.array(
                        df[col].dt.to_pydatetime()
                    )  # keep full timestamp
            return sql, df.to_dict(orient="records")

        # Helper to convert a tuple to a param dict
        def convert_tuple(tup: tuple[Any, ...]) -> dict[str, Any]:
            return {f"param{i + 1}": val for i, val in enumerate(tup)}

        # Single tuple
        if isinstance(args, tuple):
            return sql, convert_tuple(args)

        # List of tuples
        if isinstance(args, list) and all(isinstance(a, tuple) for a in args):
            return sql, [convert_tuple(cast(tuple[Any, ...], tup)) for tup in args]

        # All accepted cases got passed by so raise error because invalid args format
        raise UnsupportedSQLArgsFormat(args)

    except UnsupportedSQLArgsFormat:
        raise


def _validate_args_for_bulk(args: Any) -> None:
    """
    Validate that bulk args are of an accepted type and non-empty.

    Args:
        args (Any): Arguments to validate.

    Raises:
        BadArgumentsBulk: If args are empty or invalid.
    """
    valid_arg_types = (list, dict, tuple, pd.DataFrame)

    if args is None:
        raise BadArgumentsBulk("Bad arguments for bulk insert/execute: args is None")

    if not isinstance(args, valid_arg_types):
        raise BadArgumentsBulk(f"Bad arguments for bulk insert/execute: {type(args)}")

    if isinstance(args, pd.DataFrame):
        if args.empty:
            raise BadArgumentsBulk(
                "Bad arguments for bulk insert/execute: DataFrame is empty"
            )
    else:
        if len(args) == 0:
            raise BadArgumentsBulk(
                "Bad arguments for bulk insert/execute: args is empty"
            )


def _quote_identifier(identifier: str, db_type: str) -> str:
    """
    Apply dialect-specific quoting to SQL identifiers (table or column).

    Args:
        identifier (str): The identifier to quote.
        db_type (str): One of "mysql", "postgresql", "sqlite".

    Returns:
        str: Quoted identifier.

    Raises:
        UnsupportedDatabaseType: If the dialect is not recognized.
    """
    if db_type == "mysql":
        return f"`{identifier}`"
    elif db_type in {"postgresql", "sqlite"}:
        return f'"{identifier}"'
    else:
        raise UnsupportedDatabaseType(db_type)


def _build_insert_statement(table_name: str, columns: list[str], db_type: str) -> str:
    """
    Build a parameterized INSERT INTO statement with quoted identifiers.

    Args:
        table_name (str): Full table name (optionally schema-qualified).
        columns (list[str]): List of column names.
        db_type (str): Database type.

    Returns:
        str: Complete INSERT SQL statement.

    Raises:
        UnsupportedDatabaseType: If db_type is not supported.
    """
    try:
        quoted_cols = ", ".join(_quote_identifier(col, db_type) for col in columns)

        # Split schema.table if needed
        if "." in table_name:
            schema, table = table_name.split(".")
            quoted_table = f"{_quote_identifier(schema, db_type)}.{_quote_identifier(table, db_type)}"
        else:
            quoted_table = _quote_identifier(table_name, db_type)

        placeholders = ", ".join(f":{col}" for col in columns)

        return f"INSERT INTO {quoted_table} ({quoted_cols}) VALUES ({placeholders})"

    except UnsupportedDatabaseType:
        raise


def _parse_datetime_key_based_pagination(
    key_value: Union[dt, str, int],
    label: str,
    accepted_formats: list[str],
) -> dt:
    """
    Parse a datetime pagination key from a string or validate a datetime object.

    This function is used internally to safely handle user-provided `start_key` or `end_key`
    values in key-based pagination. It accepts either:
    - A Python `datetime.datetime` object (returned as-is)
    - A string in one of the accepted datetime formats (parsed using `strptime`)

    Args:
        key_value (Union[datetime.datetime, str, int]): The value to validate or parse. Expected to be a `datetime` or a string.
        label (str): A label used for error messages (e.g., "start_key" or "end_key").
        accepted_formats (list[str]): A list of `strptime`-compatible datetime formats to try.

    Returns:
        datetime.datetime: A parsed or validated datetime object.

    Raises:
        InvalidSQLOperation: If the value is not a `datetime` or a recognized string format.

    Example:
        _parse_datetime_key_based_pagination("2024-06-18", "start_key", ["%Y-%m-%d", "%Y-%m-%d %H:%M:%S"])
        → datetime.datetime(2024, 6, 18, 0, 0)

    Notes:
        - If a string is passed, it is parsed using the first matching format from `accepted_formats`.
        - Commonly accepted formats are "%Y-%m-%d" and "%Y-%m-%d %H:%M:%S".
        - Use this to support flexible date-based input for pagination boundaries.
    """
    if isinstance(key_value, dt):
        return key_value
    if isinstance(key_value, str):
        for fmt in accepted_formats:
            try:
                return dt.strptime(key_value, fmt)
            except ValueError:
                continue
        raise InvalidSQLOperation(
            f"{label} must match 'YYYY-MM-DD HH:MM:SS' or 'YYYY-MM-DD'. Got: {key_value}"
        )
    else:
        raise InvalidSQLOperation(
            f"{label} must be a datetime or string, got: {type(key_value).__name__}"
        )


def _validate_select_no_limit_offset(sql: str) -> None:
    """
    Ensure SQL is a SELECT statement without LIMIT or OFFSET clauses.

    Args:
        sql (str): SQL query to validate.

    Raises:
        QuerySelectOnlyError: If query is not SELECT.
        QueryDisallowedClauseError: If LIMIT or OFFSET is present.
    """
    statements = sqlparse.parse(sql)
    if not statements:
        raise QuerySelectOnlyError()

    stmt = statements[0]

    # Check that the first keyword is SELECT
    found_select = False
    for token in stmt.tokens:
        if token.ttype is DML and token.value.upper() == "SELECT":
            found_select = True
            break
    if not found_select:
        raise QuerySelectOnlyError()

    # Flatten the token tree and look for LIMIT or OFFSET
    for token in stmt.flatten():
        if token.ttype is Keyword and token.value.upper() in ("LIMIT", "OFFSET"):
            raise QueryDisallowedClauseError(token)


def _validate_select(sql: str) -> None:
    """
    Ensure SQL is a valid SELECT statement.

    Args:
        sql (str): SQL query to validate.

    Raises:
        QuerySelectOnlyError: If the statement is not a SELECT.
    """
    statements = sqlparse.parse(sql)
    if not statements:
        raise QuerySelectOnlyError()

    stmt = statements[0]

    # Check that the first keyword is SELECT
    found_select = False
    for token in stmt.tokens:
        if token.ttype is DML and token.value.upper() == "SELECT":
            found_select = True
            break
    if not found_select:
        raise QuerySelectOnlyError()
