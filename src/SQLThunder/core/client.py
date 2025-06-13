### --- Standard library imports --- ###
import threading
from concurrent.futures import ThreadPoolExecutor, wait
from datetime import datetime as dt
from queue import Empty, Queue
from typing import Any, Literal, Optional, Sequence, Union, cast

### --- Third-party imports --- ###
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.engine.row import Row
from sqlalchemy.exc import NoSuchModuleError, OperationalError, SQLAlchemyError
from tqdm import tqdm

### --- Internal package imports --- ###
from SQLThunder.exceptions.base import (
    BaseSQLConversionError,
    ConfigFileError,
    SQLExecutionError,
)
from SQLThunder.exceptions.config import LimitMaxWorkersError, UnsupportedDatabaseType
from SQLThunder.exceptions.dbclient import (
    DBClientClosedError,
    DriverNotFoundError,
    SQLAlchemyEngineError,
)
from SQLThunder.exceptions.execution import (
    BadArgumentsBulk,
    DatabaseConnectionError,
    InvalidSQLOperation,
    QueryDisallowedClauseError,
    QueryExecutionError,
    QueryResultFormatError,
    QuerySelectOnlyError,
    ReopenConnectionError,
    UnsupportedMultiThreadedDatabase,
)
from SQLThunder.logging_config import logger
from SQLThunder.utils.config import _load_config, _resolve_ssl_paths
from SQLThunder.utils.engine import _build_connect_args, _get_db_url
from SQLThunder.utils.insert_helpers import _apply_on_duplicate_clause
from SQLThunder.utils.sql_conversion import (
    _build_insert_statement,
    _convert_dbapi_to_sqlalchemy_style,
    _parse_datetime_key_based_pagination,
    _validate_args_for_bulk,
    _validate_select,
    _validate_select_no_limit_offset,
)

### --- Core class DBClient --- ###


class DBClient:
    """
    DBClient handles all SQL database interactions, including threaded bulk insert/update operations,
    query batching, and efficient pagination. Compatible with MySQL, PostgreSQL, and SQLite.
    """

    # Supported datetime format
    DATETIME_FORMATS = ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d"]

    # Default pool size and max_overflow
    DEFAULT_POOL_SIZE = 10
    DEFAULT_MAX_OVERFLOW = 5

    # INFINITY VALUES
    INT_NEG_INF = -(10**12)
    INT_POS_INF = 10**15

    def __init__(
        self,
        config_file_path: str,
        db_type: Optional[str] = None,
        pool_size: int = DEFAULT_POOL_SIZE,
        max_overflow: int = DEFAULT_MAX_OVERFLOW,
        max_workers: Optional[int] = None,
    ) -> None:
        """
        Initializes a DBClient instance from a config file, creating a SQLAlchemy engine and thread pool.

        Args:
            config_file_path (str): Path to the YAML configuration file.
            db_type (Optional[str]): Optional override for database type ("mysql", "postgresql", or "sqlite").
            pool_size (int): Size of the SQLAlchemy engine connection pool. Defaults to DEFAULT_POOL_SIZE.
            max_overflow (int): Maximum overflow connections allowed above pool_size. Defaults to DEFAULT_MAX_OVERFLOW.
            max_workers (Optional[int]): Maximum number of threads for concurrent execution.
                If None, defaults to pool_size + max_overflow.

        Raises:
            ConfigFileError: If the config file is missing or invalid.
            LimitMaxWorkersError: If max_workers exceeds the pool capacity.
            DatabaseConnectionError: If the DB connection test fails after initialization.
        """
        # Load the config file and get SQLAlchemy db URL
        try:
            # Load config
            self._config = _load_config(config_file_path)
            logger.debug(f"Config loaded from {config_file_path}")
            # Create db url and get driver name
            self._db_url, self._driver, self._db_type = _get_db_url(
                self._config, db_type
            )
            logger.debug(f"DB URL loaded from {self._config}")
        except ConfigFileError as e:
            logger.error(f"Failed to load config and get db_url: {e}")
            raise

        # Store pool settings
        self._pool_size = pool_size
        self._max_overflow = max_overflow

        # SSL file path check
        try:
            self._ssl_paths = _resolve_ssl_paths(self._config)
            logger.info(f"SSL paths resolved: {self._ssl_paths}")
        except ConfigFileError as e:
            logger.error(f"Failed to load config and resolve SSL paths: {e}")
            raise

        # Create connect args for SSL config to pass in create engine
        self._connect_args = _build_connect_args(
            self._driver, self._ssl_paths, self._config
        )

        # Create a close flag so when close() is called we make instance unusable for error prevention
        self._closed = False

        # Create engine
        self._engine = self._create_engine_alchemy()

        # Max workers logic
        self._total_pool_capacity = self._pool_size + self._max_overflow
        if max_workers is not None and max_workers > self._total_pool_capacity:
            raise LimitMaxWorkersError(max_workers, self._total_pool_capacity)
        self._max_workers = max_workers or self._total_pool_capacity

        # Set up ThreadPoolExecutor
        self._executor = ThreadPoolExecutor(max_workers=self._max_workers)

        # Test connection
        try:
            self._test_connection()
        except DatabaseConnectionError:
            self.close()  # clean up engine + threadpool
            raise

    ### --- Initialization --- ###

    def _create_engine_alchemy(self) -> Engine:
        """
        Creates and returns a SQLAlchemy Engine instance using internal config.

        Returns:
            Engine: SQLAlchemy Engine configured with SSL and pooling options.

        Raises:
            DriverNotFoundError: If the DB driver module cannot be loaded.
            SQLAlchemyEngineError: For any SQLAlchemy-related engine creation failure.
        """
        # Create engine
        try:
            return create_engine(
                self._db_url,
                connect_args=self._connect_args,
                pool_size=self._pool_size,
                max_overflow=self._max_overflow,
                echo=False,
                future=True,
                pool_pre_ping=True,
            )
        except NoSuchModuleError as e:
            logger.error(
                f"Engine creation failed: missing or invalid driver module - {e}"
            )
            raise DriverNotFoundError(self._driver) from e
        except SQLAlchemyError as e:
            logger.error(f"Engine creation failed: SQLAlchemy internal error - {e}")
            raise SQLAlchemyEngineError(e) from e
        except Exception as e:
            logger.error(f"Engine creation failed: unknown error - {e}")
            raise SQLAlchemyEngineError(e) from e

    def _test_connection(self) -> None:
        """
        Tests the database connection by executing a lightweight query.

        Raises:
            DatabaseConnectionError: If connection test fails due to SSL, auth, or network issues.
        """
        try:
            with self._engine.connect() as conn:
                conn.execute(text("SELECT 1"))
                logger.info("Connection test succeeded.")
        except OperationalError as e:
            logger.error(f"OperationalError during connection test: {e}")
            msg = (
                "SSL is required by the server but missing in configuration."
                if "required_secure_transport" in str(e).lower()
                else "Failed to establish database connection"
            )
            raise DatabaseConnectionError(msg, e)

        except SQLAlchemyError as e:
            logger.error(f"SQLAlchemyError during connection test: {e}")
            raise DatabaseConnectionError("SQLAlchemy error during test connection", e)

    def _check_closed(self) -> None:
        """
        Internal check to prevent operations on a closed DBClient.

        Raises:
            DBClientClosedError: If the instance has already been closed.
        """
        if self._closed:
            raise DBClientClosedError()

    ### --- Public connection controls and resources management --- ###

    def test_connection(self) -> bool:
        """
        Checks if the database connection is currently valid.

        Returns:
            bool: True if connection test succeeds, False otherwise.
        """
        try:
            # Check if engine was closed
            self._check_closed()
            # Check connection
            self._test_connection()
            logger.info("Connection test succeeded.")
            return True
        except DBClientClosedError as e:
            logger.warning(f"Connection test failed: {e}")
            return False
        except DatabaseConnectionError as e:
            logger.warning(f"Connection test failed: {e}")
            return False

    def reopen_connection(self) -> None:
        """
        Reopen a previously closed DBClient by recreating the engine and thread pool.

        Useful for long-running applications where connections might be dropped or closed.

        Raises:
            ReopenConnectionError: If reconnection fails after engine creation.
        """
        if not self._closed:
            logger.warning("DBClient connection is already open. Skipping reopen.")
            return

        logger.info("Reopening DBClient...")

        try:
            self._engine = self._create_engine_alchemy()
            self._executor = ThreadPoolExecutor(max_workers=self._max_workers)
            self._closed = False
            self._test_connection()
            logger.info("DBClient successfully reopened.")
        except Exception as e:
            logger.error(f"Failed to reopen DBClient: {e}")
            self.close()  # Ensure partial resources are cleaned
            raise ReopenConnectionError("Failed to reopen DBClient", e)

    def close(self) -> None:
        """
        Cleanly dispose of the SQLAlchemy engine and shutdown the thread pool.

        After calling `close()`, the instance becomes unusable unless `reopen_connection()` is called.
        """
        if not self._closed:
            self._engine.dispose()
            self._executor.shutdown(wait=True)
            self._closed = True
            logger.info("DBClient shut down")

    @property
    def is_closed(self) -> bool:
        """
        Indicates whether the DBClient has been closed.

        Returns:
            bool: True if `close()` was called and the instance is now inactive.
        """
        return self._closed

    ### --- Read operations --- ###

    ### --- Query (Single transaction) --- ###

    def query(
        self,
        sql: str,
        args: Optional[
            Union[
                list[tuple[Any, ...]],
                list[dict[str, Any]],
                tuple[Any, ...],
                dict[str, Any],
            ]
        ] = None,
        return_type: Literal["df", "raw", "list", "none"] = "df",
        print_result: bool = False,
        print_limit: int = 5,
    ) -> Union[pd.DataFrame, list[dict[str, Any]], Sequence[Row], None]:
        """
        Executes a single SQL SELECT query with optional bound parameters.

        Args:
            sql (str): A SQL SELECT statement. May include named placeholders (e.g., :id).
            args (Optional[Union[list[tuple[Any, ...]], list[dict[str, Any]], tuple[Any, ...], dict[str, Any]]]):
                Parameters to bind to the query. Must be a single dict or tuple, or a list containing exactly one such element.
            return_type (Literal["df", "raw", "list", "none"]): Format of the returned result. One of:
                - "df": Return a pandas.DataFrame (default)
                - "list": Return a list of dictionaries
                - "raw": Return a list of SQLAlchemy Row objects
                - "none": Return None
            print_result (bool): Whether to print the query result to stdout.
            print_limit (int): Number of rows to display when printing. Only used if print_result is True.

        Returns:
            Union[pandas.DataFrame, list[dict[str, Any]], Sequence[Row], None]:
                The query result in the specified format.

        Raises:
            QuerySelectOnlyError: If the SQL statement is not a SELECT query.
            InvalidSQLOperation: If the SQL is malformed or multiple argument sets are passed.
            QueryExecutionError: If query execution fails.
            QueryResultFormatError: If return_type is unsupported.
            DBClientClosedError: If the instance has already been closed.
        """
        self._check_closed()

        # Check if it is a select statement
        try:
            _validate_select(sql=sql)
        except QuerySelectOnlyError:
            raise

        # Check accepted return types
        return_format: str = return_type
        if return_format.lower() not in {"df", "list", "raw", "none"}:
            raise QueryResultFormatError(return_type)

        # Convert args
        try:
            if args is not None:
                sql, converted_args = _convert_dbapi_to_sqlalchemy_style(sql, args)
                if isinstance(converted_args, list):
                    if len(converted_args) != 1:
                        raise InvalidSQLOperation(
                            "query() only accepts one row of parameters."
                        )
                    args = converted_args[0]
                else:
                    args = converted_args
        except Exception as e:
            logger.warning(f"Invalid SQL or args: {e}")
            raise InvalidSQLOperation(f"Failed to prepare SQL/args: {e}")

        # Execute
        try:
            with self._engine.connect() as conn:
                result = conn.execute(text(sql), args or {})
                rows = result.fetchall()
                columns = list(
                    result.keys()
                )  # For static type checking consistency (would work at runtime w/o list)
                logger.info(f"Successfully executed query: {sql}")
        except SQLAlchemyError as e:
            logger.warning(f"Query failed: {e}")
            raise QueryExecutionError(e)

        # Print preview if requested
        if print_result:
            preview_df = pd.DataFrame(rows[:print_limit], columns=columns or None)
            print(preview_df.to_string(index=False))

        # Return according to requested format
        return_format = return_format.lower()
        if return_format == "df":
            return pd.DataFrame(rows, columns=columns or None)
        elif return_format == "list":
            return [dict(zip(columns, row)) for row in rows] if columns else []
        elif return_format == "raw":
            return rows  # List[Row]
        elif return_format == "none":
            return None
        else:
            raise QueryResultFormatError(return_type)

    ### --- Query keyed (Key-based pagination, multiple transactions) --- ###

    def query_keyed(
        self,
        sql: str,
        key_column: str,
        key_column_type: Literal["int", "string", "date"],
        start_key: Optional[Union[int, dt, str]] = None,
        end_key: Optional[Union[int, dt, str]] = None,
        order: Literal["asc", "desc"] = "asc",
        args: Optional[
            Union[
                list[tuple[Any, ...]],
                list[dict[str, Any]],
                tuple[Any, ...],
                dict[str, Any],
            ]
        ] = None,
        chunk_size: int = 10_000,
        return_type: Literal["df", "raw", "list", "none"] = "df",
        print_result: bool = False,
        print_limit: int = 10,
        return_last_key: bool = False,
        return_status: bool = False,
    ) -> Union[
        pd.DataFrame,
        list[dict[str, Any]],
        Sequence[Row],
        None,
        tuple[Union[pd.DataFrame, list[dict[str, Any]], Sequence[Row], None], Any],
        tuple[Union[pd.DataFrame, list[dict[str, Any]], Sequence[Row], None], bool],
        tuple[
            Union[pd.DataFrame, list[dict[str, Any]], Sequence[Row], None], bool, Any
        ],
    ]:
        """
        Executes a large SQL SELECT query using key-based pagination on a sortable column.

        This method chunks a large query by using a monotonically increasing key column,
        avoiding OFFSET-based pagination for better performance on large tables. It supports
        resuming, printing, and returning additional metadata.

        Args:
            sql (str): Base SQL SELECT query (without LIMIT or pagination conditions).
            key_column (str): The column to use as the pagination key.
            key_column_type (Literal["int", "string", "date"]): Type of the key column for proper formatting and validation.
            start_key (Optional[Union[int, datetime.datetime, str]]): Exclusive lower bound key to start from.
            end_key (Optional[Union[int, datetime.datetime, str]]): Inclusive upper bound key to stop at.
            order (Literal["asc", "desc"]): Sort direction for pagination. Defaults to "asc".
            args (Optional[Union[list[tuple[Any, ...]], list[dict[str, Any]], tuple[Any, ...], dict[str, Any]]]):
                Parameters to bind to the SQL query. Must be a single dict/tuple or a list containing one such element.
            chunk_size (int): Number of rows to fetch per chunk. Defaults to 10,000.
            return_type (Literal["df", "raw", "list", "none"]): Format of the returned result. One of:
                - "df": Return a pandas.DataFrame (default)
                - "list": Return a list of dictionaries
                - "raw": Return a list of SQLAlchemy Row objects
                - "none": Return None
            print_result (bool): Whether to print a preview of the query result.
            print_limit (int): Number of rows to preview if printing is enabled.
            return_last_key (bool): Whether to return the last key seen in the result set.
            return_status (bool): Whether to return a boolean indicating query success.

        Returns:
            Union[
                pandas.DataFrame,
                list[dict[str, Any]],
                Sequence[Row],
                None,
                tuple[result, last_key],
                tuple[result, success],
                tuple[result, success, last_key]
            ]:
                The query result in the specified format. Optionally includes the last key and/or a success flag.

        Raises:
            QuerySelectOnlyError: If the SQL is not a SELECT statement.
            QueryDisallowedClauseError: If LIMIT or OFFSET is present in the SQL.
            QueryResultFormatError: If return_type is not one of the supported formats.
            InvalidSQLOperation: If the key column type or bound arguments are invalid,
                or if required key values are missing or incorrectly typed.
            DBClientClosedError: If the instance has already been closed.
        """
        self._check_closed()

        # Validate select query
        try:
            _validate_select_no_limit_offset(sql=sql)
        except QuerySelectOnlyError:
            raise
        except QueryDisallowedClauseError:
            logger.error(
                "Use start_key and end_key if you want to achieve a similar result to offset and limit"
            )
            raise

        # Check accepted return types
        return_format: str = return_type
        if return_format.lower() not in {"df", "list", "raw", "none"}:
            raise QueryResultFormatError(return_type)

        # Check key_column_type
        accepted_key_column_type = {"int", "string", "date"}
        if key_column_type not in accepted_key_column_type:
            raise InvalidSQLOperation(
                f"query_keyed() requires a key_column_type to be one of {accepted_key_column_type}."
            )

        # Set order to default to asc if None
        order = order or "asc"

        try:
            if args is not None:
                sql, converted_args = _convert_dbapi_to_sqlalchemy_style(sql, args)
                if isinstance(converted_args, list):
                    if len(converted_args) != 1:
                        raise InvalidSQLOperation(
                            "query_keyed() only accepts one row of parameters."
                        )
                    args = converted_args[0]
                else:
                    args = converted_args
        except Exception as e:
            logger.warning(f"Invalid SQL or args: {e}")
            raise InvalidSQLOperation(f"Failed to prepare SQL/args: {e}")

        # Create new args dic for current_key and last key, start key depending
        bind_args = args.copy() if args else {}

        # For mypy
        current_key: Union[int, str, dt]
        max_key: Optional[Union[int, str, dt]]

        ### --- Initialize current_key and max key --- ###

        # Convert to datetime format if using a date as key (key_column_type="date")
        if key_column_type == "date":
            if (start_key is None) or (not isinstance(start_key, (dt, str))):
                raise InvalidSQLOperation(
                    "query_keyed() requires a start_key when key_column_type='date'. "
                    f"It must either be a string in one of the {self.DATETIME_FORMATS} formats or a datetime object. "
                )
            else:
                try:
                    current_key = _parse_datetime_key_based_pagination(
                        start_key, "start_key", accepted_formats=self.DATETIME_FORMATS
                    )
                except InvalidSQLOperation:
                    raise
                if end_key is not None:
                    try:
                        max_key = _parse_datetime_key_based_pagination(
                            end_key, "end_key", accepted_formats=self.DATETIME_FORMATS
                        )
                    except InvalidSQLOperation:
                        raise
                else:
                    max_key = None

        # Initialize as string if key_column_type="string"
        elif key_column_type == "string":
            if (start_key is None) or (not isinstance(start_key, str)):
                raise InvalidSQLOperation(
                    "query_keyed() requires a start_key when key_column_type='string'. "
                    "It must be a string. "
                    "You can use '' as a start_key with order 'asc' to get all rows from the beginning. "
                    "Using '' as a surrogate is unsafe unless you guarantee no empty-string keys. "
                )
            else:
                current_key = start_key
                if end_key is not None:
                    if isinstance(end_key, str):
                        max_key = end_key
                    else:
                        raise InvalidSQLOperation(
                            "query_keyed() requires end_key to be a string when key_column_type='string'"
                        )
                else:
                    max_key = None

        else:  # key_column_type == "int"
            # Start key
            if start_key is None:
                current_key = self.INT_NEG_INF if order == "asc" else self.INT_POS_INF
            elif isinstance(start_key, int):
                current_key = start_key
            else:
                raise InvalidSQLOperation(
                    "query_keyed() requires start_key to be an int when key_column_type='int'"
                )
            # Max key
            if end_key is not None:
                if isinstance(end_key, int):
                    max_key = end_key
                else:
                    raise InvalidSQLOperation(
                        "query_keyed() requires end_key to be an int when key_column_type='int'"
                    )
            else:
                max_key = None

        # Assign max_key
        if max_key is not None:
            bind_args["end_key"] = max_key

        ### --- Main logic --- ###
        # Initiate empty all_rows and column names to store results
        all_rows = []
        column_names: list[str] = []

        # Initialize state (if start_key provided)
        last_key = None  # Last seen key
        success = True
        first_pass = True  # To add first row with >= operator in query

        # Start query logic
        while True:
            # create where clause for key base pagination (key_column + end_key)
            where_clauses = []

            if order == "asc":
                operator = ">=" if first_pass else ">"
                where_clauses.append(f"{key_column} {operator} :last_key")
                if end_key is not None:
                    where_clauses.append(f"{key_column} <= :end_key")
            else:
                operator = "<=" if first_pass else "<"
                where_clauses.append(f"{key_column} {operator} :last_key")
                if end_key is not None:
                    where_clauses.append(f"{key_column} >= :end_key")

            where_sql = " AND ".join(where_clauses)
            paginated_sql = f"""
                {sql.strip().rstrip(';')}
                {"AND" if "where" in sql.lower() else "WHERE"} {where_sql}
                ORDER BY {key_column} {order.upper()}
                LIMIT {chunk_size}
            """

            # Update last key value
            bind_args["last_key"] = current_key

            try:
                with self._engine.connect() as conn:
                    result = conn.execute(text(paginated_sql), bind_args)
                    rows = result.fetchall()
                    first_pass = False  # Not first pass anymore
                    if not column_names and result.keys():
                        column_names = list(result.keys())
                        column_index_map = {
                            col: idx for idx, col in enumerate(column_names)
                        }  # Used to get last_key
                        key_index = column_index_map[key_column]
            except Exception as e:
                logger.warning(f"Key-based chunk failed: {e}")
                success = False
                break

            # Break if no more rows
            if not rows:
                break

            all_rows.extend(rows)

            # Break if length of rows is smaller than chunk_size
            if len(rows) < chunk_size:
                try:
                    last_key = rows[-1][key_index]
                except Exception as e:
                    last_key = None
                    logger.warning(f"Could not extract key from last row: : {e}")
                break

            # Update current key and last_key
            try:
                current_key = rows[-1][key_index]
                last_key = current_key
            except Exception as e:
                last_key = None
                success = False
                logger.warning(f"Could not extract last key: {e}")
                break

        # Print results
        if print_result and all_rows:
            preview_df = pd.DataFrame(
                all_rows[:print_limit], columns=column_names or None
            )
            print(preview_df.to_string(index=False))

        # Return
        return_format = return_format.lower()
        if return_format == "df":
            result = pd.DataFrame(all_rows, columns=column_names or None)
        elif return_format == "none":
            result = None
        elif return_format == "raw":
            result = all_rows
        elif return_format == "list":
            result = (
                [dict(zip(column_names, row)) for row in all_rows]
                if column_names
                else []
            )
        else:
            raise QueryResultFormatError(return_type)

        if return_last_key and return_status:
            return result, success, last_key
        elif return_last_key:
            return result, last_key
        elif return_status:
            return result, success
        else:
            return result

    ### --- Query batch (Threaded, multiple transactions) --- ###

    def query_batch(
        self,
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
        chunk_size: int = 10_000,
        max_workers: int = 15,
        return_type: Literal["df", "raw", "list", "none"] = "df",
        return_status: bool = False,
        print_result: bool = False,
        print_limit: int = 10,
    ) -> Union[
        pd.DataFrame,
        list[dict[str, Any]],
        Sequence[Row],
        None,
        tuple[Union[pd.DataFrame, list[dict[str, Any]], Sequence[Row], None], bool],
    ]:
        """
        Executes a large SQL SELECT query in parallel chunks using LIMIT and OFFSET.

        This method divides a SELECT query into chunks and fetches them concurrently using a thread pool.
        It stops automatically when a chunk returns no rows. Useful for large data extractions where
        key-based pagination is not possible.

        Args:
            sql (str): Base SQL SELECT query (without LIMIT or OFFSET clauses).
            args (Optional[Union[list[tuple[Any, ...]], list[dict[str, Any]], tuple[Any, ...], dict[str, Any], pandas.DataFrame]]):
                Parameters to bind to the query. Must represent a single row of parameters.
            chunk_size (int): Number of rows to fetch per chunk. Defaults to 10,000.
            max_workers (int): Number of threads to run in parallel. Defaults to 15.
            return_type (Literal["df", "raw", "list", "none"]): Format of the returned result. One of:
                - "df": Return a pandas.DataFrame (default)
                - "list": Return a list of dictionaries
                - "raw": Return a list of SQLAlchemy Row objects
                - "none": Return None
            return_status (bool): If True, returns a tuple with the result and a success flag.
            print_result (bool): Whether to print a preview of the result to stdout.
            print_limit (int): Number of rows to print if print_result is True.

        Returns:
            Union[
                pandas.DataFrame,
                list[dict[str, Any]],
                Sequence[Row],
                None,
                tuple[Union[pandas.DataFrame, list[dict[str, Any]], Sequence[Row], None], bool]
            ]:
                The query result in the specified format. If return_status is True, a tuple is returned with a success flag.

        Raises:
            QuerySelectOnlyError: If the SQL statement is not a SELECT query.
            QueryDisallowedClauseError: If LIMIT or OFFSET is present in the SQL.
            QueryResultFormatError: If return_type is not one of the supported values.
            InvalidSQLOperation: If the SQL or arguments are malformed or incompatible.
            LimitMaxWorkersError: If max_workers exceeds the connection pool capacity.
            UnsupportedMultiThreadedDatabase: If multithreaded reads are attempted on a SQLite database.
            DBClientClosedError: If the instance has already been closed.
        """
        # Check if engine was closed
        self._check_closed()

        # If SQLite raises
        if self._db_type == "sqlite":
            logger.error(
                "Threaded reads are not supported on SQLite. Use query or query_keyed instead. "
            )
            raise UnsupportedMultiThreadedDatabase(self._db_type)

        # Validate select query
        try:
            _validate_select_no_limit_offset(sql=sql)
        except QuerySelectOnlyError:
            raise
        except QueryDisallowedClauseError:
            logger.error(
                "query_batch() does not support the use of limit or offset clauses. "
            )
            raise

        # Check if valid return type
        return_format: str = return_type
        if return_format.lower() not in {"df", "list", "raw", "none"}:
            raise QueryResultFormatError(return_type)

        # Check if max_worker given by user above total_pool_size
        if max_workers > self._total_pool_capacity:
            raise LimitMaxWorkersError(max_workers, self._total_pool_capacity)

        # Convert args
        try:
            if args is not None:
                sql, converted_args = _convert_dbapi_to_sqlalchemy_style(sql, args)
                if isinstance(converted_args, list):
                    if len(converted_args) != 1:
                        raise InvalidSQLOperation(
                            "query_batch() only accepts one row of parameters."
                        )
                    args = converted_args[0]
                else:
                    args = converted_args
        except Exception as e:
            logger.warning(f"Invalid SQL or args: {e}")
            raise InvalidSQLOperation(f"Failed to prepare SQL/args: {e}")

        if self._db_type == "sqlite" and self._max_workers > 1:
            logger.warning(
                "SQLite supports concurrent reads, but using many threads on file-based DBs may be suboptimal."
            )

        # Initialize work queue and results
        work_queue: Queue[int] = Queue()
        results = []
        success = {"status": True}
        results_lock = threading.Lock()
        success_lock = threading.Lock()

        # Seed the first `max_workers` chunk indices
        for i in range(max_workers):
            work_queue.put(i)

        # Thread function (dynamic queuing since unknown number of chunks)
        def fetch_worker() -> None:
            while True:
                try:
                    chunk_index = work_queue.get(timeout=1)
                except Empty:
                    break

                offset = chunk_index * chunk_size
                paginated_sql = (
                    f"{sql.strip().rstrip(';')} LIMIT {chunk_size} OFFSET {offset}"
                )

                # noinspection PyShadowingNames
                try:
                    with self._engine.connect() as conn:
                        result = conn.execute(text(paginated_sql), args or {})
                        rows = result.fetchall()
                        if rows:
                            with results_lock:
                                results.append((chunk_index, rows, result.keys()))
                            # Queue the next sequential chunk
                            work_queue.put(chunk_index + max_workers)
                        # else: stop naturally — don't queue anything
                except Exception as e:
                    logger.warning(f"Chunk {chunk_index} failed: {e}")
                    with success_lock:
                        success["status"] = False
                finally:
                    work_queue.task_done()

        # Create new executor if max_workers != self._max_workers
        if max_workers != self._max_workers:
            query_executor = ThreadPoolExecutor(max_workers=max_workers)
            temp_executor = True
        else:
            query_executor = self._executor
            temp_executor = False

        # Launch query with progress bar
        futures = [query_executor.submit(fetch_worker) for _ in range(max_workers)]

        # Progress bar (can't have the end final for progress bar so update as it runs but start at max_workers)
        with tqdm(total=max_workers, desc="Querying chunks") as pbar:
            previous_max = 0
            while any(f.done() is False for f in futures):
                current_size = len(results)
                if current_size > previous_max:
                    if current_size > pbar.total:
                        pbar.total = current_size
                        pbar.refresh()
                    pbar.update(current_size - previous_max)
                    previous_max = current_size

        # Stop main thread (using futures -> all futures are done only if all tasks are done in fetch_worker logic)
        wait(futures)

        # Shutdown temp executor if created
        if temp_executor:
            query_executor.shutdown(wait=False)

        # Sort and flatten results
        results.sort(key=lambda x: x[0])
        # Extract first valid column names
        column_names = []
        for _, _, cols in results:
            if cols:
                column_names = list(cols)
                break
        # Flatten rows from all chunks
        all_rows = [row for _, rows, _ in results for row in rows]

        # If no rows
        if not all_rows:
            logger.info("Query executed successfully but returned no rows.")
        # If no column_names
        if not column_names:
            logger.info("Query returned no column names: check your sql statement.")

        # Optional print
        if print_result:
            preview_df = pd.DataFrame(
                all_rows[:print_limit], columns=column_names or None
            )
            print(preview_df.to_string(index=False))

        # Return
        return_format = return_format.lower()
        if return_format == "df":
            res = pd.DataFrame(all_rows, columns=column_names or None)
        elif return_format == "none":
            res = None
        elif return_format == "raw":
            res = all_rows
        elif return_format == "list":
            res = (
                [dict(zip(column_names, row)) for row in all_rows]
                if column_names
                else []
            )
        else:
            raise QueryResultFormatError(return_type)

        if return_status:
            return res, success["status"]
        else:
            return res

    ### --- Write operations --- ###

    ### --- Single transaction --- ###

    ### --- Execute (single transaction, single args) --- ###

    def execute(
        self,
        sql: str,
        args: Optional[
            Union[
                list[tuple[Any, ...]],
                list[dict[str, Any]],
                tuple[Any, ...],
                dict[str, Any],
            ]
        ] = None,
        on_duplicate: Optional[str] = None,
        return_failures: bool = True,
        return_status: bool = False,
    ) -> Union[
        tuple[pd.DataFrame, bool],
        tuple[pd.DataFrame, None],
        tuple[None, bool],
        tuple[None, None],
    ]:
        """
        Executes a single non-SELECT SQL statement (INSERT, UPDATE, DELETE, CREATE, ...) within a transaction.

        This method is intended for single-row or non-batch operations. For multi-row or large-scale writes,
        consider using `execute_many()` or `execute_batch()` instead.

        Args:
            sql (str): SQL statement with optional named placeholders (e.g., :id).
            args (Optional[Union[list[tuple[Any, ...]], list[dict[str, Any]], tuple[Any, ...], dict[str, Any]]]):
                A single row of bound parameters.
            on_duplicate (Optional[str]): Optional duplicate-handling clause to apply (e.g., "ignore", "replace").
            return_failures (bool): If True, returns a DataFrame with error details on failure. Defaults to True.
            return_status (bool): If True, includes a boolean success flag in the return. Defaults to False.

        Returns:
            Union[
                tuple[pandas.DataFrame, bool],
                tuple[pandas.DataFrame, None],
                tuple[None, bool],
                tuple[None, None]
            ]: A tuple containing:
                - A DataFrame of the failed record (if any, else empty DataFrame, and `return_failures` is True) or None.
                - A success flag (if `return_status` is True), otherwise None.

        Raises:
            InvalidSQLOperation: If the SQL or bound arguments are invalid or malformed.
            SQLExecutionError: If duplicate-handling logic could not be applied.
            DBClientClosedError: If the instance has already been closed.
        """
        # Check if close hasn't been called yet
        self._check_closed()

        # Convert params to sqlalchemy compatible placeholders and check args
        try:
            if args is not None:
                sql, converted_args = _convert_dbapi_to_sqlalchemy_style(sql, args)
                # Ensure args is always a single dict
                if isinstance(converted_args, list):
                    if len(converted_args) != 1:
                        raise InvalidSQLOperation(
                            "execute() only accepts a single row of parameters."
                        )
                    args = converted_args[0]
                else:
                    args = converted_args
        except Exception as e:
            raise InvalidSQLOperation(f"Failed to convert SQL/args: {e}")

        # Ignore duplicates logic
        try:
            sql = _apply_on_duplicate_clause(sql, self._db_type, on_duplicate)
        except SQLExecutionError as e:
            logger.error(f"Duplicate handling logic error {e}")
            raise

        # Execute transaction
        try:
            with self._engine.begin() as conn:
                conn.execute(text(sql), args or {})
            logger.info("Single SQL statement executed successfully.")
            if return_failures and return_status:
                return pd.DataFrame(), True
            elif return_failures:
                return pd.DataFrame(), None
            elif return_status:
                return None, False
            else:
                return None, None
        except Exception as e:
            logger.warning(f"Execution failed: {e}")
            logger.debug(f"SQL: {sql}")
            logger.debug(f"Args: {args}")
            if return_failures:
                df_failures = pd.DataFrame(
                    [
                        {
                            **(args or {}),
                            "error_message": str(e),
                            "sql": sql[:300],  # preview of query
                        }
                    ]
                )
                if return_status:
                    return df_failures, False
                else:
                    return df_failures, None
            else:
                if return_status:
                    return None, False
                else:
                    return None, None

    ### --- Execute many (Single transaction, Multiple args) --- ###

    def execute_many(
        self,
        sql: str,
        args: Union[
            list[tuple[Any, ...]],
            list[dict[str, Any]],
            tuple[Any, ...],
            dict[str, Any],
            pd.DataFrame,
        ],
        on_duplicate: Optional[str] = None,
        return_failures: bool = True,
        return_status: bool = False,
    ) -> Union[
        tuple[pd.DataFrame, bool],
        tuple[pd.DataFrame, None],
        tuple[None, bool],
        tuple[None, None],
    ]:
        """
        Executes a bulk non-SELECT SQL operation (INSERT, UPDATE, or DELETE) in a single transaction.

        This is an all-or-nothing operation: if any row in the batch fails, the entire transaction is rolled back.
        Useful for applying a uniform statement across many rows (e.g., bulk inserts with conflict handling).

        Args:
            sql (str): SQL statement with placeholders (e.g., :id, :value).
            args (Union[list[tuple[Any, ...]], list[dict[str, Any]], tuple[Any, ...], dict[str, Any], pandas.DataFrame]):
                Bound parameters to apply to the SQL query. Can be a list of tuples/dicts or a DataFrame.
            on_duplicate (Optional[str]): Optional duplicate-handling mode ("ignore", "replace", or None).
            return_failures (bool): If True, returns a DataFrame of failed rows with error messages on failure.
            return_status (bool): If True, includes a success flag in the return value.

        Returns:
            Union[
                tuple[pandas.DataFrame, bool],
                tuple[pandas.DataFrame, None],
                tuple[None, bool],
                tuple[None, None]
            ]: A tuple containing:
                - A DataFrame of failed records (if any, else empty DataFrame, and `return_failures` is True) or None.
                - A success flag (if `return_status` is True), otherwise None.

        Raises:
            InvalidSQLOperation: If the SQL or input arguments are malformed or cannot be converted.
            SQLExecutionError: If duplicate-handling clause insertion fails.
            BadArgumentsBulk: If no valid rows are provided for execution.
            BaseSQLConversionError: If argument conversion fails.
            DBClientClosedError: If the instance has already been closed.
        """

        # Check if close hasn't been called yet
        self._check_closed()

        # Check if args were provided (necessary for chunks otherwise use execute single)
        try:
            _validate_args_for_bulk(args)
            logger.debug("Valid args for execute_many")
        except BadArgumentsBulk as e:
            logger.error(f"Invalid arguments for execute_many {e}")
            raise

        # Convert params to sqlalchemy compatible placeholders
        try:
            sql, args = _convert_dbapi_to_sqlalchemy_style(sql, args)
            if isinstance(args, dict):  # In case only 1 row was used with execute_many
                args = [args]
            # Narrow the type explicitly for MyPy
            args = cast(list[dict[str, Any]], args)
        except BaseSQLConversionError as e:
            logger.error(f"Invalid args format: {e}")
            raise

        # Ignore duplicates logic
        try:
            sql = _apply_on_duplicate_clause(sql, self._db_type, on_duplicate)
        except SQLExecutionError as e:
            logger.error(f"Duplicate handling logic error {e}")
            raise

        # Execute transaction
        try:
            with self._engine.begin() as conn:
                conn.execute(text(sql), args)
            logger.info("All records executed successfully in a single transaction.")
            if return_failures and return_status:
                return pd.DataFrame(), True
            elif return_failures:
                return pd.DataFrame(), None
            elif return_status:
                return None, False
            else:
                return None, None
        except Exception as e:
            logger.warning(f"Transaction failed: {e}")
            logger.debug(f"SQL: {sql}")
            logger.debug(f"Args: {args}")

            # Return success flag and failures df logic
            if return_failures and return_status:
                return (
                    pd.DataFrame([{**row, "error_message": str(e)} for row in args]),
                    False,
                )
            elif return_failures:
                return (
                    pd.DataFrame([{**row, "error_message": str(e)} for row in args]),
                    None,
                )
            elif return_status:
                return None, False
            else:
                return None, None

    ### --- Insert many (Single transaction, Multiple args, just for inserts) --- ###

    def insert_many(
        self,
        df: pd.DataFrame,
        table_name: str,
        on_duplicate: Optional[str] = None,
        return_failures: bool = True,
        return_status: bool = False,
    ) -> Union[
        tuple[pd.DataFrame, bool],
        tuple[pd.DataFrame, None],
        tuple[None, bool],
        tuple[None, None],
    ]:
        """
        Inserts a pandas DataFrame into a SQL table using a single atomic transaction.

        This method builds a parameterized INSERT statement and executes it with the data from the DataFrame.
        All rows are inserted in a single commit. If any row fails, the entire transaction is rolled back.

        Args:
            df (pandas.DataFrame): DataFrame containing the rows to insert.
            table_name (str): Target table name, e.g., "schema.table".
            on_duplicate (Optional[str]): Conflict handling mode ("ignore", "replace", or None).
            return_failures (bool): If True, returns a DataFrame of failed rows with error messages on failure.
            return_status (bool): If True, includes a success flag in the return value.

        Returns:
            Union[
                tuple[pandas.DataFrame, bool],
                tuple[pandas.DataFrame, None],
                tuple[None, bool],
                tuple[None, None]
            ]: A tuple containing:
                - A DataFrame of failed records (if any, else empty DataFrame, and `return_failures` is True) or None.
                - A success flag (if `return_status` is True), otherwise None.

        Raises:
            InvalidSQLOperation: If SQL insert generation fails or arguments are malformed.
            SQLExecutionError: If duplicate-handling clause generation fails.
            UnsupportedDatabaseType: If the current database type is unsupported for insert generation.
            BadArgumentsBulk: If the input DataFrame is empty or invalid.
            BaseSQLConversionError: If argument conversion during delegated insert fails.
            DBClientClosedError: If the instance has already been closed.
        """
        # Check if close hasn't been called yet
        self._check_closed()

        # Check that df is not empty
        try:
            _validate_args_for_bulk(df)
            logger.debug("Valid args for insert_many")
        except BadArgumentsBulk as e:
            logger.error(f"Invalid arguments for insert_many {e}")
            raise

        try:
            column_name = list(df.columns)
            sql = _build_insert_statement(
                table_name=table_name, columns=column_name, db_type=self._db_type
            )
        except UnsupportedDatabaseType as e:
            logger.error(
                f"Could not generate insert statement for table '{table_name}': {e}"
            )
            raise

        # Ignore duplicates logic
        try:
            sql = _apply_on_duplicate_clause(sql, self._db_type, on_duplicate)
        except SQLExecutionError as e:
            logger.error(f"Duplicate handling logic error {e}")
            raise

        return self.execute_many(
            sql=sql,
            args=df,
            on_duplicate=None,  # already applied
            return_failures=return_failures,
            return_status=return_status,
        )

    ### --- Multiple transactions (not atomic) --- ###

    ### --- Execute batch (Threaded, Multiple transactions, Multiple args) --- ###

    def execute_batch(
        self,
        sql: str,
        args: Union[
            list[tuple[Any, ...]],
            list[dict[str, Any]],
            tuple[Any, ...],
            dict[str, Any],
            pd.DataFrame,
        ],
        chunk_size: int = 512,
        max_workers: Optional[int] = None,
        on_duplicate: Optional[str] = None,
        return_failures: bool = True,
        return_status: bool = False,
    ) -> Union[
        tuple[pd.DataFrame, bool],
        tuple[pd.DataFrame, None],
        tuple[None, bool],
        tuple[None, None],
    ]:
        """
        Executes a SQL operation (INSERT, UPDATE, DELETE) in parallel batches using threads.

        The input data is split into chunks and each chunk is executed in a separate thread. The method supports
        error capture per chunk, duplicate-handling clauses, and optional success/failure reporting.

        Args:
            sql (str): SQL statement with placeholders (e.g., :id, :value).
            args (Union[list[tuple[Any, ...]], list[dict[str, Any]], tuple[Any, ...], dict[str, Any], pandas.DataFrame]):
                Rows of parameters to bind to the SQL query.
            chunk_size (int): Number of rows per batch. Defaults to 512.
            max_workers (Optional[int]): Maximum number of concurrent threads. Defaults to internal pool size.
            on_duplicate (Optional[str]): Conflict resolution mode ("ignore", "replace", or None).
            return_failures (bool): If True, includes failed records with error messages in the result.
            return_status (bool): If True, includes a boolean success flag in the result.

        Returns:
            Union[
                tuple[pandas.DataFrame, bool],
                tuple[pandas.DataFrame, None],
                tuple[None, bool],
                tuple[None, None]
            ]: A tuple containing:
                - A DataFrame of failed records (if any, else empty DataFrame, and `return_failures` is True) or None.
                - A success flag (if `return_status` is True), otherwise None.

        Raises:
            InvalidSQLOperation: If the SQL or arguments are invalid or cannot be processed.
            BadArgumentsBulk: If no valid rows are provided.
            SQLExecutionError: If duplicate-handling clause generation fails.
            BaseSQLConversionError: If argument conversion fails during SQL preparation.
            UnsupportedMultiThreadedDatabase: If multithreaded writes are attempted on SQLite.
            LimitMaxWorkersError: If max_workers exceeds the available thread pool capacity.
            DBClientClosedError: If the instance has already been closed.
        """
        # Check if close hasn't been called yet
        self._check_closed()

        # Check if args were provided (necessary for chunks otherwise use execute single)
        try:
            _validate_args_for_bulk(args)
            logger.debug("Valid args for execute_batch")
        except BadArgumentsBulk as e:
            logger.error(f"Invalid arguments for execute_batch {e}")
            raise

        if self._db_type == "sqlite":
            logger.error(
                "Threaded writes are not supported on SQLite. Use execute_many or execute instead. "
            )
            raise UnsupportedMultiThreadedDatabase(self._db_type)

        # Check if max_worker given by user above total_pool_size
        if max_workers is not None and max_workers > self._total_pool_capacity:
            raise LimitMaxWorkersError(max_workers, self._total_pool_capacity)

        # Convert params to sqlalchemy compatible placeholders
        try:
            sql, args = _convert_dbapi_to_sqlalchemy_style(sql, args)
            if isinstance(args, dict):
                args = [args]
            # Narrow the type explicitly for MyPy
            args = cast(list[dict[str, Any]], args)
        except BaseSQLConversionError as e:
            logger.error(f"Invalid args format: {e}")
            raise

        # Ignore duplicates logic
        try:
            sql = _apply_on_duplicate_clause(sql, self._db_type, on_duplicate)
        except SQLExecutionError as e:
            logger.error(f"Duplicate handling logic error {e}")
            raise

        # Split up insert/update in chunks and initialize failed_record list
        chunks = [args[i : i + chunk_size] for i in range(0, len(args), chunk_size)]
        failed_records = []

        # Create insert chunk function
        def execute_chunk(chunk_args: list[dict[str, Any]], chunk_num: int) -> None:
            # noinspection PyShadowingNames
            try:
                with self._engine.begin() as conn:
                    conn.execute(text(sql), chunk_args)
            # Silent failing and collecting failed args in df
            except Exception as e:
                logger.warning(f"Chunk {chunk_num} failed: {e}")
                logger.debug(f"SQL: {sql}")
                logger.debug(f"Args: {chunk_args}")
                for record in chunk_args:
                    failed_records.append(
                        {**record, "chunk_index": chunk_num, "error_message": str(e)}
                    )

        # Create new executor that we'll dispose later if max_workers specified and different from max_workers at init
        if max_workers is not None:
            execute_executor = ThreadPoolExecutor(max_workers=max_workers)
            temp_executor = True
        else:
            execute_executor = self._executor
            temp_executor = False

        # Launch threads with executor
        iterable = zip(chunks, range(len(chunks)))

        for _ in tqdm(
            execute_executor.map(lambda x: execute_chunk(*x), iterable),
            total=len(chunks),
            desc="Inserting chunks",
        ):
            pass

        # Shutdown temp executor if was created
        if temp_executor:
            execute_executor.shutdown(wait=False)

        if failed_records:
            logger.warning(
                f"{len(failed_records)} record(s) across some chunk(s) failed during execution. "
                "You can inspect or retry them using the returned DataFrame if return_failures=True."
            )
            if return_failures and return_status:
                return pd.DataFrame(failed_records), False
            elif return_status:
                logger.info(
                    "return_failures=False, failed records will not be returned."
                )
                return None, False
            else:
                logger.info(
                    "return_failures=False, failed records will not be returned."
                )
                return None, None

        else:
            logger.info("All chunks executed successfully.")
            if return_failures and return_status:
                return pd.DataFrame(), True
            elif return_failures:
                return pd.DataFrame(), None
            elif return_status:
                return None, False
            else:
                return None, None

    ### --- Insert batch (Threaded, Multiple transactions, Multiple args, Inserts Only) --- ###

    def insert_batch(
        self,
        df: pd.DataFrame,
        table_name: str,
        chunk_size: int = 512,
        max_workers: Optional[int] = None,
        on_duplicate: Optional[str] = None,
        return_failures: bool = True,
        return_status: bool = False,
    ) -> Union[
        tuple[pd.DataFrame, bool],
        tuple[pd.DataFrame, None],
        tuple[None, bool],
        tuple[None, None],
    ]:
        """
        Inserts a pandas DataFrame into a SQL table using concurrent threaded chunking.

        This wraps `execute_batch()` by automatically generating an INSERT statement based on the DataFrame
        columns and table name. Data is split into chunks and written concurrently using a thread pool.

        Args:
            df (pandas.DataFrame): The DataFrame containing rows to insert. Columns must match the target table schema.
            table_name (str): Full table name, e.g., "schema.table".
            chunk_size (int): Number of rows per batch. Defaults to 512.
            max_workers (Optional[int]): Maximum number of concurrent threads. Defaults to internal pool size.
            on_duplicate (Optional[str]): Conflict resolution mode ("ignore", "replace", or None).
            return_failures (bool): If True, includes failed records with error messages in the result.
            return_status (bool): If True, includes a boolean success flag in the result.

        Returns:
            Union[
                tuple[pandas.DataFrame, bool],
                tuple[pandas.DataFrame, None],
                tuple[None, bool],
                tuple[None, None]
            ]: A tuple containing:
                - A DataFrame of failed records (if any, else empty DataFrame, and `return_failures` is True) or None.
                - A success flag (if `return_status` is True), otherwise None.

        Raises:
            BadArgumentsBulk: If the input DataFrame is empty or invalid.
            UnsupportedDatabaseType: If the current database type does not support insert generation.
            UnsupportedMultiThreadedDatabase: If multithreaded inserts are attempted on SQLite.
            LimitMaxWorkersError: If max_workers exceeds available thread pool capacity.
            SQLExecutionError: If duplicate-handling logic insertion fails.
            BaseSQLConversionError: If argument conversion fails internally.
            DBClientClosedError: If the instance has already been closed.
        """

        # Check engine is not closed
        self._check_closed()

        # Check that df is not empty
        try:
            _validate_args_for_bulk(df)
            logger.debug("Valid args for insert_many")
        except BadArgumentsBulk as e:
            logger.error(f"Invalid arguments for insert_many {e}")
            raise

        # Check that it is not a sqlite db
        if self._db_type == "sqlite":
            logger.error(
                "Threaded writes are not supported on SQLite. Use insert_many or execute instead."
            )
            raise UnsupportedMultiThreadedDatabase(self._db_type)

        # Check if max_worker given by user above total_pool_size
        if max_workers is not None and max_workers > self._total_pool_capacity:
            raise LimitMaxWorkersError(max_workers, self._total_pool_capacity)

        # Get a sql string for the table to use it in execute_chunk
        try:
            column_name = list(df.columns)
            sql = _build_insert_statement(
                table_name=table_name, columns=column_name, db_type=self._db_type
            )
        except UnsupportedDatabaseType as e:
            logger.error(
                f"Could not generate insert statement for table '{table_name}': {e}"
            )
            raise

        # Ignore duplicates logic
        try:
            sql = _apply_on_duplicate_clause(sql, self._db_type, on_duplicate)
        except SQLExecutionError as e:
            logger.error(f"Duplicate handling logic error {e}")
            raise

        # Call execute_batch
        return self.execute_batch(
            sql=sql,
            args=df,
            chunk_size=chunk_size,
            max_workers=max_workers,
            on_duplicate=None,  # already applied in here
            return_failures=return_failures,
            return_status=return_status,
        )
