### --- Standard library imports --- ###
from typing import Optional

### --- Third-party imports --- ###
from sqlparse.sql import Token

### --- Internal package imports --- ###
from .base import SQLExecutionError

### --- Exceptions definition --- ###


class DatabaseConnectionError(SQLExecutionError):
    """Raised when a test connection to the database fails."""

    def __init__(
        self, message: str, original_exception: Optional[Exception] = None
    ) -> None:
        self.original_exception = original_exception
        message = f"{message}: {original_exception}"
        super().__init__(message)


class ReopenConnectionError(SQLExecutionError):
    """Raised when a DBClient fails to reopen a closed connection."""

    def __init__(
        self, message: str, original_exception: Optional[Exception] = None
    ) -> None:
        self.original_exception = original_exception
        message = f"{message}: {original_exception}"
        super().__init__(message)


class ChunkExecutionError(SQLExecutionError):
    """
    Raised when a specific chunk fails during execution. Can be useful for next version if fail-fast option added.
    Contains the chunk index and original exception.
    """

    def __init__(self, chunk_index: int, original_exception: Exception) -> None:
        self.chunk_index = chunk_index
        self.original_exception = original_exception
        message = f"Chunk {chunk_index} failed: {str(original_exception)}"
        super().__init__(message)


class BatchPartialExecutionError(SQLExecutionError):
    """
    Raised when one or more chunks fail.
    Not raised by default — useful if you want fail-fast logic.
    """

    def __init__(self, failed_chunk_indices: list[int]) -> None:
        self.failed_chunk_indices = failed_chunk_indices
        message = f"{len(failed_chunk_indices)} chunk(s) failed: {failed_chunk_indices}"
        super().__init__(message)


class InvalidSQLOperation(SQLExecutionError):
    """
    Raised when the SQL statement is malformed or incompatible with given args.
    """

    def __init__(self, message: str) -> None:
        super().__init__(f"Invalid SQL operation: {message}")


class UnsupportedDuplicateHandling(SQLExecutionError):
    """
    Raised when the `on_duplicate` behavior is not supported for a specific database backend.
    """

    def __init__(self, message: str) -> None:
        super().__init__(f"Unsupported duplicate handling: {message}")


class BadArgumentsBulk(SQLExecutionError):
    """
    Raised when args for bulk insert/execute are empty.
    """

    def __init__(self, message: str) -> None:
        super().__init__(f"{message}")


class UnsupportedMultiThreadedDatabase(SQLExecutionError):
    """
    Raised when the multithreaded execute/insert behavior is not supported for a specific database backend.
    """

    def __init__(self, db_type: str) -> None:
        super().__init__(f"Unsupported database type for multithreading: {db_type}")


class QueryExecutionError(SQLExecutionError):
    """Raised when a SQL SELECT query fails during execution."""

    def __init__(self, original_exception: Exception) -> None:
        super().__init__(f"Query execution failed: {original_exception}")


class QueryResultFormatError(SQLExecutionError):
    """Raised when an unsupported return_type is provided to query."""

    def __init__(self, original_return_type: str) -> None:
        message = (
            f"Unsupported return type for query: {original_return_type} "
            "Supported return types: 'df', 'list', 'raw', 'None'"
        )
        super().__init__(message)


class QuerySelectOnlyError(SQLExecutionError):
    """Raised when another operation than SELECT is used with query."""

    def __init__(self) -> None:
        super().__init__("Query and query_batch only support read operations (SELECT) ")


class QueryDisallowedClauseError(SQLExecutionError):
    """Raised when an unsupported clause (limit/offset) is used with query_keyed() or query_batch()"""

    def __init__(self, wrong_token: Token) -> None:
        message = (
            f"Disallowed clause '{wrong_token.value.upper()}' found. "
            "Don't use LIMIT or OFFSET in your SQL statement when using query_keyed or query_batch."
        )
        super().__init__(message)
