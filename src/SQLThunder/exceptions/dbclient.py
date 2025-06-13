### --- Internal package imports --- ###
from .base import DBClientError

### --- Exceptions definition --- ###


class DBClientClosedError(DBClientError):
    """Raised when a closed DBClient instance is used."""

    def __init__(self) -> None:
        message = (
            "DBClient is closed and can no longer be used. "
            "Initiate a new instance if necessary or run DBClient.reopen_connection() . "
        )
        super().__init__(message)


class DBClientSessionClosedError(DBClientError):
    """Raised when a closed DBClient instance is used in a DBSession wrapper."""

    def __init__(self, session_label: str) -> None:
        message = (
            f"DBClient passed to DBSession '{session_label}' is closed. "
            "Enable `auto_reopen=True` or manually call `reopen_connection()`."
        )
        super().__init__(message)


class ClientInitializationError(DBClientError):
    """Raised when DBClient fails to initialize."""

    def __init__(self, config_path: str, original_exception: Exception) -> None:
        super().__init__(
            f"Failed to initialize DBClient with config '{config_path}': {original_exception}"
        )


# Engine creation
class EngineCreationError(DBClientError):
    """Base class for engine creation errors."""

    pass


class DriverNotFoundError(EngineCreationError):
    """Raised when SQLAlchemy cannot find the specified DB driver module."""

    def __init__(self, driver_name: str) -> None:
        super().__init__(
            f"Missing driver module {driver_name}. Reinstall {driver_name} using pip install {driver_name}. "
        )


class SQLAlchemyEngineError(EngineCreationError):
    """Raised for general SQLAlchemy engine creation failures."""

    def __init__(self, original_exception: Exception) -> None:
        exc_type = type(original_exception).__name__
        super().__init__(
            f"Engine creation failed with {exc_type}: {original_exception}"
        )
