### --- Internal package imports --- ###
from .base import ConfigFileError, ThreadPoolLimitError

### --- Exceptions definition --- ###


class ConfigFileNotFoundError(ConfigFileError):
    """Raised when the config file path does not exist."""

    def __init__(self, path: str) -> None:
        message = (
            f"Configuration file not found: {path} "
            "You can use relative path or absolute path"
        )
        super().__init__(message)


class SSLFileNotFoundError(ConfigFileError):
    """Raised when an expected SSL file is missing."""

    def __init__(self, key: str, path: str) -> None:
        super().__init__(f"SSL file '{key}' not found at: {path}")


class ConfigFileParseError(ConfigFileError):
    """Raised when the YAML config file could not be parsed."""

    def __init__(self, path: str) -> None:
        super().__init__(f"Failed to parse yaml configuration file: {path}")


class ConfigFileUnknownError(ConfigFileError):
    """Raised when an unexpected error occurs while loading config."""

    def __init__(self, path: str, original_exception: Exception) -> None:
        exc_type = type(original_exception).__name__
        super().__init__(
            f"Unexpected error loading config file '{path}': [{exc_type}] {original_exception}"
        )


class InvalidDatabaseConfiguration(ConfigFileError):
    """Raised when required config keys are missing or malformed for a database connection."""

    def __init__(self, db_type: str, missing_keys: list[str]) -> None:
        message = (
            f"Missing required keys for '{db_type}' database: {missing_keys}. "
            "Expected keys: ['user', 'password', 'host', 'database'] "
            "(optional: 'port')"
        )
        super().__init__(message)


class MissingSQLitePath(ConfigFileError):
    """Raised when 'db_path' is not provided for SQLite configuration."""

    def __init__(self) -> None:
        message = (
            "SQLite configuration requires a 'db_path' key, but none was provided. "
            "SQLite requires 'db_path.db' or ':memory:'. "
        )
        super().__init__(message)


class UnsupportedDatabaseType(ConfigFileError):
    """Raised when an unknown or unsupported database type is provided."""

    def __init__(self, db_type: str) -> None:
        super().__init__(
            f"Unsupported database type: '{db_type}'. Supported types: sqlite, mysql, postgresql."
        )


class LimitMaxWorkersError(ThreadPoolLimitError):
    """Raised when max_workers exceeds the connection pool's total capacity."""

    def __init__(self, max_workers: int, total_pool_capacity: int) -> None:
        message = (
            f"max_workers ({max_workers}) exceeds total pool capacity ({total_pool_capacity}). "
            "Adjust max_workers, pool_size, or max_overflow accordingly. "
        )
        super().__init__(message)
