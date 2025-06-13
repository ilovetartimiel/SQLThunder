### --- Base Exceptions definition --- ###


class BaseSQLConversionError(Exception):
    """Base class for all SQL to SQLAlchemy format conversion related errors."""

    pass


class ConfigFileError(Exception):
    """Base class for configuration errors."""

    pass


class DBClientError(Exception):
    """Base class for DBClient errors."""

    pass


class SQLExecutionError(Exception):
    """Base class for SQL execution-related errors."""

    pass


class FileSaveError(Exception):
    """Raised when saving to disk fails."""

    pass


class DataFileLoadError(Exception):
    """Raised when loading a data file from disk fails."""

    pass


class ThreadPoolLimitError(Exception):
    """Raised when max_workers exceeds the connection pool's total capacity."""

    pass
