### --- Standard library imports --- ###
from typing import Any

### --- Internal package imports --- ###
from .base import BaseSQLConversionError

### --- Exceptions definition --- ###


class UnsupportedSQLArgsFormat(BaseSQLConversionError):
    """Raised when the provided args format is unsupported for SQL conversion."""

    def __init__(self, args: Any) -> None:
        message = (
            f"Unsupported args format of type {type(args).__name__}. "
            "Expected one of: tuple, list of tuples, dict, list of dicts, or pandas.DataFrame."
        )
        super().__init__(message)
