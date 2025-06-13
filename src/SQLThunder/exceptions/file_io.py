### --- Internal package imports --- ###
from .base import DataFileLoadError, FileSaveError

### --- Exceptions definition --- ###


class FileOutputSaveError(FileSaveError):
    """Raised when saving a DataFrame to disk fails."""

    def __init__(self, message: str) -> None:
        super().__init__(message)


class DataFileNotFoundError(DataFileLoadError):
    """Raised when the data file provided by the user is not found."""

    def __init__(self, path: str) -> None:
        super().__init__(f"Data file not found: {path}")


class UnsupportedDataFormatError(DataFileLoadError):
    """Raised when data file format is unsupported"""

    def __init__(self, ext: str) -> None:
        message = (
            f"Unsupported file extension '{ext}' ."
            "Only .csv, .xls, and .xlsx are supported."
        )
        super().__init__(message)


class DataFileLoadErrorUnknown(DataFileLoadError):
    """Raised when unknown error happen during data file loading"""

    def __init__(self, path: str, original_exception: Exception) -> None:
        super().__init__(f"Failed to load data file '{path}': {original_exception}")
