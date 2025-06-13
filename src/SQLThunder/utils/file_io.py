### --- Standard library imports --- ###
import os

### --- Third-party imports --- ###
import pandas as pd

### --- Internal package imports --- ###
from SQLThunder.exceptions import (
    DataFileLoadErrorUnknown,
    DataFileNotFoundError,
    FileOutputSaveError,
    UnsupportedDataFormatError,
)

### --- Utils --- ###


def save_dataframe(df: pd.DataFrame, output: str, output_path: str) -> None:
    """
    Save a pandas DataFrame to disk in CSV or Excel format.

    Args:
        df (pd.DataFrame): The DataFrame to save.
        output (str): Output format, either "csv" or "excel".
        output_path (str): Path to the output file. Can be relative or absolute.

    Raises:
        FileOutputSaveError: If saving fails or the output format is unsupported.
    """
    try:
        # Expand ~ and normalize/absolutize the path
        expanded_path = os.path.expanduser(output_path)
        abs_path = os.path.abspath(os.path.normpath(expanded_path))

        # Ensure the parent directory exists
        dir_path = os.path.dirname(abs_path)
        os.makedirs(dir_path, exist_ok=True)

        # Save the DataFrame
        if output == "csv":
            df.to_csv(abs_path, index=False)
        elif output == "excel":
            df.to_excel(abs_path, index=False)
        else:
            raise FileOutputSaveError(
                f"Unsupported output format: '{output}'. Must be 'csv' or 'excel'."
            )

    except Exception as e:
        raise FileOutputSaveError(
            f"Failed to save DataFrame to {output_path}: {e}"
        ) from e


def load_data(file_path: str) -> pd.DataFrame:
    """
    Load a CSV or Excel file into a pandas DataFrame.

    Args:
        file_path (str): Path to the input file (.csv, .xls, .xlsx).

    Returns:
        pd.DataFrame: The loaded data.

    Raises:
        DataFileNotFoundError: If the file is not found.
        UnsupportedDataFormatError: If the file format is not supported.
        DataFileLoadErrorUnknown: If an unexpected error occurs during file reading.
    """
    try:
        # Expand and normalize path
        expanded_path = os.path.expanduser(file_path)
        abs_path = os.path.abspath(os.path.normpath(expanded_path))

        if not os.path.isfile(abs_path):
            raise DataFileNotFoundError(file_path)

        # Check extension
        ext = os.path.splitext(abs_path)[1].lower()
        if ext == ".csv":
            return pd.read_csv(abs_path)
        elif ext in {".xls", ".xlsx"}:
            return pd.read_excel(abs_path)
        else:
            raise UnsupportedDataFormatError(ext)

    except (DataFileNotFoundError, UnsupportedDataFormatError):
        raise  # Reraise cleanly
    except Exception as e:
        raise DataFileLoadErrorUnknown(file_path, e)
