### --- Standard library imports --- ###
import os
from tempfile import TemporaryDirectory

import pandas as pd

### --- Third-party imports --- ###
import pytest

from SQLThunder.exceptions import (
    DataFileLoadErrorUnknown,
    DataFileNotFoundError,
    FileOutputSaveError,
    UnsupportedDataFormatError,
)

### --- Internal package imports --- ###
from SQLThunder.utils.file_io import load_data, save_dataframe

### --- Test Save df --- ###


class TestSaveDataFrame:

    def test_save_csv_success(self):
        df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
        with TemporaryDirectory() as tmpdir:
            out_path = os.path.join(tmpdir, "test.csv")
            save_dataframe(df, "csv", out_path)
            loaded = pd.read_csv(out_path)
            pd.testing.assert_frame_equal(df, loaded)

    def test_save_excel_success(self):
        df = pd.DataFrame({"x": [10, 20], "y": [30, 40]})
        with TemporaryDirectory() as tmpdir:
            out_path = os.path.join(tmpdir, "test.xlsx")
            save_dataframe(df, "excel", out_path)
            loaded = pd.read_excel(out_path)
            pd.testing.assert_frame_equal(df, loaded)

    def test_invalid_format_raises(self):
        df = pd.DataFrame({"foo": [1, 2]})
        with TemporaryDirectory() as tmpdir:
            out_path = os.path.join(tmpdir, "file.invalid")
            with pytest.raises(FileOutputSaveError, match="Unsupported output format"):
                save_dataframe(df, "invalid_format", out_path)

    def test_save_to_nonexistent_dir(self):
        df = pd.DataFrame({"a": [1]})
        with TemporaryDirectory() as tmpdir:
            nested_path = os.path.join(tmpdir, "subdir", "data.csv")
            save_dataframe(df, "csv", nested_path)
            assert os.path.exists(nested_path)


### --- Test Load df --- ###


class TestLoadData:

    def test_load_csv_success(self):
        df = pd.DataFrame({"c": [7, 8], "d": [9, 10]})
        with TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "input.csv")
            df.to_csv(path, index=False)
            loaded = load_data(path)
            pd.testing.assert_frame_equal(df, loaded)

    def test_load_excel_success(self):
        df = pd.DataFrame({"m": [5, 6], "n": [1, 2]})
        with TemporaryDirectory() as tmpdir:
            path = os.path.join(tmpdir, "input.xlsx")
            df.to_excel(path, index=False)
            loaded = load_data(path)
            pd.testing.assert_frame_equal(df, loaded)

    def test_file_not_found_raises(self):
        with pytest.raises(DataFileNotFoundError):
            load_data("/nonexistent/path.csv")

    def test_unsupported_format_raises(self):
        with TemporaryDirectory() as tmpdir:
            fake = os.path.join(tmpdir, "unsupported.txt")
            with open(fake, "w") as f:
                f.write("unsupported content")
            with pytest.raises(UnsupportedDataFormatError):
                load_data(fake)

    def test_broken_file_raises_data_load_error(self):
        with TemporaryDirectory() as tmpdir:
            broken_path = os.path.join(tmpdir, "corrupt.csv")
            with open(broken_path, "w") as f:
                f.write("some\nbad\ncsv\nstructure\nwith,too,many,columns")
            with pytest.raises(DataFileLoadErrorUnknown):
                load_data(broken_path)
