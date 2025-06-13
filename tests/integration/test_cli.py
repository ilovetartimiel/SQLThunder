### --- Standard library imports --- ###
import subprocess
from pathlib import Path

import pandas as pd

### --- Third-party imports --- ###
import pytest

### --- Fixtures --- ###


@pytest.fixture(scope="module")
def populate_test_table_for_cli(db_client, setup_test_table):
    """
    Populate the shared test_table with static rows for CLI integration testing.

    Skips the test if the DB is not MySQL (intended for subprocess-based CLI testing only).
    """
    if db_client._db_type != "mysql":
        pytest.skip("populate_test_table_for_cli is only used for MySQL CLI tests.")

    db_client.execute(f"DELETE FROM {setup_test_table}")
    df = pd.DataFrame(
        [
            {"id": 1, "name": "Alice", "value": 100.0, "created_at": "2023-01-01"},
            {"id": 2, "name": "Bob", "value": 200.0, "created_at": "2023-01-02"},
        ]
    )
    db_client.insert_many(df, setup_test_table)
    yield
    db_client.execute(f"DELETE FROM {setup_test_table}")


@pytest.fixture(scope="session")
def mysql_config_path(db_config_paths) -> Path:
    """Extract MySQL config path from the full config dictionary."""
    return db_config_paths["mysql"]


@pytest.fixture(scope="module")
def sample_data_path(tmp_path_factory) -> str:
    """
    Write a small Excel file with test rows to disk.

    Returns:
        str: Path to the saved file.
    """
    path = tmp_path_factory.mktemp("cli_data") / "sample.xlsx"
    df = pd.DataFrame(
        {
            "id": [1, 2],
            "name": ["Alice", "Bob"],
            "value": [100.0, 200.0],
            "created_at": ["2023-01-01", "2023-01-02"],
        }
    )
    df.to_excel(path, index=False)
    return str(path)


### --- Test CLI Execute --- ###


class TestCLIExecute:

    def test_cli_execute_mysql(self, mysql_config_path):
        res = subprocess.run(
            [
                "python",
                "-m",
                "SQLThunder",
                "execute",
                "DELETE FROM test_table",
                "-c",
                mysql_config_path,
            ],
            capture_output=True,
            text=True,
        )

        assert res.returncode == 0
        assert "executed successfully" in res.stdout.lower()


### --- Test CLI Insert --- ###


class TestCLIInsert:

    def test_cli_insert(self, mysql_config_path, sample_data_path):
        res = subprocess.run(
            [
                "python",
                "-m",
                "SQLThunder",
                "insert",
                sample_data_path,
                "test_table",
                "-c",
                mysql_config_path,
            ],
            capture_output=True,
            text=True,
        )

        assert res.returncode == 0

    def test_cli_insert_batch(self, mysql_config_path, sample_data_path):
        res = subprocess.run(
            [
                "python",
                "-m",
                "SQLThunder",
                "insert",
                sample_data_path,
                "test_table",
                "-c",
                mysql_config_path,
                "--batch",
                "--chunk_size",
                "1",
                "--max_workers",
                "2",
            ],
            capture_output=True,
            text=True,
        )

        assert res.returncode == 0


### --- Test CLI Query --- ###


class TestCLIQuery:

    @pytest.mark.parametrize("db_client", [{"db": "mysql"}], indirect=True)
    def test_cli_query(self, mysql_config_path, populate_test_table_for_cli):
        res = subprocess.run(
            [
                "python",
                "-m",
                "SQLThunder",
                "query",
                "SELECT * FROM test_table",
                "-c",
                mysql_config_path,
                "--print",
            ],
            capture_output=True,
            text=True,
        )

        assert res.returncode == 0
        assert "alice" in res.stdout.lower()

    @pytest.mark.parametrize("db_client", [{"db": "mysql"}], indirect=True)
    def test_cli_query_key_based(self, mysql_config_path, populate_test_table_for_cli):
        res = subprocess.run(
            [
                "python",
                "-m",
                "SQLThunder",
                "query",
                "SELECT * FROM test_table",
                "-c",
                mysql_config_path,
                "--key_based",
                "--key_column",
                "id",
                "--key_column_type",
                "int",
            ],
            capture_output=True,
            text=True,
        )

        assert res.returncode == 0


### --- Test CLI Output --- ###


class TestCLIOutputValidation:

    def test_cli_output_flag_missing_path(self, mysql_config_path):
        res = subprocess.run(
            [
                "python",
                "-m",
                "SQLThunder",
                "query",
                "SELECT * FROM test_table",
                "-c",
                mysql_config_path,
                "--output",
                "csv",
            ],
            capture_output=True,
            text=True,
        )

        assert res.returncode != 0
        assert "--output and --output_path must be used together" in res.stderr.lower()

    def test_cli_output_path_missing_format(self, mysql_config_path, tmp_path):
        res = subprocess.run(
            [
                "python",
                "-m",
                "SQLThunder",
                "query",
                "SELECT * FROM test_table",
                "-c",
                mysql_config_path,
                "--output_path",
                str(tmp_path / "out.csv"),
            ],
            capture_output=True,
            text=True,
        )

        assert res.returncode != 0
        assert "--output and --output_path must be used together" in res.stderr.lower()


### --- Test CLI Query Keyed --- ###


class TestCLIKeyBasedValidation:

    def test_cli_key_based_missing_key_column(self, mysql_config_path):
        res = subprocess.run(
            [
                "python",
                "-m",
                "SQLThunder",
                "query",
                "SELECT * FROM test_table",
                "-c",
                mysql_config_path,
                "--key_based",
                "--key_column_type",
                "int",
            ],
            capture_output=True,
            text=True,
        )

        assert res.returncode != 0
        assert "--key_based requires --key_column to be given" in res.stderr.lower()

    def test_cli_key_based_missing_column_type(self, mysql_config_path):
        res = subprocess.run(
            [
                "python",
                "-m",
                "SQLThunder",
                "query",
                "SELECT * FROM test_table",
                "-c",
                mysql_config_path,
                "--key_based",
                "--key_column",
                "id",
            ],
            capture_output=True,
            text=True,
        )

        assert res.returncode != 0
        assert (
            "--key_based requires --key_column_type to be given" in res.stderr.lower()
        )

    def test_cli_key_based_string_missing_start_key(self, mysql_config_path):
        res = subprocess.run(
            [
                "python",
                "-m",
                "SQLThunder",
                "query",
                "SELECT * FROM test_table",
                "-c",
                mysql_config_path,
                "--key_based",
                "--key_column",
                "id",
                "--key_column_type",
                "string",
            ],
            capture_output=True,
            text=True,
        )

        assert res.returncode != 0
        assert "--key_based requires --start_key to be given" in res.stderr.lower()


### --- Test CLI Pooling/Concurrency --- ###


class TestCLIPoolAndConcurrency:

    def test_cli_invalid_worker_exceeds_pool(self, mysql_config_path, sample_data_path):
        res = subprocess.run(
            [
                "python",
                "-m",
                "SQLThunder",
                "insert",
                sample_data_path,
                "test_table",
                "-c",
                mysql_config_path,
                "--batch",
                "--chunk_size",
                "1",
                "--pool_size",
                "2",
                "--max_overflow",
                "1",
                "--max_workers",
                "10",
            ],
            capture_output=True,
            text=True,
        )

        assert res.returncode != 0
        assert "exceeds allowed maximum" in res.stderr.lower()

    def test_cli_only_pool_size_given(self, mysql_config_path, sample_data_path):
        res = subprocess.run(
            [
                "python",
                "-m",
                "SQLThunder",
                "insert",
                sample_data_path,
                "test_table",
                "-c",
                mysql_config_path,
                "--batch",
                "--pool_size",
                "5",
            ],
            capture_output=True,
            text=True,
        )

        assert res.returncode != 0
        assert (
            "both --pool_size and --max_overflow must be provided together"
            in res.stderr.lower()
        )
