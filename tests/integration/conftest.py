### --- Standard library imports --- ###
import socket
import time
from pathlib import Path

### --- Third-party imports --- ###
import numpy as np
import pandas as pd
import pytest
import yaml

### --- Internal package imports --- ###
from SQLThunder.core.client import DBClient

### --- Utility Functions --- ###


def wait_for_service(host: str, port: int, timeout: int = 30) -> None:
    """
    Wait until a TCP service becomes available (e.g., MySQL/PostgreSQL containers).

    Args:
        host (str): Host to connect to.
        port (int): Port to connect to.
        timeout (int): Maximum wait time in seconds.

    Raises:
        RuntimeError: If service is not available before timeout.
    """
    start = time.time()
    while True:
        try:
            with socket.create_connection((host, port), timeout=1):
                return
        except OSError:
            if time.time() - start > timeout:
                raise RuntimeError(f"Timeout waiting for {host}:{port}")
            time.sleep(1)


def write_temp_config(tmp_path: Path, config_dict: dict) -> Path:
    """
    Write a temporary YAML config file to disk.

    Args:
        tmp_path (Path): Temporary directory path from pytest.
        config_dict (dict): Database config content.

    Returns:
        Path: Path to the written YAML file.
    """
    config_file = tmp_path / "db_config.yaml"
    with open(config_file, "w") as f:
        yaml.dump(config_dict, f)
    return config_file


### --- Database Config Fixtures --- ###


@pytest.fixture(scope="session")
def postgres_config_path(tmp_path_factory) -> Path:
    wait_for_service("localhost", 5433)
    config = {
        "db_type": "postgresql",
        "user": "test_user",
        "password": "test_password",
        "host": "localhost",
        "port": 5433,
        "database": "test_db",
        "connect_timeout": 30,
    }
    return write_temp_config(tmp_path_factory.mktemp("configs"), config)


@pytest.fixture(scope="session")
def mysql_config_path(tmp_path_factory) -> Path:
    wait_for_service("localhost", 3307)
    config = {
        "db_type": "mysql",
        "user": "test_user",
        "password": "test_password",
        "host": "localhost",
        "port": 3307,
        "database": "test_db",
    }
    return write_temp_config(tmp_path_factory.mktemp("configs"), config)


@pytest.fixture(scope="session")
def sqlite_config_path(tmp_path_factory) -> Path:
    tmp_dir = tmp_path_factory.mktemp("sqlite_data")
    db_path = tmp_dir / "sqlite_test.db"
    db_path.touch(exist_ok=True)
    config = {"db_type": "sqlite", "path": str(db_path.resolve())}
    return write_temp_config(tmp_path_factory.mktemp("configs"), config)


@pytest.fixture(scope="session")
def db_config_paths(
    postgres_config_path, mysql_config_path, sqlite_config_path
) -> dict:
    """
    Returns paths to config files for all supported DBs.

    Returns:
        dict: Mapping of db_type to config path.
    """
    return {
        "postgres": str(postgres_config_path),
        "mysql": str(mysql_config_path),
        "sqlite": str(sqlite_config_path),
    }


### --- Client & Table Fixtures --- ###


@pytest.fixture(scope="session")
def db_client(request, db_config_paths):
    """
    Parametrized DBClient fixture.

    Param:
        request.param = {
            "db": "postgres" | "mysql" | "sqlite",
            "kwargs": {...}
        }

    Yields:
        DBClient: Connected client for requested DB type.
    """
    db_type = request.param.get("db")
    kwargs = request.param.get("kwargs", {})
    config_path = db_config_paths[db_type]

    client = DBClient(config_file_path=config_path, **kwargs)
    yield client
    client.close()


@pytest.fixture(scope="session")
def setup_test_table(request, db_client) -> str:
    """
    Create and tear down a shared test table across DBs.

    Returns:
        str: Table name used in tests.
    """
    table_name = "test_table"
    db_client.execute(f"DROP TABLE IF EXISTS {table_name}")

    db_type = db_client._db_type
    type_map = {
        "sqlite": {
            "int": "INTEGER PRIMARY KEY",
            "str": "TEXT",
            "float": "REAL",
            "date": "TEXT",
        },
        "mysql": {
            "int": "INT PRIMARY KEY",
            "str": "VARCHAR(255)",
            "float": "FLOAT",
            "date": "DATE",
        },
        "postgresql": {
            "int": "INT PRIMARY KEY",
            "str": "TEXT",
            "float": "FLOAT",
            "date": "DATE",
        },
    }

    types = type_map[db_type]
    create_sql = f"""
    CREATE TABLE {table_name} (
        id {types['int']},
        name {types['str']},
        value {types['float']},
        created_at {types['date']}
    )
    """
    db_client.execute(create_sql)

    def teardown():
        if db_client.is_closed:
            db_client.reopen_connection()
        db_client.execute(f"DROP TABLE IF EXISTS {table_name}")

    request.addfinalizer(teardown)
    return table_name


@pytest.fixture(scope="function")
def truncate_test_table(db_client, setup_test_table) -> None:
    """
    Truncate or DELETE all rows before each test (per function).

    Args:
        db_client (DBClient): Connected client.
        setup_test_table (str): Name of the shared test table.
    """
    db_type = db_client._db_type
    if db_type in ("mysql", "postgresql"):
        db_client.execute(f"TRUNCATE TABLE {setup_test_table}")
    else:
        db_client.execute(f"DELETE FROM {setup_test_table}")


### --- Test Data Fixtures --- ###


@pytest.fixture(scope="session")
def large_dataframe():
    """
    Generate a consistent 100k-row test DataFrame.

    Returns:
        pd.DataFrame: Mock dataset for insert/read tests.
    """
    n = 100_000
    df = pd.DataFrame(
        {
            "id": np.arange(n),
            "name": [f"name_{i}" for i in range(n)],
            "value": np.random.uniform(0, 1000, size=n),
            "created_at": pd.date_range("2023-01-01", periods=n, freq="min").strftime(
                "%Y-%m-%d"
            ),
        }
    )
    return df
