### --- Third-party imports --- ###
import pytest
import yaml

### --- Internal package imports --- ###
from SQLThunder.core.client import DBClient
from SQLThunder.exceptions.config import ConfigFileError
from SQLThunder.exceptions.execution import DatabaseConnectionError

### --- Helper function --- ###


def yaml_dump(data: dict) -> str:
    """Helper to dump YAML cleanly from dicts."""
    return yaml.dump(data)


### --- Test DBClient Lifecycle --- ###


class TestDBClientLifecycle:

    @pytest.mark.parametrize(
        "db_client",
        [
            {"db": "postgres", "kwargs": {"pool_size": 3}},
            {"db": "mysql", "kwargs": {"max_workers": 6}},
            {"db": "sqlite", "kwargs": {}},
        ],
        indirect=True,
    )
    def test_connection_init_and_close(self, db_client):
        assert db_client._engine is not None
        assert db_client.test_connection() is True
        db_client.close()
        assert db_client.is_closed is True

    @pytest.mark.parametrize(
        "db_client",
        [
            {"db": "postgres", "kwargs": {}},
            {"db": "mysql", "kwargs": {}},
            {"db": "sqlite", "kwargs": {}},
        ],
        indirect=True,
    )
    def test_using_closed_client_raises(self, db_client):
        db_client.close()
        assert db_client.test_connection() is False

    @pytest.mark.parametrize(
        "db_client",
        [
            {"db": "postgres", "kwargs": {}},
            {"db": "mysql", "kwargs": {}},
            {"db": "sqlite", "kwargs": {}},
        ],
        indirect=True,
    )
    def test_reopen_connection(self, db_client):
        db_client.close()
        assert db_client.is_closed is True
        db_client.reopen_connection()
        assert db_client.is_closed is False
        assert db_client.test_connection() is True

    @pytest.mark.parametrize(
        "db_client",
        [
            {"db": "postgres", "kwargs": {}},
            {"db": "mysql", "kwargs": {}},
            {"db": "sqlite", "kwargs": {}},
        ],
        indirect=True,
    )
    def test_redundant_reopen_logs_but_does_not_fail(self, db_client):
        db_client.reopen_connection()
        assert db_client.is_closed is False


### --- Test DBClient Failures --- ###


class TestDBClientFailures:

    @pytest.mark.parametrize(
        "broken_config",
        [
            {
                "db_type": "postgresql",
                "user": "test_user",
                "password": "test_password",
                "host": "localhost",
                "port": 9999,
                "database": "test_db",
                "connect_timeout": 3,
            },
            {
                "db_type": "mysql",
                "user": "test_user",
                "password": "test_password",
                "host": "localhost",
                "port": 9998,
                "database": "test_db",
            },
        ],
    )
    def test_connection_failure_wrong_port(self, tmp_path, broken_config):
        config_file = tmp_path / "bad_config.yaml"
        config_file.write_text(yaml_dump(broken_config))

        with pytest.raises(DatabaseConnectionError):
            DBClient(config_file_path=str(config_file))

    @pytest.mark.parametrize(
        "broken_config",
        [
            {
                "db_type": "postgresql",
                "user": "wrong_user",
                "password": "wrong_pass",
                "host": "localhost",
                "port": 5433,
                "database": "test_db",
                "connect_timeout": 3,
            },
            {
                "db_type": "mysql",
                "user": "wrong_user",
                "password": "wrong_pass",
                "host": "localhost",
                "port": 3307,
                "database": "test_db",
            },
        ],
    )
    def test_connection_failure_wrong_credentials(self, tmp_path, broken_config):
        config_file = tmp_path / "bad_config.yaml"
        config_file.write_text(yaml_dump(broken_config))

        with pytest.raises(DatabaseConnectionError):
            DBClient(config_file_path=str(config_file))

    def test_invalid_config_missing_required_keys(self, tmp_path):
        bad_config = {
            "host": "localhost",
            "port": 5433,
            "database": "test_db",
            # missing db_type, user, password
        }
        config_file = tmp_path / "invalid_config.yaml"
        config_file.write_text(yaml_dump(bad_config))

        with pytest.raises(ConfigFileError):
            DBClient(config_file_path=str(config_file))

    def test_ssl_misconfiguration_missing_file(self, tmp_path):
        config = {
            "db_type": "postgresql",
            "user": "test_user",
            "password": "test_password",
            "host": "localhost",
            "port": 5433,
            "database": "test_db",
            "connect_timeout": 3,
            "ssl_ca": "/nonexistent/path/ca.pem",
            "ssl_cert": "/nonexistent/path/cert.pem",
            "ssl_key": "/nonexistent/path/key.pem",
        }
        config_file = tmp_path / "bad_ssl_config.yaml"
        config_file.write_text(yaml_dump(config))

        with pytest.raises(ConfigFileError):
            DBClient(config_file_path=str(config_file))
