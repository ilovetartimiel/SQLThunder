### --- Standard library imports --- ###
import sys

### --- Third-party imports --- ###
import pytest

from SQLThunder.exceptions import (
    InvalidDatabaseConfiguration,
    MissingSQLitePath,
    UnsupportedDatabaseType,
)

### --- Internal package imports --- ###
from SQLThunder.utils.engine import _build_connect_args, _get_db_url

### --- Test Get DB URL --- ###


class TestGetDBUrl:

    def test_mysql_valid_config(self):
        config = {
            "user": "user",
            "password": "pass",
            "host": "localhost",
            "database": "testdb",
        }
        url, driver, db_type = _get_db_url(config, db_type="mysql")
        assert url.startswith("mysql+pymysql://")
        assert driver == "pymysql"
        assert db_type == "mysql"

    def test_postgres_valid_config_with_timeout(self):
        config = {
            "user": "user",
            "password": "pass",
            "host": "localhost",
            "database": "pgdb",
            "connect_timeout": 5,
        }
        url, driver, db_type = _get_db_url(config, db_type="postgresql")
        assert "postgresql+psycopg2://" in url
        assert "connect_timeout=5" in url
        assert driver == "psycopg2"

    def test_sqlite_memory(self):
        config = {"db_path": ":memory:"}
        url, driver, db_type = _get_db_url(config, db_type="sqlite")
        assert url == "sqlite://"
        assert driver == "sqlite"

    def test_sqlite_file_path(self):
        config = {"db_path": "~/test.db"}
        url, driver, db_type = _get_db_url(config, db_type="sqlite")
        assert (
            url.startswith("sqlite:///")
            if sys.platform.startswith("win")
            else url.startswith("sqlite:////")
        )
        assert "test.db" in url

    def test_sqlite_missing_path_raises(self):
        config = {}
        with pytest.raises(MissingSQLitePath):
            _get_db_url(config, db_type="sqlite")

    def test_missing_keys_for_mysql(self):
        config = {"user": "user", "host": "localhost"}
        with pytest.raises(InvalidDatabaseConfiguration) as exc:
            _get_db_url(config, db_type="mysql")
        assert "password" in str(exc.value)
        assert "database" in str(exc.value)

    def test_unsupported_db_type(self):
        config = {"db_path": ":memory:"}
        with pytest.raises(UnsupportedDatabaseType):
            _get_db_url(config, db_type="oracle")


### --- Test Build Connect Args --- ###


class TestBuildConnectArgs:

    def test_sqlite_args(self):
        res = _build_connect_args("sqlite", {}, {})
        assert res == {"check_same_thread": False}

    def test_mysql_connect_args_with_ssl_and_timeouts(self):
        ssl_paths = {
            "ssl_ca": "/path/ca.pem",
            "ssl_cert": "/path/cert.pem",
            "ssl_key": "/path/key.pem",
        }
        config = {"connect_timeout": 7, "read_timeout": 20, "write_timeout": 25}
        res = _build_connect_args("pymysql", ssl_paths, config)
        assert res["ssl"]["ca"] == "/path/ca.pem"
        assert res["connect_timeout"] == 7
        assert res["read_timeout"] == 20
        assert res["write_timeout"] == 25

    def test_postgres_connect_args_with_ssl_and_metadata(self):
        ssl_paths = {
            "ssl_ca": "/path/ca.crt",
            "ssl_cert": "/path/client.crt",
            "ssl_key": "/path/client.key",
        }
        config = {
            "ssl_mode": "verify-ca",
            "application_name": "myapp",
            "pg_options": "-c statement_timeout=5000",
        }
        res = _build_connect_args("psycopg2", ssl_paths, config)
        assert res["sslmode"] == "verify-ca"
        assert res["sslrootcert"] == "/path/ca.crt"
        assert res["sslcert"] == "/path/client.crt"
        assert res["sslkey"] == "/path/client.key"
        assert res["application_name"] == "myapp"
        assert res["options"] == "-c statement_timeout=5000"
