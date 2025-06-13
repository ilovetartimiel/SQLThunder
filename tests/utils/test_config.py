### --- Standard library imports --- ###
import os
from tempfile import NamedTemporaryFile, TemporaryDirectory

### --- Third-party imports --- ###
import pytest

from SQLThunder.exceptions import (
    ConfigFileNotFoundError,
    ConfigFileParseError,
    ConfigFileUnknownError,
    SSLFileNotFoundError,
)

### --- Internal package imports --- ###
from SQLThunder.utils.config import _load_config, _resolve_ssl_paths

### --- Test Load Config --- ###


class TestLoadConfig:

    def test_load_valid_yaml_file(self):
        with NamedTemporaryFile("w", suffix=".yaml", delete=False) as tmp:
            tmp.write("db_type: mysql\nuser: test\n")
            tmp_path = tmp.name

        config = _load_config(tmp_path)
        assert config["db_type"] == "mysql"
        assert config["user"] == "test"

        os.remove(tmp_path)

    def test_file_not_found_raises(self):
        with pytest.raises(ConfigFileNotFoundError):
            _load_config("/nonexistent/config.yaml")

    def test_invalid_yaml_raises_parse_error(self):
        with NamedTemporaryFile("w", suffix=".yml", delete=False) as tmp:
            tmp.write("db:\n  - foo: bar\n    - baz")  # BAD YAML
            tmp_path = tmp.name

        with pytest.raises(ConfigFileParseError):
            _load_config(tmp_path)

        os.remove(tmp_path)

    def test_unexpected_exception_raises_unknown_error(self, monkeypatch):
        def mock_open(*args, **kwargs):
            raise RuntimeError("unexpected")

        monkeypatch.setattr("builtins.open", mock_open)

        with pytest.raises(ConfigFileUnknownError):
            _load_config("fake_path.yaml")


### --- Test Resolve SSL Paths --- ###


class TestResolveSSLPaths:

    def test_resolve_existing_paths(self):
        with TemporaryDirectory() as tmpdir:
            ca = os.path.join(tmpdir, "ca.pem")
            cert = os.path.join(tmpdir, "cert.pem")
            key = os.path.join(tmpdir, "key.pem")

            # Create empty files
            for path in [ca, cert, key]:
                with open(path, "w") as f:
                    f.write("")

            config = {"ssl_ca": ca, "ssl_cert": cert, "ssl_key": key}

            resolved = _resolve_ssl_paths(config)
            assert resolved["ssl_ca"] == os.path.abspath(ca)
            assert resolved["ssl_cert"] == os.path.abspath(cert)
            assert resolved["ssl_key"] == os.path.abspath(key)

    def test_missing_ssl_file_raises(self):
        test_path = "/nonexistent/path/ca.pem"
        config = {"ssl_ca": test_path}

        with pytest.raises(SSLFileNotFoundError) as exc:
            _resolve_ssl_paths(config)
        assert "ssl_ca" in str(exc.value)
        expected_abs_path = os.path.abspath(
            os.path.normpath(os.path.expanduser(test_path))
        )
        assert expected_abs_path in str(exc.value)

    def test_ignores_missing_optional_keys(self):
        with TemporaryDirectory() as tmpdir:
            ca = os.path.join(tmpdir, "ca.pem")
            with open(ca, "w") as f:
                f.write("")

            config = {"ssl_ca": ca}  # only required key present

            resolved = _resolve_ssl_paths(config)
            assert "ssl_ca" in resolved
            assert "ssl_cert" not in resolved
            assert "ssl_key" not in resolved
