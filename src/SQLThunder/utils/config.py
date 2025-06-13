### --- Standard library imports --- ###
import os
from typing import Any, cast

### --- Third-party imports --- ###
import yaml

### --- Internal package imports --- ###
from SQLThunder.exceptions import (
    ConfigFileNotFoundError,
    ConfigFileParseError,
    ConfigFileUnknownError,
    SSLFileNotFoundError,
)

### --- Utils --- ###


def _load_config(config_file_path: str) -> dict[str, Any]:
    """
    Load a YAML configuration file and return its contents as a dictionary.

    The path can be relative or absolute. Internally it is normalized and resolved.

    Args:
        config_file_path (str): Path to the YAML file (.yaml or .yml). Can use `~`.

    Returns:
        dict: Parsed configuration data.

    Raises:
        ConfigFileNotFoundError: If the file does not exist.
        ConfigFileParseError: If the file is not valid YAML.
        ConfigFileUnknownError: For unexpected errors during loading.

    Notes:
        This function supports both `.yaml` and `.yml` extensions.
    """
    expanded_path = os.path.expanduser(config_file_path)
    abs_path = os.path.abspath(os.path.normpath(expanded_path))
    try:
        with open(abs_path, "r") as config_file:
            return cast(dict[str, Any], yaml.safe_load(config_file))
    except FileNotFoundError:
        raise ConfigFileNotFoundError(abs_path)
    except yaml.YAMLError:
        raise ConfigFileParseError(abs_path)
    except Exception as e:
        raise ConfigFileUnknownError(abs_path, e)


def _resolve_ssl_paths(config: dict[str, Any]) -> dict[str, str]:
    """
    Validate and resolve absolute paths for SSL-related files in the config.

    Keys checked:
        - ssl_ca (required for some databases)
        - ssl_cert (optional)
        - ssl_key (optional)

    Args:
        config (dict[str, Any]): The parsed configuration dictionary.

    Returns:
        dict[str, str]: Dictionary of resolved paths {key: absolute_path}.

    Raises:
        SSLFileNotFoundError: If any specified SSL file does not exist.
    """
    ssl_keys = ["ssl_ca", "ssl_cert", "ssl_key"]
    resolved_paths = {}

    for key in ssl_keys:
        if key in config:
            expanded_path = os.path.expanduser(config[key])
            path = os.path.abspath(os.path.normpath(expanded_path))
            if not os.path.exists(path):
                raise SSLFileNotFoundError(key, path)
            resolved_paths[key] = path

    return resolved_paths
