### --- Standard library imports --- ###
import os
import sys
from typing import Any, Optional
from urllib.parse import quote_plus

### --- Internal package imports --- ###
from SQLThunder.exceptions import (
    InvalidDatabaseConfiguration,
    MissingSQLitePath,
    UnsupportedDatabaseType,
)

### --- Utils --- ###


def _get_db_url(
    config: dict[str, Any], db_type: Optional[str] = None
) -> tuple[str, str, str]:
    """
    Build a SQLAlchemy-compatible database URL string based on configuration.

    Supports MySQL, PostgreSQL, and SQLite with automatic path and credential resolution.

    Args:
        config (dict[str, Any]): Configuration dictionary with DB credentials and options.
        db_type (Optional[str]): Optional override for DB type ("mysql", "postgresql", "sqlite").

    Returns:
        tuple[str, str, str]: A tuple containing:
            - SQLAlchemy connection string
            - Driver name (e.g., "pymysql", "psycopg2", "sqlite")
            - Normalized db_type ("mysql", "postgresql", or "sqlite")

    Raises:
        MissingSQLitePath: If SQLite path is not provided.
        InvalidDatabaseConfiguration: If required MySQL/PostgreSQL keys are missing.
        UnsupportedDatabaseType: If db_type is not supported.
    """
    # Get db_type from flag or config
    db_type = (db_type or config.get("db_type", "mysql")).lower()

    supported_db = {"mysql", "sqlite", "postgresql"}

    if db_type not in supported_db:
        raise UnsupportedDatabaseType(db_type)

    # Check for sqlite since different URL format and return url
    if db_type == "sqlite":
        path = config.get("db_path") or config.get("path")
        if not path:
            raise MissingSQLitePath()

        # Handle in-memory DB
        if path == ":memory:":
            return "sqlite://", "sqlite", db_type

        # Normalize and expand
        path = os.path.expanduser(path)
        abs_path = os.path.abspath(os.path.normpath(path))

        if sys.platform.startswith("win"):
            # On Windows, SQLAlchemy needs the form: sqlite:///C:/path/to/db
            # Avoid extra leading slash, which causes incorrect path
            return f"sqlite:///{abs_path}", "sqlite", db_type
        else:
            # On Unix-based systems (Linux/macOS), four slashes are needed for absolute paths
            return f"sqlite:////{abs_path}", "sqlite", db_type

    # Check for required keys for mysql and postgresql
    required_keys = ["user", "password", "host", "database"]
    missing_keys = [key for key in required_keys if key not in config]
    if missing_keys:
        raise InvalidDatabaseConfiguration(db_type, missing_keys)

    # Get values of config
    user = quote_plus(config["user"])
    password = quote_plus(config["password"])
    host = quote_plus(config["host"])
    database = config["database"]
    port = config.get("port")
    connect_timeout = config.get("connect_timeout", 10)

    # Return URL for MySQL db
    if db_type == "mysql":
        port = port or 3306  # Specified port or default port
        return (
            f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}",
            "pymysql",
            db_type,
        )

    # Return URL for Postgres db
    else:
        port = port or 5432  # Specified port or default port
        return (
            f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
            f"?connect_timeout={connect_timeout}",
            "psycopg2",
            db_type,
        )


def _build_connect_args(
    driver: str, ssl_paths: dict[str, str], config: dict[str, Any]
) -> dict[str, Any]:
    """
    Construct SQLAlchemy `connect_args` dictionary based on driver, SSL, and optional config.

    Args:
        driver (str): One of "sqlite", "pymysql", or "psycopg2".
        ssl_paths (dict[str, str]): Dictionary with SSL file paths.
        config (dict[str, Any]): Configuration options such as timeouts or metadata.

    Returns:
        dict[str, Any]: SQLAlchemy-compatible connect_args dictionary.

    Notes:
        - For SQLite: disables thread check.
        - For MySQL: supports SSL, timeouts.
        - For PostgreSQL: supports SSL and extra metadata.
    """
    if driver == "sqlite":
        return {"check_same_thread": False}

    connect_args: dict[str, Any] = {}

    if driver == "pymysql":
        # SSL
        ssl_config = {}
        if "ssl_ca" in ssl_paths:
            ssl_config["ca"] = ssl_paths["ssl_ca"]
        if "ssl_cert" in ssl_paths:
            ssl_config["cert"] = ssl_paths["ssl_cert"]
        if "ssl_key" in ssl_paths:
            ssl_config["key"] = ssl_paths["ssl_key"]

        if ssl_config:
            connect_args["ssl"] = ssl_config

        # Timeouts
        connect_args["connect_timeout"] = config.get("connect_timeout", 10)
        connect_args["read_timeout"] = config.get("read_timeout", 30)
        connect_args["write_timeout"] = config.get("write_timeout", 30)

    elif driver == "psycopg2":
        # SSL
        ssl_mode = config.get("ssl_mode")
        if ssl_mode:
            connect_args["sslmode"] = ssl_mode
        if "ssl_ca" in ssl_paths:
            connect_args["sslrootcert"] = ssl_paths["ssl_ca"]
        if "ssl_cert" in ssl_paths:
            connect_args["sslcert"] = ssl_paths["ssl_cert"]
        if "ssl_key" in ssl_paths:
            connect_args["sslkey"] = ssl_paths["ssl_key"]

        # Metadata
        if "application_name" in config:
            connect_args["application_name"] = config["application_name"]
        if "pg_options" in config:
            connect_args["options"] = config["pg_options"]

    return connect_args
