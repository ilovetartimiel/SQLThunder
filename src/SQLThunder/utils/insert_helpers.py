### --- Standard library imports --- ###
import re
from typing import Optional

### --- Internal package imports --- ###
from SQLThunder.exceptions import UnsupportedDatabaseType, UnsupportedDuplicateHandling

### --- Utils --- ###


def _apply_on_duplicate_clause(
    sql: str, db_type: str, on_duplicate: Optional[str]
) -> str:
    """
    Modify an SQL INSERT statement to apply duplicate-handling behavior based on DB type.

    Supports:
        - "ignore": skips rows that conflict
        - "replace": overwrites existing rows (not supported on PostgreSQL)
        - None: no modification

    Args:
        sql (str): Raw SQL INSERT statement.
        db_type (str): Target DB type ("mysql", "sqlite", "postgresql").
        on_duplicate (Optional[str]): One of {"ignore", "replace", None}.

    Returns:
        str: Modified SQL with duplicate handling clause (if applied).

    Raises:
        UnsupportedDuplicateHandling: If behavior is unsupported for the DB.
        UnsupportedDatabaseType: If db_type is not recognized.
    """
    supported_db = {"mysql", "sqlite", "postgresql"}

    if db_type not in supported_db:
        raise UnsupportedDatabaseType(db_type)

    if on_duplicate is None:
        return sql

    if on_duplicate.lower() not in {"ignore", "replace"}:
        raise UnsupportedDuplicateHandling(
            f"Unknown on_duplicate value: {on_duplicate}"
        )

    insert_pattern = r"(?i)^\s*insert\s+into"

    if on_duplicate == "ignore":
        if db_type == "mysql":
            return re.sub(insert_pattern, "INSERT IGNORE INTO", sql, count=1)
        elif db_type == "sqlite":
            return re.sub(insert_pattern, "INSERT OR IGNORE INTO", sql, count=1)
        else:  # db_type == "postgresql":
            if "on conflict" not in sql.lower():
                return sql.rstrip().rstrip(";") + " ON CONFLICT DO NOTHING"
            return sql

    else:  # on_duplicate == "replace":
        if db_type == "mysql":
            return re.sub(insert_pattern, "REPLACE INTO", sql, count=1)
        elif db_type == "sqlite":
            return re.sub(insert_pattern, "INSERT OR REPLACE INTO", sql, count=1)
        else:  # db_type == "postgresql":
            raise UnsupportedDuplicateHandling(
                "PostgreSQL requires explicit conflict keys for 'replace' behavior. "
                "This is not currently supported "
                "Write your own query logic and select on_duplicate: None"
            )
