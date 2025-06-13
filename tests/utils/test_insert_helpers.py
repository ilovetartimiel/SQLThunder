### --- Third-party imports --- ###
import pytest

from SQLThunder.exceptions import UnsupportedDatabaseType, UnsupportedDuplicateHandling

### --- Internal package imports --- ###
from SQLThunder.utils.insert_helpers import _apply_on_duplicate_clause

### --- Test Apply Duplicate Logic --- ###


class TestApplyOnDuplicateClause:

    @pytest.mark.parametrize(
        "db_type,expected",
        [
            ("mysql", "INSERT IGNORE INTO users (id, name) VALUES (:id, :name)"),
            ("sqlite", "INSERT OR IGNORE INTO users (id, name) VALUES (:id, :name)"),
            (
                "postgresql",
                "INSERT INTO users (id, name) VALUES (:id, :name) ON CONFLICT DO NOTHING",
            ),
        ],
    )
    def test_ignore(self, db_type, expected):
        sql = "INSERT INTO users (id, name) VALUES (:id, :name)"
        res = _apply_on_duplicate_clause(sql, db_type, "ignore")
        assert res == expected

    @pytest.mark.parametrize(
        "db_type,expected",
        [
            ("mysql", "REPLACE INTO users (id, name) VALUES (:id, :name)"),
            ("sqlite", "INSERT OR REPLACE INTO users (id, name) VALUES (:id, :name)"),
        ],
    )
    def test_replace(self, db_type, expected):
        sql = "INSERT INTO users (id, name) VALUES (:id, :name)"
        res = _apply_on_duplicate_clause(sql, db_type, "replace")
        assert res == expected

    def test_postgresql_replace_raises(self):
        sql = "INSERT INTO users (id, name) VALUES (:id, :name)"
        with pytest.raises(
            UnsupportedDuplicateHandling,
            match="PostgreSQL requires explicit conflict keys",
        ):
            _apply_on_duplicate_clause(sql, "postgresql", "replace")

    def test_unknown_duplicate_mode(self):
        sql = "INSERT INTO users (id, name) VALUES (:id, :name)"
        with pytest.raises(
            UnsupportedDuplicateHandling, match="Unknown on_duplicate value"
        ):
            _apply_on_duplicate_clause(sql, "mysql", "merge")

    def test_unsupported_db_type_raises(self):
        sql = "INSERT INTO users (id, name) VALUES (:id, :name)"
        with pytest.raises(UnsupportedDatabaseType, match="Unsupported database type"):
            _apply_on_duplicate_clause(sql, "oracle", "ignore")

    def test_none_mode_returns_original_sql(self):
        sql = "INSERT INTO users (id, name) VALUES (:id, :name)"
        assert _apply_on_duplicate_clause(sql, "mysql", None) == sql

    def test_postgres_ignore_with_conflict_clause_keeps_original(self):
        sql = "INSERT INTO users (id, name) VALUES (:id, :name) ON CONFLICT DO NOTHING"
        assert _apply_on_duplicate_clause(sql, "postgresql", "ignore") == sql
