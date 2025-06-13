### --- Third-party imports --- ###
import pytest

### --- Internal package imports --- ###
from SQLThunder.core.session import DBSession
from SQLThunder.exceptions.dbclient import DBClientSessionClosedError

### --- Test DBSession --- ###


class TestDBSession:

    @pytest.mark.parametrize(
        "db_client",
        [
            {"db": "sqlite"},
            {"db": "mysql"},
            {"db": "postgres"},
        ],
        indirect=True,
    )
    def test_basic_enter_exit(self, db_client):
        with DBSession(db_client, label="BasicSession") as client:
            assert client is db_client
            assert not client.is_closed  # Should be open during session

        assert not db_client.is_closed  # Session should not auto-close

    @pytest.mark.parametrize(
        "db_client",
        [
            {"db": "sqlite"},
            {"db": "mysql"},
            {"db": "postgres"},
        ],
        indirect=True,
    )
    def test_auto_close(self, db_client):
        with DBSession(db_client, label="AutoCloseSession", auto_close=True) as client:
            assert not client.is_closed
        assert db_client.is_closed  # Client should be closed after context

    @pytest.mark.parametrize(
        "db_client",
        [
            {"db": "sqlite"},
            {"db": "mysql"},
            {"db": "postgres"},
        ],
        indirect=True,
    )
    def test_auto_reopen(self, db_client):
        db_client.close()
        assert db_client.is_closed

        with DBSession(
            db_client, label="AutoReopenSession", auto_reopen=True
        ) as client:
            assert not client.is_closed
            assert client is db_client

        assert not db_client.is_closed  # Stays open after context

    @pytest.mark.parametrize(
        "db_client",
        [
            {"db": "sqlite"},
            {"db": "mysql"},
            {"db": "postgres"},
        ],
        indirect=True,
    )
    def test_closed_without_reopen_raises(self, db_client):
        db_client.close()
        assert db_client.is_closed

        with pytest.raises(DBClientSessionClosedError) as exc_info:
            with DBSession(db_client, label="NoReopenSession", auto_reopen=False):
                pass

        assert "NoReopenSession" in str(exc_info.value)
