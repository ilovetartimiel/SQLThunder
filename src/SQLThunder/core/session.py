### --- Standard library imports --- ###
import time
from types import TracebackType
from typing import Optional, Type

### --- Internal package imports --- ###
from SQLThunder.exceptions.dbclient import DBClientSessionClosedError
from SQLThunder.logging_config import logger

from .client import DBClient

### --- Session class wrapper for DBClient --- ###


class DBSession:
    """
    Context-managed wrapper around a DBClient for safe and clean scoping of operations.

    Supports optional auto-reopening of a closed client upon entry and auto-closing on exit.
    Useful for profiling, logical grouping of operations, and simplifying usage in notebooks
    and long-running services.
    """

    def __init__(
        self,
        client: DBClient,
        label: Optional[str] = None,
        auto_close: bool = False,
        auto_reopen: bool = False,
    ) -> None:
        """
        Initializes a DBSession context.

        Args:
            client (DBClient): A reusable DBClient instance to wrap.
            label (Optional[str]): Optional label to use in logs. Defaults to "UnnamedSession".
            auto_close (bool): Whether to call `client.close()` on exit. Defaults to False.
            auto_reopen (bool): Whether to call `client.reopen_connection()` on entry if closed. Defaults to False.
        """
        self._client = client
        self._label = label or "UnnamedSession"
        self._auto_close = auto_close
        self._auto_reopen = auto_reopen
        self._start_time: Optional[float] = None

    def __enter__(self) -> DBClient:
        """
        Enter the session context. Optionally reopens the DBClient if `auto_reopen=True` if it was previously closed.

        Returns:
            DBClient: The underlying DBClient instance ready for use.

        Raises:
            DBClientSessionClosedError: If the DBClient is closed and `auto_reopen` is False.
        """
        if self._client.is_closed:
            if self._auto_reopen:
                logger.warning(
                    f"[{self._label}] DBClient was closed. Reopening before session."
                )
                self._client.reopen_connection()
            else:
                raise DBClientSessionClosedError(self._label)

        self._start_time = time.perf_counter()
        logger.debug(f"[{self._label}] Entering DB session")
        return self._client

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        """
        Exit the session context. Optionally closes the DBClient if `auto_close=True`, logs timing and exceptions

        Args:
            exc_type (Optional[Type[BaseException]]): Exception type, if one occurred.
            exc_val (Optional[BaseException]): Exception instance, if one occurred.
            exc_tb (Optional[TracebackType]): Exception traceback object, if one occurred.
        """
        if self._start_time is not None:
            elapsed = time.perf_counter() - self._start_time
        else:
            elapsed = 0.0

        if exc_type:
            logger.warning(f"[{self._label}] Exception during session: {exc_val}")

        if self._auto_close:
            logger.info(f"[{self._label}] Auto-closing DBClient after session.")
            self._client.close()

        logger.debug(f"[{self._label}] Exiting DB session (elapsed: {elapsed:.2f}s)")
