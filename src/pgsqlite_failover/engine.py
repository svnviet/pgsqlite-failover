from __future__ import annotations

import logging
import threading
from contextlib import contextmanager
from typing import Any, Iterator

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection, Engine
from sqlalchemy.exc import DBAPIError, OperationalError

from .backup import BackupScheduler
from .config import FailoverConfig
from .health import HealthMonitor
from .journal import Journal

log = logging.getLogger("pgsqlite_failover")

_READ_PREFIXES = ("select", "with", "pragma", "explain", "show")


def _is_write(sql: str) -> bool:
    return not sql.lstrip().lower().startswith(_READ_PREFIXES)


def _is_connection_error(exc: BaseException) -> bool:
    return isinstance(exc, OperationalError) or (
        isinstance(exc, DBAPIError) and exc.connection_invalidated
    )


class _JournalingConnection:
    """Wraps a fallback Connection and records write statements for later replay."""

    def __init__(self, conn: Connection) -> None:
        self._conn = conn
        self.recorded: list[tuple[str, Any]] = []

    def execute(self, statement, parameters=None, *args, **kwargs):
        sql = str(statement) if not isinstance(statement, str) else statement
        if _is_write(sql):
            self.recorded.append((sql, parameters))
        return self._conn.execute(statement, parameters, *args, **kwargs)

    def __getattr__(self, name):
        return getattr(self._conn, name)


class FailoverEngine:
    """Routes SQLAlchemy Core transactions to PostgreSQL, or to SQLite when it is down.

    Writes made on the fallback are journaled per transaction and replayed to the
    primary, in order, once it recovers. The routing decision is made per
    transaction; a transaction is never split across backends.
    """

    def __init__(
        self,
        postgres_url: str | None = None,
        *,
        config: FailoverConfig | None = None,
        primary_engine: Engine | None = None,
        monitor: bool = True,
        **config_kwargs: Any,
    ) -> None:
        self.config = config or FailoverConfig(**config_kwargs)
        self.primary = primary_engine or create_engine(postgres_url, pool_pre_ping=True)
        self.fallback = create_engine(f"sqlite:///{self.config.fallback_path}")
        self.journal = Journal(self.config.journal_path)
        self._replay_lock = threading.Lock()
        self.health = HealthMonitor(self.primary, self.config.health_interval, self.replay)
        self._backup = BackupScheduler(
            self.config.fallback_path, self.config.backup_path, self.config.backup_interval
        )
        if monitor:
            self.health.start()
            self._backup.start()

    @property
    def active_backend(self) -> str:
        return "primary" if self.health.healthy else "fallback"

    # -- transactions -------------------------------------------------------
    @contextmanager
    def begin(self) -> Iterator[Any]:
        if self.health.healthy:
            try:
                cm = self.primary.begin()
                conn = cm.__enter__()
            except Exception as exc:
                if not _is_connection_error(exc):
                    raise
                self.health.mark_down()
            else:
                try:
                    yield conn
                except BaseException as exc:
                    cm.__exit__(type(exc), exc, exc.__traceback__)
                    if _is_connection_error(exc):
                        self.health.mark_down()
                    raise
                else:
                    cm.__exit__(None, None, None)
                return
        with self._fallback_begin() as conn:
            yield conn

    @contextmanager
    def _fallback_begin(self) -> Iterator[_JournalingConnection]:
        with self.fallback.begin() as raw:
            wrapped = _JournalingConnection(raw)
            yield wrapped
            # Reached only if the body did not raise; journal before the SQLite commit.
            self.journal.append_transaction(wrapped.recorded)

    @contextmanager
    def connect(self) -> Iterator[Any]:
        """Autocommit-less connection. Prefer ``begin()`` for writes while degraded."""
        engine = self.primary if self.health.healthy else self.fallback
        with engine.connect() as conn:
            yield conn

    # -- recovery -----------------------------------------------------------
    def replay(self) -> bool:
        """Replay journaled transactions to the primary. Returns True when fully drained."""
        with self._replay_lock:
            for txn, batch in self.journal.pending():
                try:
                    with self.primary.begin() as conn:
                        for _, sql, params in batch:
                            conn.execute(text(sql), params or {})
                except Exception:
                    log.exception("replay of txn %s failed; stopping", txn)
                    return False
                self.journal.mark_replayed(txn)
            return True

    def dispose(self) -> None:
        self.health.stop()
        self._backup.stop()
        self.journal.close()
        self.primary.dispose()
        self.fallback.dispose()


def create_failover_engine(postgres_url: str, **kwargs: Any) -> FailoverEngine:
    return FailoverEngine(postgres_url, **kwargs)
