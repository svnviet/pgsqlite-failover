from __future__ import annotations

import logging
import re
import threading
from contextlib import contextmanager
from typing import Any, Iterator

from sqlalchemy import create_engine, event
from sqlalchemy.engine import Connection, Engine

from .backup import BackupScheduler
from .config import FailoverConfig
from .health import HealthMonitor
from .journal import Journal

log = logging.getLogger("pgsqlite_failover")

_WRITE = re.compile(r"^\s*(insert|update|delete|replace|merge|truncate)\b", re.I)
_CTE_WRITE = re.compile(r"^\s*with\b.*\b(insert|update|delete)\b", re.I | re.S)
_BUF = "_pgsqlite_failover_buf"
_SAVEPOINTS = "_pgsqlite_failover_sp"


def _is_write(sql: str) -> bool:
    return bool(_WRITE.match(sql) or _CTE_WRITE.match(sql))


class FailoverEngine:
    """Engine-compatible router: PostgreSQL primary, SQLite fallback.

    ``connect()`` returns a real SQLAlchemy ``Connection`` from whichever backend is
    active, so it works with Core and with ``Session(bind=engine)`` / ``sessionmaker``.
    Writes committed on the fallback are journaled per transaction (compiled for the
    primary's dialect) and replayed in order once the primary recovers.
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
        self._install_primary_events()
        self._install_fallback_events()
        if monitor:
            self.health.start()
            self._backup.start()

    # -- engine-like surface ------------------------------------------------
    @property
    def active_backend(self) -> str:
        return "primary" if self.health.healthy else "fallback"

    @property
    def _active(self) -> Engine:
        return self.primary if self.health.healthy else self.fallback

    def connect(self) -> Connection:
        if self.health.healthy:
            try:
                return self.primary.connect()
            except Exception:
                # handle_error already flagged a connection failure; anything else is real.
                if self.health.healthy:
                    raise
        return self.fallback.connect()

    @contextmanager
    def begin(self) -> Iterator[Connection]:
        with self.connect() as conn:
            with conn.begin():
                yield conn

    def __getattr__(self, name: str) -> Any:  # dialect, url, pool, ... of the active engine
        if name.startswith("__") or name in {"primary", "fallback", "health"}:
            raise AttributeError(name)
        return getattr(self._active, name)

    # -- failure detection --------------------------------------------------
    def _install_primary_events(self) -> None:
        @event.listens_for(self.primary, "handle_error")
        def _on_error(ctx):  # connection refused (no connection yet) or dropped mid-flight
            if ctx.is_disconnect or ctx.connection is None:
                self.health.mark_down()

    # -- journaling on the fallback ----------------------------------------
    def _install_fallback_events(self) -> None:
        fb, primary = self.fallback, self.primary

        @event.listens_for(fb, "before_cursor_execute")
        def _record(conn, cursor, statement, parameters, context, executemany):
            compiled = getattr(context, "compiled", None)
            if compiled is None:
                if _is_write(statement):
                    log.warning("write via exec_driver_sql is not journaled: %.80s", statement)
                return
            if compiled.is_ddl or getattr(context, "_pgf_recorded", False):
                return  # one logical execution may issue several cursor calls (insertmanyvalues)
            context._pgf_recorded = True
            pg = compiled.statement.compile(
                dialect=primary.dialect, column_keys=compiled.column_keys
            )
            if not _is_write(pg.string):
                return
            rows = []
            for cp in context.compiled_parameters:
                p = pg.construct_params(cp)
                rows.append([p.get(k) for k in pg.positiontup] if pg.positiontup else p)
            conn.info.setdefault(_BUF, []).append((pg.string, rows))

        @event.listens_for(fb, "commit")
        def _commit(conn):
            buf = conn.info.pop(_BUF, None)
            conn.info.pop(_SAVEPOINTS, None)
            if buf:
                self.journal.append_transaction(buf)  # before the SQLite commit

        @event.listens_for(fb, "rollback")
        def _rollback(conn):
            conn.info.pop(_BUF, None)
            conn.info.pop(_SAVEPOINTS, None)

        @event.listens_for(fb, "savepoint")
        def _sp(conn, name):
            conn.info.setdefault(_SAVEPOINTS, {})[name] = len(conn.info.get(_BUF, []))

        @event.listens_for(fb, "rollback_savepoint")
        def _sp_rollback(conn, name, context):
            mark = conn.info.get(_SAVEPOINTS, {}).get(name)
            if mark is not None and _BUF in conn.info:
                del conn.info[_BUF][mark:]

        @event.listens_for(fb, "checkout")
        def _checkout(dbapi_conn, record, proxy):
            record.info.pop(_BUF, None)
            record.info.pop(_SAVEPOINTS, None)

    # -- recovery -----------------------------------------------------------
    def replay(self) -> bool:
        """Replay journaled transactions to the primary. True when fully drained."""
        with self._replay_lock:
            for txn, batch in self.journal.pending():
                try:
                    with self.primary.begin() as conn:
                        for _, sql, rows in batch:
                            for row in rows:  # row by row: RETURNING forbids executemany on some drivers
                                conn.exec_driver_sql(sql, tuple(row) if isinstance(row, list) else row)
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
