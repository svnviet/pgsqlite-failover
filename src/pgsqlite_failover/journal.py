from __future__ import annotations

import json
import sqlite3
import threading
from typing import Any, Iterator

_SCHEMA = """
CREATE TABLE IF NOT EXISTS journal (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    txn INTEGER NOT NULL,
    seq INTEGER NOT NULL,
    statement TEXT NOT NULL,
    params TEXT,
    replayed INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
)
"""


class Journal:
    """Durable, ordered log of write transactions committed to the fallback."""

    def __init__(self, path) -> None:
        self._lock = threading.Lock()
        self._db = sqlite3.connect(str(path), check_same_thread=False, isolation_level=None)
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.execute("PRAGMA synchronous=FULL")
        self._db.execute(_SCHEMA)

    def append_transaction(self, statements: list[tuple[str, Any]]) -> None:
        if not statements:
            return
        with self._lock:
            self._db.execute("BEGIN IMMEDIATE")
            try:
                (txn,) = self._db.execute("SELECT COALESCE(MAX(txn), 0) + 1 FROM journal").fetchone()
                for seq, (sql, params) in enumerate(statements):
                    self._db.execute(
                        "INSERT INTO journal (txn, seq, statement, params) VALUES (?,?,?,?)",
                        (txn, seq, sql, json.dumps(params, default=str)),
                    )
                self._db.execute("COMMIT")
            except BaseException:
                self._db.execute("ROLLBACK")
                raise

    def pending(self) -> Iterator[tuple[int, list[tuple[int, str, Any]]]]:
        """Yield (txn, [(row_id, sql, params), ...]) for unreplayed transactions, oldest first."""
        with self._lock:
            rows = self._db.execute(
                "SELECT txn, id, statement, params FROM journal WHERE replayed = 0 ORDER BY txn, seq"
            ).fetchall()
        current, batch = None, []
        for txn, rid, sql, params in rows:
            if current is not None and txn != current:
                yield current, batch
                batch = []
            current = txn
            batch.append((rid, sql, json.loads(params) if params else None))
        if batch:
            yield current, batch

    def pending_count(self) -> int:
        with self._lock:
            return self._db.execute("SELECT COUNT(DISTINCT txn) FROM journal WHERE replayed = 0").fetchone()[0]

    def mark_replayed(self, txn: int) -> None:
        with self._lock:
            self._db.execute("UPDATE journal SET replayed = 1 WHERE txn = ?", (txn,))

    def close(self) -> None:
        self._db.close()
