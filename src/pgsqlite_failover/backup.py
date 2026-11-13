from __future__ import annotations

import logging
import sqlite3
import threading
from pathlib import Path

log = logging.getLogger("pgsqlite_failover.backup")


def snapshot(src: Path, dst: Path) -> None:
    """Consistent snapshot using the SQLite online backup API."""
    tmp = dst.with_suffix(".tmp")
    s, d = sqlite3.connect(str(src)), sqlite3.connect(str(tmp))
    try:
        s.backup(d)
    finally:
        d.close()
        s.close()
    tmp.replace(dst)


class BackupScheduler:
    def __init__(self, src: Path, dst: Path, interval: float) -> None:
        self._src, self._dst, self._interval = src, dst, interval
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, name="pgsqlite-failover-backup", daemon=True)

    def _run(self) -> None:
        while not self._stop.wait(self._interval):
            try:
                snapshot(self._src, self._dst)
            except Exception:
                log.exception("backup failed")

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread.is_alive():
            self._thread.join(timeout=2)
