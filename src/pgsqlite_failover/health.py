from __future__ import annotations

import logging
import threading
from typing import Callable

from sqlalchemy import text
from sqlalchemy.engine import Engine

log = logging.getLogger("pgsqlite_failover.health")


class HealthMonitor:
    """Background thread polling the primary; fires callbacks on state change."""

    def __init__(self, primary: Engine, interval: float, on_up: Callable[[], None]) -> None:
        self._primary = primary
        self._interval = interval
        self._on_up = on_up
        self._stop = threading.Event()
        self._healthy = True
        self._lock = threading.Lock()
        self._thread = threading.Thread(target=self._run, name="pgsqlite-failover-health", daemon=True)

    @property
    def healthy(self) -> bool:
        return self._healthy

    def mark_down(self) -> None:
        with self._lock:
            if self._healthy:
                log.warning("primary marked down; routing to SQLite fallback")
            self._healthy = False

    def check(self) -> bool:
        try:
            with self._primary.connect() as c:
                c.execute(text("SELECT 1"))
            return True
        except Exception:
            return False

    def poll_once(self) -> None:
        ok = self.check()
        with self._lock:
            was = self._healthy
        if ok and not was:
            log.info("primary recovered; replaying journal")
            if self._on_up():
                with self._lock:
                    self._healthy = True
        elif not ok:
            self.mark_down()

    def _run(self) -> None:
        while not self._stop.wait(self._interval):
            try:
                self.poll_once()
            except Exception:
                log.exception("health poll failed")

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread.is_alive():
            self._thread.join(timeout=2)
