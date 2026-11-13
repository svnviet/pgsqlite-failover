"""Fixtures for tests against a real PostgreSQL.

PostgreSQL comes from testcontainers (needs Docker). Set PGSQLITE_TEST_POSTGRES_URL
(e.g. postgresql+psycopg://user:pw@host:5432/db) to use an existing server instead.
Outages are simulated with a small TCP proxy so they behave the same either way.
"""
from __future__ import annotations

import os
import socket
import threading

import pytest
from sqlalchemy.engine import make_url


class TcpProxy:
    def __init__(self, host: str, port: int) -> None:
        self.target = (host, port)
        self.port = self._free_port()
        self._conns: list[socket.socket] = []
        self._lock = threading.Lock()
        self._srv: socket.socket | None = None

    @staticmethod
    def _free_port() -> int:
        with socket.socket() as s:
            s.bind(("127.0.0.1", 0))
            return s.getsockname()[1]

    def up(self) -> None:
        srv = socket.socket()
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind(("127.0.0.1", self.port))
        srv.listen(50)
        self._srv = srv
        threading.Thread(target=self._accept, args=(srv,), daemon=True).start()

    def down(self) -> None:
        if self._srv:
            try:  # wakes the thread blocked in accept(); close() alone leaves it listening
                self._srv.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            self._srv.close()
            self._srv = None
        with self._lock:
            conns, self._conns = self._conns, []
        for c in conns:
            for fn in (lambda: c.shutdown(socket.SHUT_RDWR), c.close):
                try:
                    fn()
                except OSError:
                    pass

    def _accept(self, srv) -> None:
        while True:
            try:
                client, _ = srv.accept()
                upstream = socket.create_connection(self.target, timeout=5)
                upstream.settimeout(None)
            except OSError:
                return
            with self._lock:
                self._conns += [client, upstream]
            for a, b in ((client, upstream), (upstream, client)):
                threading.Thread(target=self._pipe, args=(a, b), daemon=True).start()

    @staticmethod
    def _pipe(a, b) -> None:
        try:
            while data := a.recv(65536):
                b.sendall(data)
        except OSError:
            pass
        finally:
            for s in (a, b):
                try:
                    s.close()
                except OSError:
                    pass


@pytest.fixture(scope="session")
def pg_url():
    url = os.environ.get("PGSQLITE_TEST_POSTGRES_URL")
    if url:
        yield make_url(url)
        return
    try:
        from testcontainers.postgres import PostgresContainer

        container = PostgresContainer("postgres:16-alpine", driver="psycopg")
        container.start()
    except Exception as exc:  # Docker missing/unreachable
        pytest.skip(f"PostgreSQL unavailable (no Docker, no PGSQLITE_TEST_POSTGRES_URL): {exc}")
    try:
        yield make_url(container.get_connection_url())
    finally:
        container.stop()


@pytest.fixture
def proxy(pg_url):
    p = TcpProxy(pg_url.host, pg_url.port)
    p.up()
    yield p
    p.down()


@pytest.fixture
def proxied_url(pg_url, proxy):
    return pg_url.set(host="127.0.0.1", port=proxy.port).render_as_string(hide_password=False)
