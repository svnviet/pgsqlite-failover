"""Failover behaviour against a real PostgreSQL server."""
import time

import pytest
from sqlalchemy import create_engine, text

from pgsqlite_failover import FailoverEngine

PG_DDL = "CREATE TABLE events (id SERIAL PRIMARY KEY, name TEXT NOT NULL UNIQUE)"  # UNIQUE only on PG
LITE_DDL = "CREATE TABLE IF NOT EXISTS events (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT NOT NULL)"
INSERT = "INSERT INTO events (name) VALUES (:n)"


@pytest.fixture
def setup(pg_url, proxied_url, tmp_path):
    admin = create_engine(pg_url.render_as_string(hide_password=False))
    with admin.begin() as c:
        c.execute(text("DROP TABLE IF EXISTS events"))
        c.execute(text(PG_DDL))
    primary = create_engine(
        proxied_url, pool_pre_ping=True, connect_args={"connect_timeout": 2}
    )
    eng = FailoverEngine(primary_engine=primary, storage_dir=tmp_path / "store", monitor=False)
    with eng.fallback.begin() as c:
        c.execute(text(LITE_DDL))
    yield eng, admin
    eng.dispose()
    with admin.begin() as c:
        c.execute(text("DROP TABLE IF EXISTS events"))
    admin.dispose()


def pg_names(admin):
    with admin.connect() as c:
        return [r[0] for r in c.execute(text("SELECT name FROM events ORDER BY id"))]


def insert(eng, name):
    with eng.begin() as c:
        c.execute(text(INSERT), {"n": name})


def test_writes_go_to_postgres_when_healthy(setup):
    eng, admin = setup
    insert(eng, "a")
    assert pg_names(admin) == ["a"]
    assert eng.active_backend == "primary" and eng.journal.pending_count() == 0


def test_outage_routes_to_fallback_then_replays_in_order(setup, proxy):
    eng, admin = setup
    insert(eng, "before")
    proxy.down()

    # First write after the outage may hit the dead connection; it must not be lost.
    insert(eng, "during-1")
    insert(eng, "during-2")
    assert eng.active_backend == "fallback"
    assert eng.journal.pending_count() >= 1

    proxy.up()
    eng.health.poll_once()

    assert eng.active_backend == "primary"
    assert eng.journal.pending_count() == 0
    assert pg_names(admin) == ["before", "during-1", "during-2"]


def test_health_poll_detects_outage_and_stays_down(setup, proxy):
    eng, _ = setup
    proxy.down()
    eng.health.poll_once()
    assert eng.active_backend == "fallback"
    eng.health.poll_once()  # still down: must not flip back
    assert eng.active_backend == "fallback"


def test_background_monitor_recovers_and_replays(pg_url, proxied_url, proxy, tmp_path):
    admin = create_engine(pg_url.render_as_string(hide_password=False))
    with admin.begin() as c:
        c.execute(text("DROP TABLE IF EXISTS events"))
        c.execute(text(PG_DDL))
    primary = create_engine(proxied_url, pool_pre_ping=True, connect_args={"connect_timeout": 2})
    eng = FailoverEngine(
        primary_engine=primary, storage_dir=tmp_path / "s", monitor=True, health_interval=0.2
    )
    try:
        with eng.fallback.begin() as c:
            c.execute(text(LITE_DDL))
        proxy.down()
        deadline = time.time() + 10
        while eng.active_backend != "fallback" and time.time() < deadline:
            time.sleep(0.1)
        assert eng.active_backend == "fallback"
        insert(eng, "offline")
        proxy.up()
        deadline = time.time() + 15
        while eng.active_backend != "primary" and time.time() < deadline:
            time.sleep(0.1)
        assert eng.active_backend == "primary"
        assert pg_names(admin) == ["offline"]
    finally:
        eng.dispose()
        with admin.begin() as c:
            c.execute(text("DROP TABLE IF EXISTS events"))
        admin.dispose()


def test_failed_replay_keeps_journal_and_stays_on_fallback(setup, proxy):
    eng, admin = setup
    proxy.down()
    eng.health.poll_once()
    insert(eng, "same")
    insert(eng, "same")  # SQLite accepts it; PostgreSQL's UNIQUE(name) rejects it on replay
    proxy.up()
    eng.health.poll_once()
    assert pg_names(admin) == ["same"]
    assert eng.journal.pending_count() == 1
    assert eng.active_backend == "fallback"


def test_orm_session_failover_and_replay(setup, proxy):
    """Regression: Session(bind=engine) must work (engine.connect() returns a real Connection)."""
    from sqlalchemy import select
    from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column

    class Base(DeclarativeBase):
        pass

    class Event(Base):
        __tablename__ = "events"
        id: Mapped[int] = mapped_column(primary_key=True)
        name: Mapped[str] = mapped_column()

    eng, admin = setup
    with Session(eng) as s:
        s.add(Event(name="p"))
        s.commit()
    proxy.down()
    eng.health.poll_once()
    with Session(eng) as s:
        s.add_all([Event(name="f1"), Event(name="f2")])
        s.commit()
    proxy.up()
    eng.health.poll_once()
    with Session(eng) as s:
        assert sorted(s.scalars(select(Event.name))) == ["f1", "f2", "p"]
    assert eng.active_backend == "primary"
