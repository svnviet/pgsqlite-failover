from sqlalchemy import create_engine, text

from pgsqlite_failover import FailoverEngine

DDL = "CREATE TABLE IF NOT EXISTS events (name TEXT)"


def make(tmp_path):
    # A SQLite file stands in for PostgreSQL; the real-Postgres suite uses testcontainers.
    primary = create_engine(f"sqlite:///{tmp_path / 'primary.db'}")
    eng = FailoverEngine(primary_engine=primary, storage_dir=tmp_path / "store", monitor=False)
    with primary.begin() as c:
        c.execute(text(DDL))
    with eng.fallback.begin() as c:
        c.execute(text(DDL))
    return eng, primary


def names(engine):
    with engine.connect() as c:
        return [r[0] for r in c.execute(text("SELECT name FROM events ORDER BY rowid"))]


def test_default_layout(tmp_path):
    eng, _ = make(tmp_path)
    assert eng.config.fallback_path.parent.name == "store"
    assert eng.config.journal_path.exists()


def test_primary_when_healthy(tmp_path):
    eng, primary = make(tmp_path)
    with eng.begin() as c:
        c.execute(text("INSERT INTO events VALUES (:n)"), {"n": "a"})
    assert names(primary) == ["a"] and eng.journal.pending_count() == 0


def test_fallback_journal_and_replay(tmp_path):
    eng, primary = make(tmp_path)
    eng.health.mark_down()
    assert eng.active_backend == "fallback"
    for n in ("a", "b"):
        with eng.begin() as c:
            c.execute(text("INSERT INTO events VALUES (:n)"), {"n": n})
    assert names(eng.fallback) == ["a", "b"] and names(primary) == []
    assert eng.journal.pending_count() == 2
    eng.health.poll_once()
    assert names(primary) == ["a", "b"] and eng.journal.pending_count() == 0
    assert eng.active_backend == "primary"


def test_rolled_back_fallback_txn_not_journaled(tmp_path):
    eng, _ = make(tmp_path)
    eng.health.mark_down()
    try:
        with eng.begin() as c:
            c.execute(text("INSERT INTO events VALUES ('x')"))
            raise RuntimeError
    except RuntimeError:
        pass
    assert eng.journal.pending_count() == 0 and names(eng.fallback) == []
