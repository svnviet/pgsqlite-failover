# pgsqlite-failover

SQLAlchemy middleware: PostgreSQL is the primary database; when it is unreachable, work continues on a local
SQLite fallback. Writes made during the outage are journaled and replayed to PostgreSQL, in order, once it is back.

> **Status: alpha (0.2.x).** Read [Limitations](#limitations) before using it in production.

## Install

```bash
pip install "pgsqlite-failover[postgres]"   # [postgres] adds psycopg 3; use your own driver if you prefer
```
Requires Python 3.10+ and SQLAlchemy 2.x.

## Quick start

```python
from sqlalchemy import text
from sqlalchemy.orm import Session, sessionmaker
from pgsqlite_failover import create_failover_engine

engine = create_failover_engine(
    "postgresql+psycopg://user:pass@localhost/app",
    project_root=".",                    # default: current working directory
    # fallback_path="data/fallback.db",  # optional: choose the SQLite file
)

# Core
with engine.begin() as conn:
    conn.execute(text("INSERT INTO events (name) VALUES (:name)"), {"name": "Tin360"})

# ORM
SessionLocal = sessionmaker(bind=engine)
with SessionLocal() as session:
    session.add(Event(name="Tin360"))
    session.commit()
```

`engine.active_backend` is `"primary"` or `"fallback"`. Call `engine.dispose()` on shutdown.

## Setup guide

1. **Migrate PostgreSQL yourself** (Alembic or otherwise) against the *real* PostgreSQL URL. Never point Alembic at the
   failover engine: while degraded it would run DDL on SQLite and the two schemas would diverge.
2. **Create the same tables in the fallback.** The library does not copy your schema.
   ```python
   Base.metadata.create_all(engine.fallback)   # creates missing tables only
   ```
   After a migration that changes existing tables, recreate `fallback.db` or run the same migration against it.
3. **Add the storage folder to `.gitignore`:** `.pgsqlite-failover/`.
4. Make writes **idempotent** where possible (replay is at-least-once).

### Storage

| File | Purpose | Override |
| --- | --- | --- |
| `fallback.db` | Operational SQLite database used while degraded | `fallback_path=` |
| `journal.db` | Durable log of writes made on the fallback | `journal_path=` |
| `backup.db` | Periodic snapshot of `fallback.db` (default hourly) | `backup_path=` |

Default folder: `<project_root>/.pgsqlite-failover/`. Change it with `storage_dir=` or the `PGSQLITE_FAILOVER_DIR`
environment variable. Relative file paths resolve against `project_root`; missing folders are created.

### Options

`create_failover_engine(postgres_url, *, project_root, storage_dir, fallback_path, backup_path, journal_path,
backup_interval=3600, health_interval=5, monitor=True)`

## How it works

- `engine.connect()` / `engine.begin()` return real SQLAlchemy connections from the active backend, so Core and
  `Session` work unchanged. Routing is per transaction; a transaction is never split across backends.
- A background thread polls PostgreSQL every `health_interval` seconds. A refused or dropped connection also switches
  to the fallback immediately.
- Writes committed on the fallback are stored per transaction in `journal.db`, compiled for PostgreSQL's dialect.
  When PostgreSQL recovers they are replayed in order; if a replay fails it stops, stays on the fallback and retries
  on the next health check.

## Limitations

- **Empty fallback.** SQLite starts empty. During an outage reads only see data written on the fallback, not your
  PostgreSQL data. Loading PostgreSQL data into SQLite is not implemented (planned for 0.4).
- **Schema is your job.** No automatic creation, migration or sync of the fallback schema.
- **Portable SQL only.** Models and queries must run on both PostgreSQL and SQLite. `JSONB`, `ARRAY`, PostgreSQL
  functions and extensions will fail on the fallback.
- **No DDL journaling.** Schema changes are not replayed; don't run migrations through the failover engine.
- **At-least-once replay.** A crash during replay can re-run a transaction. Replay stops at the first failure
  (for example a constraint PostgreSQL enforces but SQLite does not), so later writes wait behind it.
- **Generated values diverge.** Autoincrement ids, sequences and server defaults are assigned independently on each
  backend. Do not rely on identical ids across the two. Values are replayed as plain Python values; rich types may
  be stringified in the journal.
- **Raw SQL.** Writes via `exec_driver_sql()` on the fallback are not journaled (a warning is logged).
  `text()` and ORM/Core statements are.
- **In-flight transactions.** A transaction open on PostgreSQL when it fails is lost and raises; retry it and it will
  run on the fallback. Writes arriving while a replay is running can wait until the next health check.
- **Single process.** No coordination between processes sharing the same storage folder (planned for 0.5).
- **Sync only.** No `AsyncEngine` / `AsyncSession` support.
- **Not a proxy.** Applications must use this engine; there is no PostgreSQL wire-protocol endpoint.

## Roadmap

| Version | Scope |
| --- | --- |
| 0.2.x | Core + ORM routing, journal and replay (current) |
| 0.3 | Hardening of transaction-level journal and replay |
| 0.4 | Automatic snapshot and bootstrap of the fallback from PostgreSQL |
| 0.5 | Multi-process coordination |
| 1.0 | Production release after fault-injection validation |

## Testing

```bash
pip install -e ".[postgres,test]"
pytest            # integration tests start PostgreSQL with testcontainers (needs Docker)
```
Without Docker, point the suite at an existing server:
`PGSQLITE_TEST_POSTGRES_URL=postgresql+psycopg://user:pw@host:5432/db pytest` (the tests create and drop an `events`
table). Outages are simulated with a local TCP proxy.

## License

MIT
