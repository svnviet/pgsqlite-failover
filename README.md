# pgsqlite-failover

SQLAlchemy middleware: PostgreSQL primary, durable SQLite fallback, transaction journal and replay.

```python
from sqlalchemy import text
from pgsqlite_failover import create_failover_engine

engine = create_failover_engine("postgresql+psycopg://user:pass@localhost/app", project_root=".")
with engine.begin() as conn:
    conn.execute(text("INSERT INTO events (name) VALUES (:name)"), {"name": "Tin360"})
```

Storage defaults to `<project_root>/.pgsqlite-failover/` (`fallback.db`, `backup.db`, `journal.db`, `logs/`);
override with `storage_dir=` or `PGSQLITE_FAILOVER_DIR`. Add the directory to `.gitignore`.

## Status (0.2.0)
SQLAlchemy Core only. Routing is per transaction; writes on the fallback are journaled and replayed in order
when the primary recovers. Known limits: the fallback schema must be provisioned separately, SQL must be
portable between PostgreSQL and SQLite, replay is at-least-once (make writes idempotent), ORM Session,
multi-process coordination and PG→SQLite bootstrap are on the roadmap (0.3–0.5).
