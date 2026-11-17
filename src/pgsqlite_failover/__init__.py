"""PostgreSQL-primary / SQLite-fallback failover for SQLAlchemy."""
from .config import FailoverConfig
from .engine import FailoverEngine, create_failover_engine

__all__ = ["FailoverConfig", "FailoverEngine", "create_failover_engine"]
__version__ = "0.2.1"
