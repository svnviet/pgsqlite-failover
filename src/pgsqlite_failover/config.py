from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

ENV_DIR = "PGSQLITE_FAILOVER_DIR"
DEFAULT_DIRNAME = ".pgsqlite-failover"


@dataclass
class FailoverConfig:
    """Storage layout and timing.

    Storage dir resolution order: explicit ``storage_dir`` > ``$PGSQLITE_FAILOVER_DIR`` >
    ``<project_root>/.pgsqlite-failover``. ``project_root`` defaults to ``Path.cwd()``.
    ``fallback_path`` / ``backup_path`` / ``journal_path`` override the individual files
    (relative paths resolve against ``project_root``); parent folders are created.
    """

    project_root: Path | str | None = None
    storage_dir: Path | str | None = None
    fallback_path: Path | str | None = None
    backup_path: Path | str | None = None
    journal_path: Path | str | None = None
    backup_interval: float = 3600.0
    health_interval: float = 5.0

    def __post_init__(self) -> None:
        root = Path(self.project_root) if self.project_root else Path.cwd()
        if self.storage_dir:
            base = Path(self.storage_dir)
        elif os.environ.get(ENV_DIR):
            base = Path(os.environ[ENV_DIR])
        else:
            base = root / DEFAULT_DIRNAME
        self.base = base.resolve()
        (self.base / "logs").mkdir(parents=True, exist_ok=True)
        for attr, default in (
            ("fallback_path", "fallback.db"),
            ("backup_path", "backup.db"),
            ("journal_path", "journal.db"),
        ):
            value = getattr(self, attr)
            path = Path(value) if value else self.base / default
            if not path.is_absolute():
                path = (root / path) if value else path
            path = path.resolve()
            path.parent.mkdir(parents=True, exist_ok=True)
            setattr(self, attr, path)
