from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

ENV_DIR = "PGSQLITE_FAILOVER_DIR"
DEFAULT_DIRNAME = ".pgsqlite-failover"


@dataclass
class FailoverConfig:
    """Storage layout and timing. Storage dir resolution order:
    explicit ``storage_dir`` > ``$PGSQLITE_FAILOVER_DIR`` > ``<project_root>/.pgsqlite-failover``.
    ``project_root`` defaults to ``Path.cwd()``.
    """

    project_root: Path | str | None = None
    storage_dir: Path | str | None = None
    backup_interval: float = 3600.0
    health_interval: float = 5.0
    paths: dict = field(init=False, default_factory=dict)

    def __post_init__(self) -> None:
        root = Path(self.project_root) if self.project_root else Path.cwd()
        if self.storage_dir:
            base = Path(self.storage_dir)
        elif os.environ.get(ENV_DIR):
            base = Path(os.environ[ENV_DIR])
        else:
            base = root / DEFAULT_DIRNAME
        self.base = base.resolve()
        self.base.mkdir(parents=True, exist_ok=True)
        (self.base / "logs").mkdir(exist_ok=True)

    @property
    def fallback_path(self) -> Path:
        return self.base / "fallback.db"

    @property
    def backup_path(self) -> Path:
        return self.base / "backup.db"

    @property
    def journal_path(self) -> Path:
        return self.base / "journal.db"
