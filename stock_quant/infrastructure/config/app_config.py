from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from stock_quant.shared.constants import DEFAULT_DATA_DIRNAME, DEFAULT_DB_FILENAME, DEFAULT_LOGS_DIRNAME


@dataclass(slots=True)
class AppConfig:
    project_root: Path
    db_path: Path
    logs_dir: Path
    data_dir: Path

    @classmethod
    def from_project_root(cls, project_root: Path, db_path: Path | None = None) -> "AppConfig":
        root = project_root.expanduser().resolve()
        resolved_db_path = (db_path or (root / DEFAULT_DB_FILENAME)).expanduser().resolve()
        return cls(
            project_root=root,
            db_path=resolved_db_path,
            logs_dir=(root / DEFAULT_LOGS_DIRNAME).resolve(),
            data_dir=(root / DEFAULT_DATA_DIRNAME).resolve(),
        )

    def ensure_directories(self) -> None:
        self.project_root.mkdir(parents=True, exist_ok=True)
        self.logs_dir.mkdir(parents=True, exist_ok=True)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
