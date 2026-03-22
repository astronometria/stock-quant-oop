from __future__ import annotations

from pathlib import Path
import os

from stock_quant.infrastructure.config.app_config import AppConfig


def resolve_project_root() -> Path:
    return Path(__file__).resolve().parents[3]


def build_app_config(db_path: str | None = None) -> AppConfig:
    project_root = resolve_project_root()
    resolved_db_path = Path(db_path).expanduser() if db_path else None
    return AppConfig.from_project_root(project_root=project_root, db_path=resolved_db_path)
