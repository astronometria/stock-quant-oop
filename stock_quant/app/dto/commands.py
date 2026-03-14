from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(slots=True)
class BaseCommand:
    db_path: Path
    verbose: bool = False


@dataclass(slots=True)
class InitDbCommand(BaseCommand):
    drop_existing: bool = False
