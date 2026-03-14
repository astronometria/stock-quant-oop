from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Any


@dataclass(slots=True)
class DatasetVersion:
    dataset_name: str
    dataset_version: str
    universe_name: str
    as_of_date: str
    feature_run_id: int | None = None
    label_run_id: int | None = None
    row_count: int = 0
    config_json: str = "{}"
    created_at: datetime = field(default_factory=datetime.utcnow)

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)
