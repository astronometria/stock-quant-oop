from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Any


@dataclass(slots=True)
class RunMetadata:
    run_type: str
    run_name: str
    status: str
    started_at: datetime
    finished_at: datetime | None = None
    config_json: str = "{}"
    metrics_json: str = "{}"
    error_message: str | None = None
    artifact_path: str | None = None
    created_at: datetime = field(default_factory=datetime.utcnow)

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)
