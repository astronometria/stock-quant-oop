from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any


@dataclass(slots=True)
class InstrumentMaster:
    instrument_id: str
    company_id: str
    symbol: str
    exchange: str | None
    security_type: str | None
    share_class: str | None = None
    is_active: bool = True
    created_at: datetime | None = None

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)
