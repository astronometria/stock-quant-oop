from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date, datetime
from typing import Any


@dataclass(slots=True)
class CorporateAction:
    instrument_id: str
    action_type: str
    action_date: date
    effective_date: date | None = None
    value_numeric: float | None = None
    value_text: str | None = None
    source_name: str | None = None
    created_at: datetime | None = None

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class PriceQualityFlag:
    instrument_id: str
    price_date: date
    flag_type: str
    flag_value: str
    source_name: str | None = None
    created_at: datetime | None = None

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)
