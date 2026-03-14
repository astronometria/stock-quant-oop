from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date, datetime
from typing import Any


@dataclass(slots=True)
class ReturnLabelDaily:
    instrument_id: str
    company_id: str | None
    symbol: str
    as_of_date: date
    fwd_return_1d: float | None = None
    fwd_return_5d: float | None = None
    fwd_return_20d: float | None = None
    direction_1d: int | None = None
    direction_5d: int | None = None
    direction_20d: int | None = None
    source_name: str | None = "labels"
    created_at: datetime | None = None

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class VolatilityLabelDaily:
    instrument_id: str
    company_id: str | None
    symbol: str
    as_of_date: date
    realized_vol_20d: float | None = None
    source_name: str | None = "labels"
    created_at: datetime | None = None

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)
