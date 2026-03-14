from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date, datetime
from typing import Any


@dataclass(slots=True)
class ShortInterestSnapshot:
    instrument_id: str
    company_id: str | None
    symbol: str
    settlement_date: date
    short_interest: float | None = None
    previous_short_interest: float | None = None
    avg_daily_volume: float | None = None
    days_to_cover: float | None = None
    source_name: str | None = "finra"
    created_at: datetime | None = None

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class DailyShortVolumeSnapshot:
    instrument_id: str
    company_id: str | None
    symbol: str
    trade_date: date
    short_volume: float | None = None
    total_volume: float | None = None
    short_volume_ratio: float | None = None
    source_name: str | None = "finra"
    created_at: datetime | None = None

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class ShortFeatureDaily:
    instrument_id: str
    company_id: str | None
    symbol: str
    as_of_date: date
    short_interest: float | None = None
    avg_daily_volume: float | None = None
    days_to_cover: float | None = None
    short_volume: float | None = None
    total_volume: float | None = None
    short_volume_ratio: float | None = None
    short_interest_change: float | None = None
    source_name: str | None = "finra"
    created_at: datetime | None = None

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)
