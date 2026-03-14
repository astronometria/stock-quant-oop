from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime


@dataclass(slots=True)
class RawShortInterestRecord:
    symbol: str
    settlement_date: date
    short_interest: int | None
    previous_short_interest: int | None
    avg_daily_volume: float | None
    shares_float: int | None
    revision_flag: str | None
    source_market: str
    source_file: str
    source_date: date | None


@dataclass(slots=True)
class ShortInterestRecord:
    symbol: str
    settlement_date: date
    short_interest: int
    previous_short_interest: int
    avg_daily_volume: float
    days_to_cover: float | None
    shares_float: int | None
    short_interest_pct_float: float | None
    revision_flag: str | None
    source_market: str
    source_file: str
    ingested_at: datetime


@dataclass(slots=True)
class ShortInterestSourceFile:
    source_file: str
    source_market: str
    source_date: date | None
    row_count: int
    loaded_at: datetime
