from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime


@dataclass(slots=True)
class RawPriceBar:
    symbol: str
    price_date: date
    open: float | None
    high: float | None
    low: float | None
    close: float | None
    volume: int | None
    source_name: str


@dataclass(slots=True)
class PriceBar:
    symbol: str
    price_date: date
    open: float
    high: float
    low: float
    close: float
    volume: int
    source_name: str
    ingested_at: datetime
