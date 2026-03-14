from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date, datetime
from typing import Any


@dataclass(slots=True)
class TechnicalFeatureDaily:
    instrument_id: str
    company_id: str | None
    symbol: str
    as_of_date: date
    close_to_sma_20: float | None = None
    rsi_14: float | None = None
    source_name: str | None = "prices"
    created_at: datetime | None = None

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class ResearchFeatureDaily:
    instrument_id: str
    company_id: str | None
    symbol: str
    as_of_date: date
    close_to_sma_20: float | None = None
    rsi_14: float | None = None
    revenue: float | None = None
    net_income: float | None = None
    net_margin: float | None = None
    debt_to_equity: float | None = None
    return_on_assets: float | None = None
    short_interest: float | None = None
    days_to_cover: float | None = None
    short_volume_ratio: float | None = None
    article_count_1d: int | None = None
    unique_cluster_count_1d: int | None = None
    avg_link_confidence: float | None = None
    source_name: str | None = "research"
    created_at: datetime | None = None

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)
