from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date, datetime
from typing import Any


@dataclass(slots=True)
class ResearchDatasetDaily:
    dataset_name: str
    dataset_version: str
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
    fwd_return_1d: float | None = None
    fwd_return_5d: float | None = None
    fwd_return_20d: float | None = None
    direction_1d: int | None = None
    direction_5d: int | None = None
    direction_20d: int | None = None
    realized_vol_20d: float | None = None
    created_at: datetime | None = None

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)
