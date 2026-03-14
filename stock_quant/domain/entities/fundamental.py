from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date, datetime
from typing import Any


@dataclass(slots=True)
class FundamentalSnapshot:
    company_id: str
    cik: str
    period_type: str
    period_end_date: date | None
    available_at: datetime | None
    revenue: float | None = None
    net_income: float | None = None
    assets: float | None = None
    liabilities: float | None = None
    equity: float | None = None
    operating_cash_flow: float | None = None
    shares_outstanding: float | None = None
    source_name: str | None = "sec"
    created_at: datetime | None = None

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class FundamentalFeatureDaily:
    company_id: str
    as_of_date: date
    period_type: str
    revenue: float | None = None
    net_income: float | None = None
    assets: float | None = None
    liabilities: float | None = None
    equity: float | None = None
    operating_cash_flow: float | None = None
    shares_outstanding: float | None = None
    net_margin: float | None = None
    debt_to_equity: float | None = None
    return_on_assets: float | None = None
    source_name: str | None = "sec"
    created_at: datetime | None = None

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)
