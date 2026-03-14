from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date, datetime
from typing import Any


@dataclass(slots=True)
class ExperimentResultDaily:
    experiment_name: str
    dataset_name: str
    dataset_version: str
    instrument_id: str
    company_id: str | None
    symbol: str
    as_of_date: date
    signal_value: float | None
    selected_flag: bool
    realized_return_1d: float | None
    created_at: datetime | None = None

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class BacktestResultSummary:
    backtest_name: str
    dataset_name: str
    dataset_version: str
    observations: int
    selected_observations: int
    mean_return_1d: float | None
    hit_rate_1d: float | None
    cumulative_return_proxy: float | None
    created_at: datetime | None = None

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)
