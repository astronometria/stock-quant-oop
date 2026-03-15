from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from stock_quant.pipelines.prices_pipeline import PricesPipeline, PricesPipelineResult
from stock_quant.shared.exceptions import ServiceError


@dataclass(slots=True)
class PriceDailyRefreshResult:
    requested_symbols: int
    fetched_symbols: int
    written_price_history_rows: int
    price_latest_rows_after_refresh: int
    start_date: str | None
    end_date: str | None


class PriceDailyRefreshOrchestrator:
    """
    Daily orchestration entrypoint for incremental price refresh.

    Responsibilities:
    - expose a stable app-level entrypoint for daily price refresh
    - delegate data work to PricesPipeline
    - keep scheduling/CLI integration simple
    """

    def __init__(self, prices_pipeline: PricesPipeline) -> None:
        self.prices_pipeline = prices_pipeline

    def run_daily_refresh(
        self,
        symbols: Iterable[str] | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> PriceDailyRefreshResult:
        try:
            result = self.prices_pipeline.run(
                symbols=symbols,
                start_date=start_date,
                end_date=end_date,
            )
            return self._to_orchestrator_result(
                result=result,
                start_date=start_date,
                end_date=end_date,
            )
        except Exception as exc:
            if isinstance(exc, ServiceError):
                raise
            raise ServiceError(f"failed to run daily price refresh: {exc}") from exc

    def _to_orchestrator_result(
        self,
        result: PricesPipelineResult,
        start_date: str | None,
        end_date: str | None,
    ) -> PriceDailyRefreshResult:
        return PriceDailyRefreshResult(
            requested_symbols=result.requested_symbols,
            fetched_symbols=result.fetched_symbols,
            written_price_history_rows=result.written_price_history_rows,
            price_latest_rows_after_refresh=result.price_latest_rows_after_refresh,
            start_date=start_date,
            end_date=end_date,
        )
