from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from stock_quant.app.services.price_ingestion_service import (
    PriceIngestionResult,
    PriceIngestionService,
)
from stock_quant.shared.exceptions import PipelineError


@dataclass(slots=True)
class PricesPipelineResult:
    requested_symbols: int
    fetched_symbols: int
    written_price_history_rows: int
    price_latest_rows_after_refresh: int


class PricesPipeline:
    """
    Incremental prices pipeline.

    Responsibilities:
    - delegate ingestion work to PriceIngestionService
    - expose a stable pipeline-level result object
    - stay compatible with daily refresh orchestration
    """

    pipeline_name = "prices_incremental"

    def __init__(self, price_ingestion_service: PriceIngestionService) -> None:
        self.price_ingestion_service = price_ingestion_service

    def run(
        self,
        symbols: Iterable[str] | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> PricesPipelineResult:
        try:
            result = self.price_ingestion_service.ingest_incremental(
                symbols=symbols,
                start_date=start_date,
                end_date=end_date,
            )
            return self._to_pipeline_result(result)
        except Exception as exc:
            if isinstance(exc, PipelineError):
                raise
            raise PipelineError(f"failed to run prices pipeline: {exc}") from exc

    def _to_pipeline_result(self, result: PriceIngestionResult) -> PricesPipelineResult:
        return PricesPipelineResult(
            requested_symbols=result.requested_symbols,
            fetched_symbols=result.fetched_symbols,
            written_price_history_rows=result.written_price_history_rows,
            price_latest_rows_after_refresh=result.price_latest_rows_after_refresh,
        )
