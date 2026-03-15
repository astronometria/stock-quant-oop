from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import date
from typing import Any

import pandas as pd

from stock_quant.app.services.price_ingestion_service import PriceIngestionService
from stock_quant.infrastructure.providers.prices.historical_price_provider import HistoricalPriceProvider
from stock_quant.infrastructure.providers.prices.yfinance_price_provider import YfinancePriceProvider
from stock_quant.pipelines.base_pipeline import BasePipeline


@dataclass(slots=True)
class BuildPricesPipeline(BasePipeline):
    repository: Any
    service: PriceIngestionService
    mode: str = "daily"
    symbols: list[str] | None = None
    as_of: date | None = None
    start_date: date | None = None
    end_date: date | None = None
    historical_provider: HistoricalPriceProvider | None = None
    daily_provider: YfinancePriceProvider | None = None

    @property
    def pipeline_name(self) -> str:
        return "build_prices"

    def extract(self) -> Any:
        symbols = self.symbols

        if self.mode == "backfill":
            if self.historical_provider is None:
                raise ValueError("historical_provider is required for mode=backfill")
            return self.historical_provider.fetch_history_frame(
                symbols=symbols,
                start_date=self.start_date,
                end_date=self.end_date,
            )

        if symbols is None:
            symbols = self._load_symbols_from_repository()

        if self.daily_provider is None:
            raise ValueError("daily_provider is required for mode=daily")

        return self.daily_provider.fetch_daily_prices(
            symbols=symbols,
            as_of=self.as_of,
        )

    def transform(self, data: Any) -> Any:
        if isinstance(data, pd.DataFrame):
            frame = self.service.build_price_frame_from_frame(data)
            summary = self.service.summarize_frame(frame, rows_read=len(data))
            return {
                "frame": frame,
                "summary": summary,
            }

        materialized = list(data)
        frame = self.service.build_price_frame(materialized)
        summary = self.service.summarize_frame(frame, rows_read=len(materialized))
        return {
            "frame": frame,
            "summary": summary,
        }

    def load(self, data: Any) -> dict[str, Any]:
        frame = data["frame"]
        summary = data["summary"]

        if frame.empty:
            return {
                "rows_read": summary.rows_read,
                "rows_written": 0,
                "rows_skipped": summary.rows_invalid,
                "warnings": ["no valid price rows to load"],
                "metrics": {
                    "mode": self.mode,
                    "symbols_count": summary.symbols_count,
                    "min_price_date": str(summary.min_price_date) if summary.min_price_date else None,
                    "max_price_date": str(summary.max_price_date) if summary.max_price_date else None,
                    "latest_rows": 0,
                },
            }

        rows_written = int(self.repository.upsert_price_history(frame))
        latest_rows = int(self.repository.refresh_price_latest())

        return {
            "rows_read": summary.rows_read,
            "rows_written": rows_written,
            "rows_skipped": summary.rows_invalid,
            "warnings": [],
            "metrics": {
                "mode": self.mode,
                "symbols_count": summary.symbols_count,
                "latest_rows": latest_rows,
                "min_price_date": str(summary.min_price_date) if summary.min_price_date else None,
                "max_price_date": str(summary.max_price_date) if summary.max_price_date else None,
            },
        }

    def _load_symbols_from_repository(self) -> list[str]:
        if hasattr(self.repository, "list_allowed_symbols"):
            return list(self.repository.list_allowed_symbols())
        if hasattr(self.repository, "get_allowed_symbols"):
            return list(self.repository.get_allowed_symbols())
        raise AttributeError(
            "price repository must implement list_allowed_symbols() or get_allowed_symbols()"
        )
