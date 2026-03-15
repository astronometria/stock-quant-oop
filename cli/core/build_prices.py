#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from datetime import date
from typing import Iterable

import pandas as pd

from stock_quant.app.orchestrators.price_daily_refresh_orchestrator import PriceDailyRefreshOrchestrator
from stock_quant.app.services.price_ingestion_service import PriceIngestionService
from stock_quant.infrastructure.config.settings_loader import build_app_config
from stock_quant.infrastructure.db.duckdb_session_factory import DuckDbSessionFactory
from stock_quant.infrastructure.db.master_data_schema import MasterDataSchemaManager
from stock_quant.infrastructure.db.unit_of_work import DuckDbUnitOfWork
from stock_quant.infrastructure.providers.prices.historical_price_provider import HistoricalPriceProvider
from stock_quant.infrastructure.providers.prices.yfinance_price_provider import YfinancePriceProvider
from stock_quant.infrastructure.repositories.duckdb_price_repository import DuckDbPriceRepository
from stock_quant.pipelines.prices_pipeline import PricesPipeline
from stock_quant.shared.exceptions import PipelineError, ServiceError


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build price_history and price_latest using the incremental OOP price pipeline."
    )
    parser.add_argument("--db-path", default=None, help="Path to DuckDB database file.")
    parser.add_argument("--mode", default="daily", choices=["daily", "backfill"], help="Price build mode.")
    parser.add_argument(
        "--historical-source",
        action="append",
        default=[],
        help="Historical source path(s). Repeat for multiple CSV/ZIP/directories when mode=backfill.",
    )
    parser.add_argument(
        "--symbol",
        action="append",
        dest="symbols",
        default=[],
        help="Optional symbol filter. Repeat for multiple symbols.",
    )
    parser.add_argument("--start-date", default=None, help="Optional start date (YYYY-MM-DD).")
    parser.add_argument("--end-date", default=None, help="Optional end date (YYYY-MM-DD).")
    parser.add_argument("--as-of", default=None, help="Optional single target date (YYYY-MM-DD) for daily mode.")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose output.")
    return parser.parse_args()


def _parse_iso_date(value: str | None) -> date | None:
    if value is None or not str(value).strip():
        return None
    return date.fromisoformat(str(value).strip())


def _normalize_symbols(values: Iterable[str] | None) -> list[str]:
    return sorted({str(v).strip().upper() for v in (values or []) if v is not None and str(v).strip()})


class ProviderFrameAdapter:
    def __init__(self, provider, mode: str) -> None:
        self._provider = provider
        self._mode = mode

    def fetch_prices(self, symbols: list[str], start_date: str | None, end_date: str | None) -> pd.DataFrame:
        if self._mode == "daily":
            as_of = _parse_iso_date(end_date or start_date)
            frame_method = getattr(self._provider, "fetch_daily_prices_frame", None)
            if callable(frame_method):
                return frame_method(symbols=symbols, as_of=as_of)
            raise ServiceError("daily provider does not support fetch_daily_prices_frame")

        start = _parse_iso_date(start_date)
        end = _parse_iso_date(end_date)
        frame_method = getattr(self._provider, "fetch_history_frame", None)
        if callable(frame_method):
            return frame_method(symbols=symbols, start_date=start, end_date=end)
        raise ServiceError("historical provider does not support fetch_history_frame")


def main() -> int:
    args = parse_args()
    config = build_app_config(db_path=args.db_path)
    config.ensure_directories()

    if args.verbose:
        print(f"[build_prices] project_root={config.project_root}")
        print(f"[build_prices] db_path={config.db_path}")
        print(f"[build_prices] mode={args.mode}")
        print(f"[build_prices] historical_source_count={len(args.historical_source)}")
        print(f"[build_prices] symbols_count={len(args.symbols)}")

    start_date = args.start_date
    end_date = args.end_date
    if args.mode == "daily" and args.as_of:
        start_date = args.as_of
        end_date = args.as_of

    requested_symbols = _normalize_symbols(args.symbols)
    session_factory = DuckDbSessionFactory(config.db_path)

    # Pass 1: schema evolution / init
    with DuckDbUnitOfWork(session_factory) as uow:
        MasterDataSchemaManager(uow).initialize()

    # Pass 2: actual price ingestion using a fresh active connection
    with DuckDbUnitOfWork(session_factory) as uow:
        repository = DuckDbPriceRepository(uow)

        if args.mode == "backfill":
            if not args.historical_source:
                raise SystemExit("--historical-source is required when --mode=backfill")
            provider = HistoricalPriceProvider(source_paths=args.historical_source)
            adapter = ProviderFrameAdapter(provider=provider, mode="backfill")
        else:
            provider = YfinancePriceProvider()
            adapter = ProviderFrameAdapter(provider=provider, mode="daily")

        service = PriceIngestionService(price_repository=repository, historical_price_provider=adapter)
        pipeline = PricesPipeline(price_ingestion_service=service)
        orchestrator = PriceDailyRefreshOrchestrator(prices_pipeline=pipeline)

        result = orchestrator.run_daily_refresh(
            symbols=requested_symbols or None,
            start_date=start_date,
            end_date=end_date,
        )

        payload = {
            "status": "SUCCESS",
            "mode": args.mode,
            "requested_symbols": result.requested_symbols,
            "fetched_symbols": result.fetched_symbols,
            "written_price_history_rows": result.written_price_history_rows,
            "price_latest_rows_after_refresh": result.price_latest_rows_after_refresh,
            "start_date": result.start_date,
            "end_date": result.end_date,
        }
        print(json.dumps(payload, indent=2, sort_keys=True))
        return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (PipelineError, ServiceError) as exc:
        print(json.dumps({"status": "FAILED", "error": str(exc)}, indent=2, sort_keys=True))
        raise SystemExit(1)
