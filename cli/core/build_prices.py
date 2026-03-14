#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

from stock_quant.app.services.price_ingestion_service import PriceIngestionService
from stock_quant.infrastructure.config.settings_loader import build_app_config
from stock_quant.infrastructure.db.duckdb_session_factory import DuckDbSessionFactory
from stock_quant.infrastructure.db.schema_manager import SchemaManager
from stock_quant.infrastructure.db.unit_of_work import DuckDbUnitOfWork
from stock_quant.infrastructure.providers.prices.historical_price_provider import HistoricalPriceProvider
from stock_quant.infrastructure.providers.prices.yfinance_price_provider import YfinancePriceProvider
from stock_quant.infrastructure.repositories.duckdb_price_repository import DuckDbPriceRepository
from stock_quant.pipelines.prices_pipeline import BuildPricesPipeline


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build price_history and price_latest using the OOP price pipeline.")
    parser.add_argument("--db-path", default=None, help="Path to DuckDB database file.")
    parser.add_argument("--mode", default="daily", choices=["daily", "backfill"], help="Price build mode.")
    parser.add_argument(
        "--historical-source",
        action="append",
        default=[],
        help="Historical source path. Repeat this flag for multiple CSV/ZIP/directories when mode=backfill.",
    )
    parser.add_argument(
        "--symbol",
        action="append",
        dest="symbols",
        default=[],
        help="Optional symbol filter. Repeat this flag for multiple symbols.",
    )
    parser.add_argument("--start-date", default=None, help="Optional start date (YYYY-MM-DD).")
    parser.add_argument("--end-date", default=None, help="Optional end date (YYYY-MM-DD).")
    parser.add_argument("--as-of", default=None, help="Optional single target date (YYYY-MM-DD) for daily mode.")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose output.")
    return parser.parse_args()


def parse_date(value: str | None):
    if value is None:
        return None
    return Path(value) and __import__("datetime").date.fromisoformat(value)


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

    session_factory = DuckDbSessionFactory(config.db_path)

    with DuckDbUnitOfWork(session_factory) as uow:
        schema_manager = SchemaManager(uow)
        schema_manager.validate()

        repository = DuckDbPriceRepository(uow)
        service = PriceIngestionService()

        historical_provider = None
        if args.mode == "backfill":
            if not args.historical_source:
                raise SystemExit("--historical-source is required when --mode=backfill")
            historical_provider = HistoricalPriceProvider(source_paths=args.historical_source)

        daily_provider = YfinancePriceProvider() if args.mode == "daily" else None

        pipeline = BuildPricesPipeline(
            repository=repository,
            service=service,
            mode=args.mode,
            symbols=args.symbols or None,
            as_of=parse_date(args.as_of),
            start_date=parse_date(args.start_date),
            end_date=parse_date(args.end_date),
            historical_provider=historical_provider,
            daily_provider=daily_provider,
        )
        result = pipeline.run()

    print(json.dumps(result.summary_dict(), indent=2, sort_keys=True))
    return 0 if result.status.value == "SUCCESS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
