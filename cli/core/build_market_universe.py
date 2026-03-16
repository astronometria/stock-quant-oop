#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json

from stock_quant.infrastructure.config.settings_loader import build_app_config
from stock_quant.infrastructure.db.duckdb_session_factory import DuckDbSessionFactory
from stock_quant.infrastructure.db.schema_manager import SchemaManager
from stock_quant.infrastructure.db.unit_of_work import DuckDbUnitOfWork
from stock_quant.infrastructure.repositories.duckdb_universe_repository import DuckDbUniverseRepository
from stock_quant.pipelines.build_market_universe_pipeline import BuildMarketUniversePipeline


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build market universe for stock-quant-oop.")
    parser.add_argument("--db-path", default=None, help="Path to DuckDB database file.")
    parser.add_argument("--drop-existing", action="store_true", help="Rebuild schema before pipeline run.")
    parser.add_argument("--disallow-adr", action="store_true", help="Exclude ADR securities from the universe.")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose output.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    config = build_app_config(db_path=args.db_path)
    config.ensure_directories()

    if args.verbose:
        print(f"[build_market_universe] project_root={config.project_root}")
        print(f"[build_market_universe] db_path={config.db_path}")
        print(f"[build_market_universe] drop_existing={args.drop_existing}")
        print(f"[build_market_universe] allow_adr={not args.disallow_adr}")

    session_factory = DuckDbSessionFactory(config.db_path)

    with DuckDbUnitOfWork(session_factory) as uow:
        schema_manager = SchemaManager(uow)
        if args.drop_existing:
            schema_manager.initialize(drop_existing=True)
        else:
            schema_manager.validate()

        repository = DuckDbUniverseRepository(uow)
        pipeline = BuildMarketUniversePipeline(
            repository=repository,
            allow_adr=not args.disallow_adr,
        )
        result = pipeline.run()

    print(json.dumps(result.summary_dict(), indent=2, sort_keys=True))
    return 0 if result.status.value == "SUCCESS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
