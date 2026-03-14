#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json

from stock_quant.infrastructure.config.settings_loader import build_app_config
from stock_quant.infrastructure.db.duckdb_session_factory import DuckDbSessionFactory
from stock_quant.infrastructure.db.fundamentals_schema import FundamentalsSchemaManager
from stock_quant.infrastructure.db.sec_schema import SecSchemaManager
from stock_quant.infrastructure.db.unit_of_work import DuckDbUnitOfWork
from stock_quant.infrastructure.repositories.duckdb_fundamentals_repository import DuckDbFundamentalsRepository
from stock_quant.pipelines.fundamentals_pipeline import BuildFundamentalsPipeline


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build fundamentals snapshots and features.")
    parser.add_argument("--db-path", default=None, help="Path to DuckDB database file.")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose output.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    config = build_app_config(db_path=args.db_path)
    config.ensure_directories()

    if args.verbose:
        print(f"[build_fundamentals] project_root={config.project_root}")
        print(f"[build_fundamentals] db_path={config.db_path}")

    session_factory = DuckDbSessionFactory(config.db_path)
    with DuckDbUnitOfWork(session_factory) as uow:
        sec_schema = SecSchemaManager(uow)
        sec_schema.initialize()

        fundamentals_schema = FundamentalsSchemaManager(uow)
        fundamentals_schema.initialize()

        repository = DuckDbFundamentalsRepository(uow)
        pipeline = BuildFundamentalsPipeline(repository=repository)
        result = pipeline.run()

    print(json.dumps(result.summary_dict(), indent=2, sort_keys=True))
    return 0 if result.status.value == "SUCCESS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
