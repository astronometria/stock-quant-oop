#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json

from stock_quant.infrastructure.config.settings_loader import build_app_config
from stock_quant.infrastructure.db.duckdb_session_factory import DuckDbSessionFactory
from stock_quant.infrastructure.db.master_data_schema import MasterDataSchemaManager
from stock_quant.infrastructure.db.short_data_schema import ShortDataSchemaManager
from stock_quant.infrastructure.db.unit_of_work import DuckDbUnitOfWork
from stock_quant.infrastructure.repositories.duckdb_short_data_repository import DuckDbShortDataRepository
from stock_quant.pipelines.short_data_pipeline import BuildShortDataPipeline
from stock_quant.shared.exceptions import PipelineError


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build short data history and features.")
    parser.add_argument("--db-path", default=None, help="Path to DuckDB database file.")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose output.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    config = build_app_config(db_path=args.db_path)
    config.ensure_directories()

    if args.verbose:
        print(f"[build_short_data] project_root={config.project_root}")
        print(f"[build_short_data] db_path={config.db_path}")

    session_factory = DuckDbSessionFactory(config.db_path)

    # Pass 1: ensure schemas exist/evolve
    with DuckDbUnitOfWork(session_factory) as uow:
        master_schema = MasterDataSchemaManager(uow)
        master_schema.initialize()

        short_schema = ShortDataSchemaManager(uow)
        short_schema.initialize()

    # Pass 2: run the pipeline on a fresh connection
    with DuckDbUnitOfWork(session_factory) as uow:
        repository = DuckDbShortDataRepository(uow)
        pipeline = BuildShortDataPipeline(repository=repository)
        result = pipeline.run()

    print(json.dumps(result.summary_dict(), indent=2, sort_keys=True))
    return 0 if result.status.value == "SUCCESS" else 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except PipelineError as exc:
        print(json.dumps({"status": "FAILED", "error": str(exc)}, indent=2, sort_keys=True))
        raise SystemExit(1)
