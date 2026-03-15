#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json

from stock_quant.infrastructure.config.settings_loader import build_app_config
from stock_quant.infrastructure.db.duckdb_session_factory import DuckDbSessionFactory
from stock_quant.infrastructure.db.master_data_schema import MasterDataSchemaManager
from stock_quant.infrastructure.db.sec_schema import SecSchemaManager
from stock_quant.infrastructure.db.unit_of_work import DuckDbUnitOfWork
from stock_quant.infrastructure.repositories.duckdb_sec_repository import DuckDbSecRepository
from stock_quant.pipelines.sec_pipeline import BuildSecFilingPipeline
from stock_quant.shared.exceptions import PipelineError


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build normalized SEC filing tables incrementally.")
    parser.add_argument("--db-path", default=None, help="Path to DuckDB database file.")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose output.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    config = build_app_config(db_path=args.db_path)
    config.ensure_directories()

    if args.verbose:
        print(f"[build_sec_filings] project_root={config.project_root}")
        print(f"[build_sec_filings] db_path={config.db_path}")

    session_factory = DuckDbSessionFactory(config.db_path)

    # Pass 1a: master-data schema
    with DuckDbUnitOfWork(session_factory) as uow:
        MasterDataSchemaManager(uow).initialize()

    # Pass 1b: SEC schema
    with DuckDbUnitOfWork(session_factory) as uow:
        SecSchemaManager(uow).initialize()

    # Pass 2: run incremental SEC filing build on a fresh connection
    with DuckDbUnitOfWork(session_factory) as uow:
        repository = DuckDbSecRepository(uow)
        pipeline = BuildSecFilingPipeline(repository=repository)
        result = pipeline.run()

    print(json.dumps(result.summary_dict(), indent=2, sort_keys=True))
    return 0 if result.status.value == "SUCCESS" else 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except PipelineError as exc:
        print(json.dumps({"status": "FAILED", "error": str(exc)}, indent=2, sort_keys=True))
        raise SystemExit(1)
