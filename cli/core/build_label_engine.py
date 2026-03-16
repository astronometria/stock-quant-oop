#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json

from stock_quant.infrastructure.config.settings_loader import build_app_config
from stock_quant.infrastructure.db.duckdb_session_factory import DuckDbSessionFactory
from stock_quant.infrastructure.db.label_engine_schema import LabelEngineSchemaManager
from stock_quant.infrastructure.db.prices_research_schema import PricesResearchSchemaManager
from stock_quant.infrastructure.db.unit_of_work import DuckDbUnitOfWork
from stock_quant.infrastructure.repositories.duckdb_label_engine_repository import DuckDbLabelEngineRepository
from stock_quant.pipelines.build_label_engine_pipeline import BuildLabelEnginePipeline


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build research label tables.")
    parser.add_argument("--db-path", default=None, help="Path to DuckDB database file.")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose output.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    config = build_app_config(db_path=args.db_path)
    config.ensure_directories()

    if args.verbose:
        print(f"[build_label_engine] project_root={config.project_root}")
        print(f"[build_label_engine] db_path={config.db_path}")

    session_factory = DuckDbSessionFactory(config.db_path)
    with DuckDbUnitOfWork(session_factory) as uow:
        PricesResearchSchemaManager(uow).initialize()
        LabelEngineSchemaManager(uow).initialize()

        repository = DuckDbLabelEngineRepository(uow)
        pipeline = BuildLabelEnginePipeline(repository=repository)
        result = pipeline.run()

    print(json.dumps(result.summary_dict(), indent=2, sort_keys=True))
    return 0 if result.status.value == "SUCCESS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
