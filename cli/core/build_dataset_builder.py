#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json

from stock_quant.infrastructure.config.settings_loader import build_app_config
from stock_quant.infrastructure.db.dataset_builder_schema import DatasetBuilderSchemaManager
from stock_quant.infrastructure.db.duckdb_session_factory import DuckDbSessionFactory
from stock_quant.infrastructure.db.feature_engine_schema import FeatureEngineSchemaManager
from stock_quant.infrastructure.db.label_engine_schema import LabelEngineSchemaManager
from stock_quant.infrastructure.db.research_schema import ResearchSchemaManager
from stock_quant.infrastructure.db.research_universe_schema import ResearchUniverseSchemaManager
from stock_quant.infrastructure.db.unit_of_work import DuckDbUnitOfWork
from stock_quant.pipelines.dataset_builder_pipeline import BuildDatasetBuilderPipeline


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build versioned research dataset table.")
    parser.add_argument("--db-path", default=None, help="Path to DuckDB database file.")
    parser.add_argument("--dataset-name", default="research_dataset_v1", help="Dataset logical name.")
    parser.add_argument("--dataset-version", default="v1", help="Dataset version.")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose output.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    config = build_app_config(db_path=args.db_path)
    config.ensure_directories()

    if args.verbose:
        print(f"[build_dataset_builder] project_root={config.project_root}")
        print(f"[build_dataset_builder] db_path={config.db_path}")
        print(f"[build_dataset_builder] dataset_name={args.dataset_name}")
        print(f"[build_dataset_builder] dataset_version={args.dataset_version}")

    session_factory = DuckDbSessionFactory(config.db_path)
    with DuckDbUnitOfWork(session_factory) as uow:
        ResearchSchemaManager(uow).initialize()
        FeatureEngineSchemaManager(uow).initialize()
        LabelEngineSchemaManager(uow).initialize()
        DatasetBuilderSchemaManager(uow).initialize()
        ResearchUniverseSchemaManager(uow).initialize()

        pipeline = BuildDatasetBuilderPipeline(
            uow=uow,
            dataset_name=args.dataset_name,
            dataset_version=args.dataset_version,
        )
        result = pipeline.run()

    print(json.dumps(result.summary_dict(), indent=2, sort_keys=True))
    return 0 if result.status.value == "SUCCESS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
