#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json

from stock_quant.infrastructure.config.settings_loader import build_app_config
from stock_quant.infrastructure.db.backtest_schema import BacktestSchemaManager
from stock_quant.infrastructure.db.dataset_builder_schema import DatasetBuilderSchemaManager
from stock_quant.infrastructure.db.duckdb_session_factory import DuckDbSessionFactory
from stock_quant.infrastructure.db.research_schema import ResearchSchemaManager
from stock_quant.infrastructure.db.unit_of_work import DuckDbUnitOfWork
from stock_quant.infrastructure.repositories.duckdb_backtest_repository import DuckDbBacktestRepository
from stock_quant.pipelines.build_backtest_pipeline import BuildBacktestPipeline


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build experiment tracking and simple backtest results.")
    parser.add_argument("--db-path", default=None, help="Path to DuckDB database file.")
    parser.add_argument("--dataset-name", default="research_dataset_v1", help="Dataset logical name.")
    parser.add_argument("--dataset-version", default="v1", help="Dataset version.")
    parser.add_argument("--experiment-name", default="momentum_experiment_v1", help="Experiment name.")
    parser.add_argument("--backtest-name", default="momentum_backtest_v1", help="Backtest name.")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose output.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    config = build_app_config(db_path=args.db_path)
    config.ensure_directories()

    if args.verbose:
        print(f"[build_backtest] project_root={config.project_root}")
        print(f"[build_backtest] db_path={config.db_path}")
        print(f"[build_backtest] dataset_name={args.dataset_name}")
        print(f"[build_backtest] dataset_version={args.dataset_version}")
        print(f"[build_backtest] experiment_name={args.experiment_name}")
        print(f"[build_backtest] backtest_name={args.backtest_name}")

    session_factory = DuckDbSessionFactory(config.db_path)
    with DuckDbUnitOfWork(session_factory) as uow:
        ResearchSchemaManager(uow).initialize()
        DatasetBuilderSchemaManager(uow).initialize()
        BacktestSchemaManager(uow).initialize()

        repository = DuckDbBacktestRepository(uow)
        pipeline = BuildBacktestPipeline(
            repository=repository,
            dataset_name=args.dataset_name,
            dataset_version=args.dataset_version,
            experiment_name=args.experiment_name,
            backtest_name=args.backtest_name,
        )
        result = pipeline.run()

    print(json.dumps(result.summary_dict(), indent=2, sort_keys=True))
    return 0 if result.status.value == "SUCCESS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
