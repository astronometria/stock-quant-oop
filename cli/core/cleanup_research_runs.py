#!/usr/bin/env python3
from __future__ import annotations

import argparse

from stock_quant.infrastructure.config.settings_loader import build_app_config
from stock_quant.infrastructure.db.duckdb_session_factory import DuckDbSessionFactory
from stock_quant.infrastructure.db.research_schema import ResearchSchemaManager
from stock_quant.infrastructure.db.unit_of_work import DuckDbUnitOfWork
from stock_quant.infrastructure.repositories.duckdb_pipeline_run_repository import DuckDbPipelineRunRepository


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Cleanup duplicate logical runs before rerun.")
    parser.add_argument("--db-path", default=None, help="Path to DuckDB database file.")
    parser.add_argument("--dataset-name", default="research_dataset_v1")
    parser.add_argument("--dataset-version", default="v1")
    parser.add_argument("--experiment-name", default="momentum_experiment_v1")
    parser.add_argument("--backtest-name", default="momentum_backtest_v1")
    parser.add_argument("--llm-run-name", default="news_llm_enrichment_v1")
    parser.add_argument("--verbose", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    config = build_app_config(db_path=args.db_path)
    config.ensure_directories()

    session_factory = DuckDbSessionFactory(config.db_path)
    with DuckDbUnitOfWork(session_factory) as uow:
        ResearchSchemaManager(uow).initialize()
        repo = DuckDbPipelineRunRepository(uow)

        repo.delete_dataset_version(args.dataset_name, args.dataset_version)
        repo.delete_experiment_runs(args.experiment_name)
        repo.delete_backtest_runs(args.backtest_name)
        repo.delete_llm_runs(args.llm_run_name)

    print("cleanup complete")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
