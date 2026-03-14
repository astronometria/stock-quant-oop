#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

from stock_quant.app.orchestrators.research_pipeline_orchestrator import ResearchPipelineOrchestrator
from stock_quant.infrastructure.config.settings_loader import build_app_config


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the research pipeline sequence.")
    parser.add_argument("--db-path", default=None, help="Path to DuckDB database file.")
    parser.add_argument("--dataset-name", default="research_dataset_v1", help="Dataset name.")
    parser.add_argument("--dataset-version", default="v1", help="Dataset version.")
    parser.add_argument("--experiment-name", default="momentum_experiment_v1", help="Experiment name.")
    parser.add_argument("--backtest-name", default="momentum_backtest_v1", help="Backtest name.")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose output in child steps.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    config = build_app_config(db_path=args.db_path)
    config.ensure_directories()

    orchestrator = ResearchPipelineOrchestrator(
        project_root=Path(config.project_root),
        db_path=config.db_path,
    )
    return orchestrator.run(
        dataset_name=args.dataset_name,
        dataset_version=args.dataset_version,
        experiment_name=args.experiment_name,
        backtest_name=args.backtest_name,
        verbose=bool(args.verbose),
    )


if __name__ == "__main__":
    raise SystemExit(main())
