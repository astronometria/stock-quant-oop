#!/usr/bin/env python3
from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

from stock_quant.infrastructure.config.settings_loader import build_app_config


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Replay a research pipeline run under alternate names.")
    parser.add_argument("--db-path", default=None, help="Path to DuckDB database file.")
    parser.add_argument("--dataset-name", required=True)
    parser.add_argument("--dataset-version", required=True)
    parser.add_argument("--experiment-name", required=True)
    parser.add_argument("--backtest-name", required=True)
    parser.add_argument("--llm-run-name", default="news_llm_enrichment_replay")
    parser.add_argument("--model-name", default="placeholder")
    parser.add_argument("--prompt-version", default="v1")
    parser.add_argument("--verbose", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    config = build_app_config(db_path=args.db_path)
    config.ensure_directories()

    project_root = Path(config.project_root)
    command = [
        sys.executable,
        str(project_root / "cli" / "ops" / "run_research_daily.py"),
        "--db-path", config.db_path,
        "--dataset-name", args.dataset_name,
        "--dataset-version", args.dataset_version,
        "--experiment-name", args.experiment_name,
        "--backtest-name", args.backtest_name,
        "--llm-run-name", args.llm_run_name,
        "--model-name", args.model_name,
        "--prompt-version", args.prompt_version,
    ]
    if args.verbose:
        command.append("--verbose")

    return subprocess.run(command).returncode


if __name__ == "__main__":
    raise SystemExit(main())
