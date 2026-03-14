#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Show research pipeline operational status.")
    parser.add_argument("--db-path", default="~/stock-quant-oop/market.duckdb")
    return parser.parse_args()


def safe_scalar(con: duckdb.DuckDBPyConnection, sql: str):
    try:
        row = con.execute(sql).fetchone()
        return row[0] if row else None
    except Exception as exc:
        return f"ERROR: {exc}"


def main() -> int:
    args = parse_args()
    db_path = str(Path(args.db_path).expanduser().resolve())
    con = duckdb.connect(db_path)

    status = {
        "db_path": db_path,
        "counts": {
            "pipeline_runs": safe_scalar(con, "SELECT COUNT(*) FROM pipeline_runs"),
            "llm_runs": safe_scalar(con, "SELECT COUNT(*) FROM llm_runs"),
            "dataset_versions": safe_scalar(con, "SELECT COUNT(*) FROM dataset_versions"),
            "experiment_runs": safe_scalar(con, "SELECT COUNT(*) FROM experiment_runs"),
            "backtest_runs": safe_scalar(con, "SELECT COUNT(*) FROM backtest_runs"),
            "research_features_daily": safe_scalar(con, "SELECT COUNT(*) FROM research_features_daily"),
            "return_labels_daily": safe_scalar(con, "SELECT COUNT(*) FROM return_labels_daily"),
            "research_dataset_daily": safe_scalar(con, "SELECT COUNT(*) FROM research_dataset_daily"),
            "experiment_results_daily": safe_scalar(con, "SELECT COUNT(*) FROM experiment_results_daily"),
        },
        "latest": {
            "last_pipeline_run": con.execute(
                """
                SELECT pipeline_name, status, rows_read, rows_written, created_at
                FROM pipeline_runs
                ORDER BY created_at DESC
                LIMIT 1
                """
            ).fetchall(),
            "last_llm_run": con.execute(
                """
                SELECT run_name, model_name, prompt_version, status, created_at
                FROM llm_runs
                ORDER BY created_at DESC
                LIMIT 1
                """
            ).fetchall(),
            "last_dataset_version": con.execute(
                """
                SELECT dataset_name, dataset_version, as_of_date, row_count, created_at
                FROM dataset_versions
                ORDER BY created_at DESC
                LIMIT 1
                """
            ).fetchall(),
            "last_experiment_run": con.execute(
                """
                SELECT experiment_name, status, metrics_json, created_at
                FROM experiment_runs
                ORDER BY created_at DESC
                LIMIT 1
                """
            ).fetchall(),
            "last_backtest_run": con.execute(
                """
                SELECT backtest_name, status, metrics_json, created_at
                FROM backtest_runs
                ORDER BY created_at DESC
                LIMIT 1
                """
            ).fetchall(),
        },
    }

    con.close()
    print(json.dumps(status, indent=2, default=str, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
