#!/usr/bin/env python3
from __future__ import annotations

"""
Research experiment orchestration entrypoint.

V1
--
Cette première version:
- exige un snapshot_id existant et completed
- enregistre un experiment_id
- persiste paramètres + métriques minimales
- fournit une base robuste pour brancher ensuite:
  - build_training_dataset.py
  - build_backtest.py
  - modèles / signaux / scoring
"""

import argparse
import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from stock_quant.infrastructure.repositories.duckdb_research_experiment_repository import (
    DuckDbResearchExperimentRepository,
    ResearchExperimentManifest,
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _safe_git_commit(project_root: Path) -> str | None:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=str(project_root),
            capture_output=True,
            text=True,
            check=True,
        )
        value = result.stdout.strip()
        return value or None
    except Exception:
        return None


def _experiment_id(experiment_name: str) -> str:
    stamp = _now_utc().strftime("%Y%m%dT%H%M%SZ")
    safe_name = experiment_name.strip().lower().replace(" ", "_")
    return f"{safe_name}_{stamp}"


def _normalize_json(raw: str | None) -> str | None:
    if raw is None:
        return None
    value = raw.strip()
    if not value:
        return None
    parsed = json.loads(value)
    return json.dumps(parsed, sort_keys=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run a research experiment from a frozen snapshot."
    )
    parser.add_argument("--db-path", required=True, help="Path to DuckDB database file.")
    parser.add_argument("--snapshot-id", required=True, help="Existing completed snapshot_id.")
    parser.add_argument(
        "--experiment-name",
        default="research_experiment_v1",
        help="Logical experiment name.",
    )
    parser.add_argument(
        "--parameters-json",
        default=None,
        help="Optional JSON experiment parameters.",
    )
    parser.add_argument(
        "--notes",
        default=None,
        help="Optional free-form notes.",
    )
    parser.add_argument(
        "--created-by-pipeline",
        default="run_research_experiment",
        help="Pipeline name stored in manifest.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    db_path = Path(args.db_path).expanduser().resolve()
    started_at = _now_utc()

    print(f"[run_research_experiment] db_path={db_path}", flush=True)

    con = duckdb.connect(str(db_path))
    try:
        repo = DuckDbResearchExperimentRepository(con)
        repo.ensure_tables()

        snapshot_row = repo.get_snapshot_manifest(args.snapshot_id)
        if snapshot_row is None:
            output = {
                "pipeline_name": "run_research_experiment",
                "status": "failed",
                "started_at": started_at,
                "finished_at": _now_utc(),
                "error_message": f"snapshot_id not found: {args.snapshot_id}",
            }
            print(json.dumps(output, default=str, indent=2), flush=True)
            return 1

        (
            snapshot_id,
            dataset_name,
            snapshot_git_commit,
            start_date,
            end_date,
            source_count,
            total_row_count,
            snapshot_status,
            snapshot_created_at,
        ) = snapshot_row

        if snapshot_status != "completed":
            output = {
                "pipeline_name": "run_research_experiment",
                "status": "failed",
                "started_at": started_at,
                "finished_at": _now_utc(),
                "error_message": f"snapshot_id is not completed: {snapshot_id}",
                "snapshot_status": snapshot_status,
            }
            print(json.dumps(output, default=str, indent=2), flush=True)
            return 1

        experiment_name = args.experiment_name.strip()
        experiment_id = _experiment_id(experiment_name)
        git_commit = _safe_git_commit(PROJECT_ROOT)
        parameters_json = _normalize_json(args.parameters_json)

        # V1: métriques minimales, mais déjà traçables.
        metrics = {
            "snapshot_dataset_name": dataset_name,
            "snapshot_git_commit": snapshot_git_commit,
            "snapshot_source_count": int(source_count),
            "snapshot_total_row_count": int(total_row_count),
            "snapshot_start_date": str(start_date) if start_date is not None else None,
            "snapshot_end_date": str(end_date) if end_date is not None else None,
            "snapshot_created_at": str(snapshot_created_at),
            "runner_started_at": str(started_at),
        }
        metrics_json = json.dumps(metrics, sort_keys=True)

        repo.insert_experiment_manifest(
            ResearchExperimentManifest(
                experiment_id=experiment_id,
                snapshot_id=snapshot_id,
                experiment_name=experiment_name,
                git_commit=git_commit,
                parameters_json=parameters_json,
                metrics_json=metrics_json,
                status="completed",
                notes=args.notes,
                created_by_pipeline=args.created_by_pipeline,
            )
        )

        finished_at = _now_utc()
        output = {
            "pipeline_name": "run_research_experiment",
            "status": "success",
            "started_at": started_at,
            "finished_at": finished_at,
            "experiment_id": experiment_id,
            "snapshot_id": snapshot_id,
            "experiment_name": experiment_name,
            "git_commit": git_commit,
            "metrics": metrics,
        }
        print(json.dumps(output, default=str, indent=2), flush=True)
        return 0

    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
