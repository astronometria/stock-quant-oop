#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path
from datetime import datetime, timezone

import duckdb

from stock_quant.infrastructure.repositories.duckdb_research_experiment_repository import (
    DuckDbResearchExperimentRepository,
    ResearchExperimentManifest,
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _now():
    return datetime.now(timezone.utc)


def _run(cmd):
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(result.stderr or result.stdout)
    return json.loads(result.stdout)


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--db-path", required=True)
    p.add_argument("--snapshot-id", required=True)
    p.add_argument("--experiment-name", default="exp_v2")
    return p.parse_args()


def main():
    args = parse_args()
    db = Path(args.db_path).resolve()

    con = duckdb.connect(str(db))
    repo = DuckDbResearchExperimentRepository(con)
    repo.ensure_tables()

    # --- dataset ---
    ds = _run([
        sys.executable,
        str(PROJECT_ROOT / "cli/core/build_research_training_dataset.py"),
        "--db-path", str(db),
        "--snapshot-id", args.snapshot_id
    ])
    dataset_id = ds["dataset_id"]

    # --- labels ---
    _run([
        sys.executable,
        str(PROJECT_ROOT / "cli/core/build_research_labels.py"),
        "--db-path", str(db),
        "--snapshot-id", args.snapshot_id,
        "--dataset-id", dataset_id
    ])

    # --- métriques simples ---
    metrics = con.execute(f"""
        SELECT
            AVG(fwd_return_1d) AS avg_return,
            COUNT(*) AS n
        FROM research_labels
        WHERE dataset_id = '{dataset_id}'
    """).fetchone()

    metrics_json = json.dumps({
        "avg_return_1d": float(metrics[0]) if metrics[0] else None,
        "n": int(metrics[1])
    })

    exp_id = f"{args.experiment_name}_{_now().strftime('%Y%m%dT%H%M%SZ')}"

    repo.insert(ResearchExperimentManifest(
        experiment_id=exp_id,
        snapshot_id=args.snapshot_id,
        dataset_id=dataset_id,
        experiment_name=args.experiment_name,
        git_commit=None,
        parameters_json=None,
        metrics_json=metrics_json,
        status="completed",
        notes=None,
        created_by_pipeline="run_research_experiment_v2"
    ))

    print(json.dumps({
        "experiment_id": exp_id,
        "dataset_id": dataset_id,
        "metrics": json.loads(metrics_json)
    }, indent=2))


if __name__ == "__main__":
    main()
