#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb

from stock_quant.infrastructure.repositories.duckdb_research_experiment_repository import (
    DuckDbResearchExperimentRepository,
    ResearchExperimentManifest,
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _extract_last_json_object(stdout: str) -> dict[str, Any]:
    lines = stdout.strip().splitlines()
    start_index = None

    for i in range(len(lines) - 1, -1, -1):
        if lines[i].lstrip().startswith("{"):
            start_index = i
            break

    if start_index is None:
        raise RuntimeError(f"no JSON object found in stdout:\n{stdout}")

    candidate = "\n".join(lines[start_index:])
    try:
        return json.loads(candidate)
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            f"failed to parse trailing JSON from stdout:\n{stdout}"
        ) from exc


def _run(cmd: list[str]) -> dict[str, Any]:
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(result.stderr or result.stdout)
    return _extract_last_json_object(result.stdout)


def _normalize_json(raw: str | None) -> str | None:
    if raw is None:
        return None
    value = raw.strip()
    if not value:
        return None
    parsed = json.loads(value)
    return json.dumps(parsed, sort_keys=True)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--db-path", required=True)
    p.add_argument("--snapshot-id", required=True)
    p.add_argument("--experiment-name", default="exp_v3")
    p.add_argument("--parameters-json", default=None)
    p.add_argument("--notes", default=None)
    return p.parse_args()


def main() -> int:
    args = parse_args()
    db = Path(args.db_path).expanduser().resolve()

    print(f"[run_research_experiment] db_path={db}", flush=True)

    con = duckdb.connect(str(db))
    try:
        repo = DuckDbResearchExperimentRepository(con)
        repo.ensure_tables()
    finally:
        con.close()

    dataset_result = _run([
        sys.executable,
        str(PROJECT_ROOT / "cli/core/build_research_training_dataset.py"),
        "--db-path", str(db),
        "--snapshot-id", args.snapshot_id,
    ])
    dataset_id = dataset_result["dataset_id"]

    labels_result = _run([
        sys.executable,
        str(PROJECT_ROOT / "cli/core/build_research_labels.py"),
        "--db-path", str(db),
        "--snapshot-id", args.snapshot_id,
        "--dataset-id", dataset_id,
    ])

    backtest_result = _run([
        sys.executable,
        str(PROJECT_ROOT / "cli/core/build_research_backtest.py"),
        "--db-path", str(db),
        "--dataset-id", dataset_id,
    ])

    con = duckdb.connect(str(db))
    try:
        repo = DuckDbResearchExperimentRepository(con)
        repo.ensure_tables()

        exp_id = f"{args.experiment_name}_{_now().strftime('%Y%m%dT%H%M%SZ')}"

        metrics_payload = {
            "dataset_build": dataset_result,
            "labels_build": labels_result,
            "backtest": backtest_result,
        }
        metrics_json = json.dumps(metrics_payload, sort_keys=True)

        repo.insert(
            ResearchExperimentManifest(
                experiment_id=exp_id,
                snapshot_id=args.snapshot_id,
                dataset_id=dataset_id,
                experiment_name=args.experiment_name,
                git_commit=None,
                parameters_json=_normalize_json(args.parameters_json),
                metrics_json=metrics_json,
                status="completed",
                notes=args.notes,
                created_by_pipeline="run_research_experiment_v3",
            )
        )

        print(
            json.dumps(
                {
                    "experiment_id": exp_id,
                    "dataset_id": dataset_id,
                    "metrics": metrics_payload,
                },
                indent=2,
            ),
            flush=True,
        )
        return 0

    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
