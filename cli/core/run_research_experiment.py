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
    """
    Extrait le dernier objet JSON complet présent dans stdout.

    Pourquoi cette version:
    - les sous-scripts peuvent écrire des logs avant le JSON final
    - un JSON final peut contenir des sous-objets imbriqués
    - prendre simplement "la dernière ligne commençant par {" n'est pas robuste
    """
    text = stdout.strip()
    if not text:
        raise RuntimeError("empty stdout, expected trailing JSON object")

    end = text.rfind("}")
    if end == -1:
        raise RuntimeError(f"no JSON object found in stdout:\n{stdout}")

    depth = 0
    in_string = False
    escape = False

    for i in range(end, -1, -1):
        ch = text[i]

        if in_string:
            if escape:
                escape = False
            elif ch == "\\":
                escape = True
            elif ch == '"':
                in_string = False
            continue

        if ch == '"':
            in_string = True
            continue

        if ch == "}":
            depth += 1
            continue

        if ch == "{":
            depth -= 1
            if depth == 0:
                candidate = text[i:end + 1]
                try:
                    return json.loads(candidate)
                except json.JSONDecodeError as exc:
                    raise RuntimeError(
                        f"failed to parse trailing JSON object from stdout:\n{stdout}"
                    ) from exc

    raise RuntimeError(f"no complete trailing JSON object found in stdout:\n{stdout}")


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
    return json.dumps(json.loads(value), sort_keys=True)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--db-path", required=True)
    p.add_argument("--snapshot-id", required=True)
    p.add_argument("--split-id", required=True)
    p.add_argument("--experiment-name", default="exp_v_scientific")
    p.add_argument("--parameters-json", default=None)
    p.add_argument("--notes", default=None)
    p.add_argument("--transaction-cost-bps", type=float, default=10.0)
    p.add_argument("--signal-threshold", type=float, default=0.5)
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
        "--split-id", args.split_id,
    ])
    dataset_id = dataset_result["dataset_id"]

    labels_result = _run([
        sys.executable,
        str(PROJECT_ROOT / "cli/core/build_research_labels.py"),
        "--db-path", str(db),
        "--snapshot-id", args.snapshot_id,
        "--dataset-id", dataset_id,
        "--split-id", args.split_id,
    ])

    backtest_result = _run([
        sys.executable,
        str(PROJECT_ROOT / "cli/core/build_research_backtest.py"),
        "--db-path", str(db),
        "--dataset-id", dataset_id,
        "--split-id", args.split_id,
        "--transaction-cost-bps", str(args.transaction_cost_bps),
        "--signal-threshold", str(args.signal_threshold),
    ])

    con = duckdb.connect(str(db))
    try:
        repo = DuckDbResearchExperimentRepository(con)
        repo.ensure_tables()

        exp_id = f"{args.experiment_name}_{_now().strftime('%Y%m%dT%H%M%SZ')}"
        metrics_payload = {
            "split_id": args.split_id,
            "dataset_build": dataset_result,
            "labels_build": labels_result,
            "backtest": backtest_result,
        }

        repo.insert(
            ResearchExperimentManifest(
                experiment_id=exp_id,
                snapshot_id=args.snapshot_id,
                dataset_id=dataset_id,
                experiment_name=args.experiment_name,
                git_commit=None,
                parameters_json=_normalize_json(args.parameters_json),
                metrics_json=json.dumps(metrics_payload, sort_keys=True),
                status="completed",
                notes=args.notes,
                created_by_pipeline="run_research_experiment_scientific",
            )
        )

        print(json.dumps({
            "experiment_id": exp_id,
            "dataset_id": dataset_id,
            "split_id": args.split_id,
            "metrics": metrics_payload,
        }, indent=2), flush=True)
        return 0

    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
