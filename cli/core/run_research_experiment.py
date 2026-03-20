#!/usr/bin/env python3
from __future__ import annotations

"""
Scientific research experiment runner.

Objectif
--------
Orchestrer, dans l'ordre:
1) build_research_training_dataset.py
2) build_research_labels.py
3) build_research_backtest.py
4) persister le manifest d'expérience

Améliorations de cette version
------------------------------
- streaming temps réel du stdout/stderr des sous-scripts
- passthrough runtime:
  --verbose
  --memory-limit
  --threads
  --temp-dir
- conservation d'un parse robuste du JSON final produit par chaque sous-script
- logs explicites pour mieux observer les runs lourds
- propagation du nouveau contrat de signal:
  --signal-name
  --signal-params-json
  --execution-lag-bars

Important
---------
On garde une architecture simple:
- Python mince pour l'orchestration
- sous-scripts responsables du SQL
- manifest final écrit seulement si toutes les étapes ont réussi
"""

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
    """Timestamp UTC pour ids reproductibles."""
    return datetime.now(timezone.utc)


def _extract_last_json_object(stdout: str) -> dict[str, Any]:
    """
    Extrait le dernier objet JSON complet présent dans stdout.
    """
    text = stdout.strip()
    if not text:
        raise RuntimeError("empty stdout, expected trailing JSON object")

    end = text.rfind("}")
    if end == -1:
        raise RuntimeError(f"no JSON object found in stdout:\n{text}")

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
                candidate = text[i : end + 1]
                try:
                    return json.loads(candidate)
                except json.JSONDecodeError as exc:
                    raise RuntimeError(
                        f"failed to parse trailing JSON object from stdout:\n{text}"
                    ) from exc

    raise RuntimeError(f"no complete trailing JSON object found in stdout:\n{text}")


def _normalize_json(raw: str | None) -> str | None:
    """
    Normalise un payload JSON optionnel pour le manifest.
    """
    if raw is None:
        return None

    value = raw.strip()
    if not value:
        return None

    return json.dumps(json.loads(value), sort_keys=True)


def _normalize_signal_params_json(
    raw: str | None,
    *,
    signal_name: str,
    signal_threshold: float,
) -> str | None:
    """
    Normalise les paramètres de signal.

    Compatibilité:
    - si l'utilisateur fournit --signal-params-json, on le valide et on le normalise
    - sinon, on fabrique un JSON stable à partir de --signal-threshold
      pour le signal par défaut short_volume_ratio_threshold
    """
    if raw is None or not raw.strip():
        if signal_name == "short_volume_ratio_threshold":
            return json.dumps(
                {
                    "feature_name": "short_volume_ratio",
                    "threshold": float(signal_threshold),
                    "zero_below_threshold": True,
                },
                sort_keys=True,
            )
        return None

    return json.dumps(json.loads(raw), sort_keys=True)


def _build_base_child_cmd(script_rel_path: str, args: argparse.Namespace, db: Path) -> list[str]:
    """
    Construit la base commune d'une commande enfant.
    """
    cmd = [
        sys.executable,
        str(PROJECT_ROOT / script_rel_path),
        "--db-path",
        str(db),
        "--memory-limit",
        args.memory_limit,
        "--threads",
        str(args.threads),
        "--temp-dir",
        args.temp_dir,
    ]

    if args.verbose:
        cmd.append("--verbose")

    return cmd


def _run(cmd: list[str], step_name: str) -> dict[str, Any]:
    """
    Exécute un sous-script en streaming.
    """
    print(f"[run_research_experiment] START step={step_name}", flush=True)
    print(
        f"[run_research_experiment] CMD step={step_name} cmd={json.dumps(cmd)}",
        flush=True,
    )

    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )

    if process.stdout is None:
        raise RuntimeError(f"{step_name}: failed to capture child stdout")

    collected_lines: list[str] = []
    for line in process.stdout:
        print(line, end="", flush=True)
        collected_lines.append(line)

    returncode = process.wait()
    stdout = "".join(collected_lines)
    if returncode != 0:
        raise RuntimeError(f"{step_name} failed:\n{stdout}")

    payload = _extract_last_json_object(stdout)
    print(f"[run_research_experiment] END step={step_name}", flush=True)
    return payload


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()

    p.add_argument("--db-path", required=True)
    p.add_argument("--snapshot-id", required=True)
    p.add_argument("--split-id", required=True)

    p.add_argument("--experiment-name", default="exp_v_scientific")
    p.add_argument("--parameters-json", default=None)
    p.add_argument("--notes", default=None)

    p.add_argument("--transaction-cost-bps", type=float, default=10.0)

    # Nouveau contrat de signal.
    p.add_argument("--signal-name", default="short_volume_ratio_threshold")
    p.add_argument("--signal-params-json", default=None)
    p.add_argument("--execution-lag-bars", type=int, default=1)

    # Compat temporaire.
    p.add_argument("--signal-threshold", type=float, default=0.5)

    # Runtime passthrough pour les gros jobs.
    p.add_argument("--memory-limit", default="24GB")
    p.add_argument("--threads", type=int, default=6)
    p.add_argument("--temp-dir", default="/home/marty/stock-quant-oop/tmp")
    p.add_argument("--verbose", action="store_true")

    return p.parse_args()


def main() -> int:
    args = parse_args()
    db = Path(args.db_path).expanduser().resolve()

    normalized_signal_params_json = _normalize_signal_params_json(
        args.signal_params_json,
        signal_name=args.signal_name,
        signal_threshold=args.signal_threshold,
    )

    print(f"[run_research_experiment] db_path={db}", flush=True)
    print(f"[run_research_experiment] snapshot_id={args.snapshot_id}", flush=True)
    print(f"[run_research_experiment] split_id={args.split_id}", flush=True)
    print(f"[run_research_experiment] signal_name={args.signal_name}", flush=True)
    print(f"[run_research_experiment] signal_params_json={normalized_signal_params_json}", flush=True)
    print(f"[run_research_experiment] execution_lag_bars={args.execution_lag_bars}", flush=True)
    print(f"[run_research_experiment] memory_limit={args.memory_limit}", flush=True)
    print(f"[run_research_experiment] threads={args.threads}", flush=True)
    print(f"[run_research_experiment] temp_dir={args.temp_dir}", flush=True)
    print(f"[run_research_experiment] verbose={args.verbose}", flush=True)

    con = duckdb.connect(str(db))
    try:
        repo = DuckDbResearchExperimentRepository(con)
        repo.ensure_tables()
    finally:
        con.close()

    dataset_cmd = _build_base_child_cmd(
        "cli/core/build_research_training_dataset.py",
        args,
        db,
    ) + [
        "--snapshot-id",
        args.snapshot_id,
        "--split-id",
        args.split_id,
    ]

    dataset_result = _run(dataset_cmd, "dataset_build")
    dataset_id = dataset_result["dataset_id"]

    labels_cmd = _build_base_child_cmd(
        "cli/core/build_research_labels.py",
        args,
        db,
    ) + [
        "--snapshot-id",
        args.snapshot_id,
        "--dataset-id",
        dataset_id,
        "--split-id",
        args.split_id,
    ]

    labels_result = _run(labels_cmd, "labels_build")

    backtest_cmd = _build_base_child_cmd(
        "cli/core/build_research_backtest.py",
        args,
        db,
    ) + [
        "--dataset-id",
        dataset_id,
        "--split-id",
        args.split_id,
        "--transaction-cost-bps",
        str(args.transaction_cost_bps),
        "--signal-name",
        args.signal_name,
        "--execution-lag-bars",
        str(args.execution_lag_bars),
    ]

    if normalized_signal_params_json is not None:
        backtest_cmd.extend(["--signal-params-json", normalized_signal_params_json])
    else:
        backtest_cmd.extend(["--signal-threshold", str(args.signal_threshold)])

    backtest_result = _run(backtest_cmd, "backtest_build")

    con = duckdb.connect(str(db))
    try:
        repo = DuckDbResearchExperimentRepository(con)
        repo.ensure_tables()

        exp_id = f"{args.experiment_name}_{_now().strftime('%Y%m%dT%H%M%SZ')}"

        metrics_payload = {
            "split_id": args.split_id,
            "signal_name": args.signal_name,
            "signal_params_json": normalized_signal_params_json,
            "execution_lag_bars": args.execution_lag_bars,
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

        print(
            json.dumps(
                {
                    "experiment_id": exp_id,
                    "dataset_id": dataset_id,
                    "split_id": args.split_id,
                    "signal_name": args.signal_name,
                    "signal_params_json": normalized_signal_params_json,
                    "execution_lag_bars": args.execution_lag_bars,
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
