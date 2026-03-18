#!/usr/bin/env python3
from __future__ import annotations

"""
Research-grade snapshot orchestration entrypoint.

Objectif
--------
Fournir un point d'entrée unique, simple et reproductible pour créer
un snapshot de recherche à partir de la base DuckDB courante.

Philosophie
-----------
- Python mince
- orchestration seulement
- validation lourde déléguée à build_research_snapshot.py
- sortie JSON claire pour logs, CI, cron et diagnostics

Ce script:
- vérifie un minimum d'état source avant de lancer le snapshot
- invoque le builder strict
- renvoie un résumé exploitable
"""

import argparse
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb


PROJECT_ROOT = Path(__file__).resolve().parents[2]
SNAPSHOT_BUILDER = PROJECT_ROOT / "cli/core/build_research_snapshot.py"


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run a research-grade snapshot build."
    )
    parser.add_argument(
        "--db-path",
        required=True,
        help="Path to DuckDB database file.",
    )
    parser.add_argument(
        "--dataset-name",
        default="research_snapshot_short_equities",
        help="Logical dataset name.",
    )
    parser.add_argument(
        "--start-date",
        default=None,
        help="Optional logical start date.",
    )
    parser.add_argument(
        "--end-date",
        default=None,
        help="Optional logical end date.",
    )
    parser.add_argument(
        "--notes",
        default=None,
        help="Optional notes written into the snapshot manifest.",
    )
    parser.add_argument(
        "--parameters-json",
        default=None,
        help="Optional JSON string for experiment parameters.",
    )
    parser.add_argument(
        "--created-by-pipeline",
        default="run_research_snapshot",
        help="Pipeline name persisted in the manifest.",
    )
    parser.add_argument(
        "--fail-on-empty-price-history",
        action="store_true",
        help="Fail early if price_history is empty before invoking the strict builder.",
    )
    return parser.parse_args()


def _table_exists(con: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    row = con.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE lower(table_name) = lower(?)
        """,
        [table_name],
    ).fetchone()
    return bool(row and int(row[0]) > 0)


def _safe_count(con: duckdb.DuckDBPyConnection, table_name: str) -> int | None:
    if not _table_exists(con, table_name):
        return None
    row = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
    if not row:
        return 0
    return int(row[0] or 0)


def _probe_state(db_path: Path) -> dict[str, Any]:
    """
    Probe léger, informatif uniquement.

    Le builder strict reste la source de vérité.
    """
    con = duckdb.connect(str(db_path))
    try:
        return {
            "price_history_count": _safe_count(con, "price_history"),
            "sec_filing_count": _safe_count(con, "sec_filing"),
            "finra_short_interest_history_count": _safe_count(con, "finra_short_interest_history"),
            "daily_short_volume_history_count": _safe_count(con, "daily_short_volume_history"),
            "short_features_daily_count": _safe_count(con, "short_features_daily"),
            "symbol_normalization_count": _safe_count(con, "symbol_normalization"),
        }
    finally:
        con.close()


def _run_snapshot_builder(args: argparse.Namespace, db_path: Path) -> subprocess.CompletedProcess[str]:
    cmd = [
        sys.executable,
        str(SNAPSHOT_BUILDER),
        "--db-path",
        str(db_path),
        "--dataset-name",
        args.dataset_name,
        "--created-by-pipeline",
        args.created_by_pipeline,
    ]

    if args.start_date:
        cmd.extend(["--start-date", args.start_date])

    if args.end_date:
        cmd.extend(["--end-date", args.end_date])

    if args.notes:
        cmd.extend(["--notes", args.notes])

    if args.parameters_json:
        cmd.extend(["--parameters-json", args.parameters_json])

    return subprocess.run(
        cmd,
        cwd=str(PROJECT_ROOT),
        capture_output=True,
        text=True,
    )


def main() -> int:
    args = parse_args()
    started_at = _now_utc()
    db_path = Path(args.db_path).expanduser().resolve()

    print(f"[run_research_snapshot] db_path={db_path}", flush=True)

    preflight = _probe_state(db_path)

    if args.fail_on_empty_price_history and (preflight["price_history_count"] or 0) <= 0:
        output = {
            "pipeline_name": "run_research_snapshot",
            "status": "failed",
            "started_at": started_at,
            "finished_at": _now_utc(),
            "error_message": "price_history is empty during preflight",
            "preflight": preflight,
        }
        print(json.dumps(output, default=str, indent=2), flush=True)
        return 1

    result = _run_snapshot_builder(args, db_path)
    finished_at = _now_utc()

    output = {
        "pipeline_name": "run_research_snapshot",
        "status": "success" if result.returncode == 0 else "failed",
        "started_at": started_at,
        "finished_at": finished_at,
        "builder_return_code": result.returncode,
        "preflight": preflight,
        "builder_stdout": result.stdout.strip(),
        "builder_stderr": result.stderr.strip(),
    }

    print(json.dumps(output, default=str, indent=2), flush=True)
    return result.returncode


if __name__ == "__main__":
    raise SystemExit(main())
