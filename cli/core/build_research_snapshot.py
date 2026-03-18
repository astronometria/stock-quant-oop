#!/usr/bin/env python3
from __future__ import annotations

"""
Create a research-grade dataset snapshot manifest.

Objectif
--------
Créer un snapshot logique de recherche reproductible, sans encore copier
physiquement toutes les tables.

Cette version est stricte:
- refuse un snapshot si une source critique est absente
- refuse un snapshot si une source critique est vide
- refuse un snapshot si une source temporelle critique n'a pas de bornes
  temporelles valides

But
---
Empêcher la production de snapshots "completed" scientifiquement douteux.
"""

import argparse
import json
import subprocess
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import duckdb

from stock_quant.infrastructure.repositories.duckdb_research_manifest_repository import (
    DuckDbResearchManifestRepository,
    ResearchDatasetInputSignature,
    ResearchDatasetManifest,
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]


@dataclass(frozen=True)
class SourceRule:
    table_name: str
    is_required: bool = True
    require_temporal_bounds: bool = True


SOURCE_RULES: list[SourceRule] = [
    SourceRule("price_history", is_required=True, require_temporal_bounds=True),
    SourceRule("sec_filing", is_required=True, require_temporal_bounds=True),
    SourceRule("finra_short_interest_history", is_required=True, require_temporal_bounds=True),
    SourceRule("daily_short_volume_history", is_required=True, require_temporal_bounds=True),
    SourceRule("short_features_daily", is_required=True, require_temporal_bounds=True),
    SourceRule("symbol_normalization", is_required=True, require_temporal_bounds=False),
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Create a research-grade dataset snapshot manifest."
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
        help="Optional logical start date for the snapshot.",
    )
    parser.add_argument(
        "--end-date",
        default=None,
        help="Optional logical end date for the snapshot.",
    )
    parser.add_argument(
        "--created-by-pipeline",
        default="build_research_snapshot",
        help="Pipeline name stored in the manifest.",
    )
    parser.add_argument(
        "--notes",
        default=None,
        help="Optional free-form notes stored in the manifest.",
    )
    parser.add_argument(
        "--parameters-json",
        default=None,
        help="Optional JSON string describing snapshot parameters.",
    )
    return parser.parse_args()


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


def _choose_date_column(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
) -> str | None:
    rows = con.execute(f"PRAGMA table_info('{table_name}')").fetchall()
    cols = {str(row[1]).strip().lower() for row in rows}

    preferred_order = [
        "date",
        "price_date",
        "trade_date",
        "settlement_date",
        "as_of_date",
        "filing_date",
        "latest_price_date",
        "timestamp",
        "datetime",
        "created_at",
        "updated_at",
    ]
    for col in preferred_order:
        if col in cols:
            return col

    # fallback défensif
    for col in cols:
        if "date" in col or "time" in col:
            return col

    return None


def _choose_symbol_column(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
) -> str | None:
    rows = con.execute(f"PRAGMA table_info('{table_name}')").fetchall()
    cols = {str(row[1]).strip().lower() for row in rows}

    preferred_order = [
        "symbol",
        "raw_symbol",
        "normalized_symbol",
    ]
    for col in preferred_order:
        if col in cols:
            return col
    return None


def _compute_table_signature(
    con: duckdb.DuckDBPyConnection,
    table_name: str,
) -> dict[str, Any]:
    if not _table_exists(con, table_name):
        return {
            "table_name": table_name,
            "table_exists": False,
            "row_count": 0,
            "min_business_date": None,
            "max_business_date": None,
            "checksum": None,
            "signature_hash": f"{table_name}|missing",
        }

    date_col = _choose_date_column(con, table_name)
    symbol_col = _choose_symbol_column(con, table_name)

    date_expr_min = f"MIN({date_col})" if date_col else "NULL"
    date_expr_max = f"MAX({date_col})" if date_col else "NULL"

    if symbol_col and date_col:
        checksum_expr = (
            f"SUM(hash(COALESCE(CAST({symbol_col} AS VARCHAR), ''), "
            f"COALESCE(CAST({date_col} AS VARCHAR), '')))"
        )
    elif symbol_col:
        checksum_expr = f"SUM(hash(COALESCE(CAST({symbol_col} AS VARCHAR), '')))"
    elif date_col:
        checksum_expr = f"SUM(hash(COALESCE(CAST({date_col} AS VARCHAR), '')))"
    else:
        checksum_expr = "NULL"

    row = con.execute(
        f"""
        SELECT
            COUNT(*) AS row_count,
            {date_expr_min} AS min_business_date,
            {date_expr_max} AS max_business_date,
            {checksum_expr} AS checksum
        FROM {table_name}
        """
    ).fetchone()

    row_count = int(row[0]) if row and row[0] is not None else 0
    min_business_date = row[1] if row else None
    max_business_date = row[2] if row else None
    checksum = row[3] if row else None

    signature_hash = (
        f"{table_name}|"
        f"{row_count}|"
        f"{min_business_date}|"
        f"{max_business_date}|"
        f"{checksum}"
    )

    return {
        "table_name": table_name,
        "table_exists": True,
        "row_count": row_count,
        "min_business_date": min_business_date,
        "max_business_date": max_business_date,
        "checksum": checksum,
        "signature_hash": signature_hash,
    }


def _snapshot_id(dataset_name: str) -> str:
    stamp = _now_utc().strftime("%Y%m%dT%H%M%SZ")
    safe_name = dataset_name.strip().lower().replace(" ", "_")
    return f"{safe_name}_{stamp}"


def _parse_parameters_json(raw: str | None) -> str | None:
    if raw is None:
        return None

    value = raw.strip()
    if not value:
        return None

    parsed = json.loads(value)
    return json.dumps(parsed, sort_keys=True)


def _validate_signature(sig: dict[str, Any], rule: SourceRule) -> list[str]:
    errors: list[str] = []

    table_name = rule.table_name
    table_exists = bool(sig.get("table_exists"))
    row_count = int(sig.get("row_count") or 0)
    min_business_date = sig.get("min_business_date")
    max_business_date = sig.get("max_business_date")

    if rule.is_required and not table_exists:
        errors.append(f"{table_name}: required table is missing")
        return errors

    if rule.is_required and row_count <= 0:
        errors.append(f"{table_name}: required table is empty")
        return errors

    if rule.require_temporal_bounds:
        if min_business_date is None or max_business_date is None:
            errors.append(f"{table_name}: temporal bounds are missing")
            return errors

        if not isinstance(min_business_date, date) or not isinstance(max_business_date, date):
            errors.append(f"{table_name}: temporal bounds are not DATE values")
            return errors

        if max_business_date < min_business_date:
            errors.append(
                f"{table_name}: invalid temporal bounds "
                f"({min_business_date} > {max_business_date})"
            )
            return errors

    return errors


def _validate_snapshot_signatures(signatures: list[dict[str, Any]]) -> None:
    by_table = {sig["table_name"]: sig for sig in signatures}
    validation_errors: list[str] = []

    for rule in SOURCE_RULES:
        sig = by_table.get(rule.table_name)
        if sig is None:
            validation_errors.append(f"{rule.table_name}: signature missing from in-memory build")
            continue
        validation_errors.extend(_validate_signature(sig, rule))

    if validation_errors:
        message = {
            "status": "failed_validation",
            "validation_errors": validation_errors,
        }
        raise RuntimeError(json.dumps(message, default=str, indent=2))


def main() -> int:
    args = parse_args()

    db_path = Path(args.db_path).expanduser().resolve()
    print(f"[build_research_snapshot] db_path={db_path}", flush=True)

    con = duckdb.connect(str(db_path))
    try:
        repo = DuckDbResearchManifestRepository(con)
        repo.ensure_tables()

        dataset_name = args.dataset_name.strip()
        snapshot_id = _snapshot_id(dataset_name)
        git_commit = _safe_git_commit(PROJECT_ROOT)
        parameters_json = _parse_parameters_json(args.parameters_json)

        signatures: list[dict[str, Any]] = []
        total_row_count = 0

        for rule in SOURCE_RULES:
            sig = _compute_table_signature(con, rule.table_name)
            signatures.append(sig)
            total_row_count += int(sig["row_count"] or 0)

        _validate_snapshot_signatures(signatures)

        manifest = ResearchDatasetManifest(
            snapshot_id=snapshot_id,
            dataset_name=dataset_name,
            git_commit=git_commit,
            parameters_json=parameters_json,
            start_date=args.start_date,
            end_date=args.end_date,
            created_by_pipeline=args.created_by_pipeline,
            source_count=len(SOURCE_RULES),
            total_row_count=total_row_count,
            status="completed",
            notes=args.notes,
        )
        repo.insert_dataset_manifest(manifest)

        for sig in signatures:
            repo.insert_dataset_input_signature(
                ResearchDatasetInputSignature(
                    snapshot_id=snapshot_id,
                    dataset_name=dataset_name,
                    source_name=sig["table_name"],
                    signature_hash=sig["signature_hash"],
                    row_count=sig["row_count"],
                    min_business_date=sig["min_business_date"],
                    max_business_date=sig["max_business_date"],
                )
            )

        output = {
            "snapshot_id": snapshot_id,
            "dataset_name": dataset_name,
            "git_commit": git_commit,
            "source_count": len(SOURCE_RULES),
            "total_row_count": total_row_count,
            "start_date": args.start_date,
            "end_date": args.end_date,
            "created_by_pipeline": args.created_by_pipeline,
            "notes": args.notes,
            "signatures": signatures,
            "validation": "passed",
        }

        print(json.dumps(output, default=str, indent=2), flush=True)
        return 0

    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
