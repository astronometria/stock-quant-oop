#!/usr/bin/env python3
from __future__ import annotations

"""
Create a research-grade dataset snapshot manifest.

Objectif
--------
Créer un snapshot logique de recherche reproductible, sans encore copier
physiquement toutes les tables.

Ce script fige:
- un snapshot_id
- le commit git courant
- la période logique
- les signatures d'entrée utilisées
- les métadonnées de build

Important
---------
Cette première version est volontairement mince:
- elle écrit le manifest
- elle écrit les signatures d'entrée
- elle ne duplique pas encore les grosses tables

C'est suffisant pour:
- tracer les expériences
- relier une recherche à un état de données
- préparer la future couche "dataset materialization"
"""

import argparse
import json
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb

from stock_quant.infrastructure.repositories.duckdb_research_manifest_repository import (
    DuckDbResearchManifestRepository,
    ResearchDatasetInputSignature,
    ResearchDatasetManifest,
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]


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
    """
    Lit le commit git courant.

    On reste robuste:
    - si git échoue, on retourne None
    - pas d'exception bloquante juste pour la métadonnée
    """
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
        "trade_date",
        "settlement_date",
        "as_of_date",
        "date",
        "filing_date",
        "latest_price_date",
    ]
    for col in preferred_order:
        if col in cols:
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
    """
    Même logique que dans run_daily_pipeline.py:
    signature SQL-first simple, robuste et lisible.
    """
    if not _table_exists(con, table_name):
        return {
            "table_name": table_name,
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
        "row_count": row_count,
        "min_business_date": min_business_date,
        "max_business_date": max_business_date,
        "checksum": checksum,
        "signature_hash": signature_hash,
    }


def _snapshot_id(dataset_name: str) -> str:
    """
    Génère un id stable lisible humain.
    """
    stamp = _now_utc().strftime("%Y%m%dT%H%M%SZ")
    safe_name = dataset_name.strip().lower().replace(" ", "_")
    return f"{safe_name}_{stamp}"


def _parse_parameters_json(raw: str | None) -> str | None:
    """
    Valide légèrement le JSON fourni.

    On retourne une chaîne JSON normalisée si possible.
    """
    if raw is None:
        return None

    value = raw.strip()
    if not value:
        return None

    parsed = json.loads(value)
    return json.dumps(parsed, sort_keys=True)


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

        # Sources minimales du domaine research actuel.
        # On part des tables canoniques qui alimentent réellement la recherche.
        source_tables = [
            "price_history",
            "sec_filing",
            "finra_short_interest_history",
            "daily_short_volume_history",
            "short_features_daily",
            "symbol_normalization",
        ]

        signatures: list[dict[str, Any]] = []
        total_row_count = 0

        for table_name in source_tables:
            sig = _compute_table_signature(con, table_name)
            signatures.append(sig)
            total_row_count += int(sig["row_count"] or 0)

        manifest = ResearchDatasetManifest(
            snapshot_id=snapshot_id,
            dataset_name=dataset_name,
            git_commit=git_commit,
            parameters_json=parameters_json,
            start_date=args.start_date,
            end_date=args.end_date,
            created_by_pipeline=args.created_by_pipeline,
            source_count=len(source_tables),
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
            "source_count": len(source_tables),
            "total_row_count": total_row_count,
            "start_date": args.start_date,
            "end_date": args.end_date,
            "created_by_pipeline": args.created_by_pipeline,
            "notes": args.notes,
            "signatures": signatures,
        }

        print(json.dumps(output, default=str, indent=2), flush=True)
        return 0

    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
