#!/usr/bin/env python3
from __future__ import annotations

"""
run_core_pipeline.py

Version non destructive, sans fixtures, sans dépendance à SchemaManager.

But:
- reconstruire price_source_daily_raw depuis les tables raw déjà présentes
- reconstruire price_history depuis price_source_daily_raw
- ne jamais reset globalement la DB
- rester simple, lisible et robuste
"""

import argparse
import json
from datetime import datetime
from pathlib import Path

import duckdb

from stock_quant.prices.canonical.build_price_source_daily_raw import (
    build_price_source_daily_raw,
)
from stock_quant.prices.canonical.build_price_history_compat import (
    build_price_history_compat,
)


def parse_args() -> argparse.Namespace:
    """
    Parse des arguments CLI.

    On reste volontairement minimal pour éviter toute logique secondaire
    non nécessaire au rebuild canonique des tables prix.
    """
    parser = argparse.ArgumentParser(
        description="Safe core rebuild from existing raw price tables."
    )
    parser.add_argument(
        "--db-path",
        required=True,
        help="Path to DuckDB database file.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose output.",
    )
    return parser.parse_args()


def table_exists(connection: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    """
    Vérifie si une table existe dans un schéma utilisateur.
    """
    row = connection.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
          AND table_name = ?
        """,
        [table_name],
    ).fetchone()
    return bool(row and row[0] > 0)


def safe_scalar(connection: duckdb.DuckDBPyConnection, sql: str):
    """
    Exécute un SELECT scalaire sans casser le script si la table n'existe pas.
    """
    try:
        row = connection.execute(sql).fetchone()
        return row[0] if row else None
    except Exception:
        return None


def safe_triplet(connection: duckdb.DuckDBPyConnection, sql: str):
    """
    Exécute un SELECT de type (count, min_date, max_date) si possible.
    """
    try:
        row = connection.execute(sql).fetchone()
        return row if row else None
    except Exception:
        return None


def main() -> int:
    args = parse_args()

    db_path = Path(args.db_path).expanduser().resolve()
    started_at = datetime.utcnow()

    connection = duckdb.connect(str(db_path))
    try:
        # ==========================================================
        # PREFLIGHT
        # ==========================================================
        required_raw_candidates = [
            "price_source_daily_raw_all",
            "price_source_daily_raw_yahoo",
        ]

        existing_raw_candidates = [
            table_name
            for table_name in required_raw_candidates
            if table_exists(connection, table_name)
        ]

        if not existing_raw_candidates:
            raise RuntimeError(
                "No raw price source table found. "
                "Expected at least one of: price_source_daily_raw_all, price_source_daily_raw_yahoo"
            )

        preflight = {
            "db_path": str(db_path),
            "started_at": started_at.isoformat(),
            "existing_raw_candidates": existing_raw_candidates,
            "table_counts_before": {
                "price_source_daily_raw_all": safe_scalar(
                    connection, "SELECT COUNT(*) FROM price_source_daily_raw_all"
                ),
                "price_source_daily_raw_yahoo": safe_scalar(
                    connection, "SELECT COUNT(*) FROM price_source_daily_raw_yahoo"
                ),
                "price_source_daily_raw": safe_scalar(
                    connection, "SELECT COUNT(*) FROM price_source_daily_raw"
                ),
                "price_history": safe_scalar(
                    connection, "SELECT COUNT(*) FROM price_history"
                ),
            },
            "ranges_before": {
                "price_source_daily_raw_all": safe_triplet(
                    connection,
                    "SELECT COUNT(*), MIN(price_date), MAX(price_date) FROM price_source_daily_raw_all",
                ),
                "price_source_daily_raw_yahoo": safe_triplet(
                    connection,
                    "SELECT COUNT(*), MIN(price_date), MAX(price_date) FROM price_source_daily_raw_yahoo",
                ),
                "price_source_daily_raw": safe_triplet(
                    connection,
                    "SELECT COUNT(*), MIN(price_date), MAX(price_date) FROM price_source_daily_raw",
                ),
                "price_history": safe_triplet(
                    connection,
                    "SELECT COUNT(*), MIN(price_date), MAX(price_date) FROM price_history",
                ),
            },
        }

        if args.verbose:
            print("===== PREFLIGHT =====")
            print(json.dumps(preflight, indent=2, default=str))
            print()

        # ==========================================================
        # STEP 1: BUILD price_source_daily_raw
        # ==========================================================
        print("===== STEP 1: BUILD price_source_daily_raw =====", flush=True)
        price_raw_metrics = build_price_source_daily_raw(connection)
        print(json.dumps(price_raw_metrics, indent=2, default=str), flush=True)
        print(flush=True)

        # ==========================================================
        # STEP 2: BUILD price_history
        # ==========================================================
        print("===== STEP 2: BUILD price_history =====", flush=True)
        price_history_metrics = build_price_history_compat(connection)
        print(json.dumps(price_history_metrics, indent=2, default=str), flush=True)
        print(flush=True)

        summary = {
            "status": "success",
            "started_at": started_at.isoformat(),
            "finished_at": datetime.utcnow().isoformat(),
            "db_path": str(db_path),
            "notes": [
                "No SchemaManager dependency.",
                "No fixture injection.",
                "No destructive global reset.",
                "Rebuilt from existing raw tables only.",
            ],
            "before": preflight["table_counts_before"],
            "after": {
                "price_source_daily_raw": safe_triplet(
                    connection,
                    "SELECT COUNT(*), MIN(price_date), MAX(price_date) FROM price_source_daily_raw",
                ),
                "price_history": safe_triplet(
                    connection,
                    "SELECT COUNT(*), MIN(price_date), MAX(price_date) FROM price_history",
                ),
            },
            "price_source_daily_raw_metrics": price_raw_metrics,
            "price_history_metrics": price_history_metrics,
        }

        print("===== CORE PIPELINE DONE =====", flush=True)
        print(json.dumps(summary, indent=2, default=str), flush=True)
        return 0

    finally:
        connection.close()


if __name__ == "__main__":
    raise SystemExit(main())
