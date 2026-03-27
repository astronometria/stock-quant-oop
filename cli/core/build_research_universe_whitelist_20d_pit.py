#!/usr/bin/env python3

from __future__ import annotations

# =============================================================================
# Build research_universe_whitelist_20d_pit
#
# Réparation ciblée:
# - plusieurs runners / trainers attendent la table
#   research_universe_whitelist_20d_pit
# - la DB actuelle ne l'a pas
# - le repo a déjà research_universe_quality_20d + universe_membership_history
#
# Cette version crée une whitelist PIT minimale, date-aware, en joignant:
# - research_universe_quality_20d
# - universe_membership_history
#
# Le script est volontairement défensif:
# - il détecte dynamiquement les colonnes disponibles
# - il supporte plusieurs conventions de statut:
#   * include_in_universe
#   * is_active
#   * membership_status
#
# Sortie:
# - une table date-aware qui conserve q.* et garantit au moins:
#   as_of_date, symbol
# =============================================================================

import argparse
import json

import duckdb
from tqdm import tqdm


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build PIT whitelist from research universe quality table.")
    parser.add_argument("--db-path", required=True, help="Path to DuckDB database file.")
    return parser.parse_args()


def table_exists(con: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    row = con.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_name = ?
        """,
        [table_name],
    ).fetchone()
    return bool(row and row[0] > 0)


def get_columns(con: duckdb.DuckDBPyConnection, table_name: str) -> set[str]:
    rows = con.execute(f"DESCRIBE {table_name}").fetchall()
    return {str(row[0]) for row in rows}


def choose_quality_date_column(columns: set[str]) -> str:
    for candidate in ["as_of_date", "bar_date", "price_date"]:
        if candidate in columns:
            return candidate
    raise RuntimeError(
        "research_universe_quality_20d must contain one of: as_of_date, bar_date, price_date"
    )


def choose_membership_predicate(columns: set[str]) -> str:
    """
    Construit le prédicat PIT le plus compatible avec le schéma réellement présent.
    """
    if "include_in_universe" in columns:
        return "COALESCE(include_in_universe, FALSE) = TRUE"

    if "is_active" in columns:
        return "COALESCE(is_active, FALSE) = TRUE"

    if "membership_status" in columns:
        return "UPPER(COALESCE(membership_status, '')) IN ('ACTIVE', 'INCLUDED', 'MEMBER', 'TRUE', 'YES')"

    # Fallback permissif si aucun flag explicite n'existe.
    return "TRUE"


def choose_effective_from(columns: set[str]) -> str:
    if "effective_from" in columns:
        return "effective_from"
    if "start_date" in columns:
        return "start_date"
    return "DATE '1900-01-01'"


def choose_effective_to(columns: set[str]) -> str:
    if "effective_to" in columns:
        return "effective_to"
    if "end_date" in columns:
        return "end_date"
    return "DATE '2999-12-31'"


def main() -> int:
    args = parse_args()
    con = duckdb.connect(args.db_path)

    steps = tqdm(total=5, desc="build_whitelist_20d_pit", unit="step")

    try:
        steps.set_description("build_whitelist_20d_pit:validate_tables")

        required = ["research_universe_quality_20d", "universe_membership_history"]
        for table_name in required:
            if not table_exists(con, table_name):
                raise RuntimeError(f"Missing required table: {table_name}")

        q_cols = get_columns(con, "research_universe_quality_20d")
        m_cols = get_columns(con, "universe_membership_history")

        q_date_col = choose_quality_date_column(q_cols)
        membership_predicate = choose_membership_predicate(m_cols)
        effective_from_expr = choose_effective_from(m_cols)
        effective_to_expr = choose_effective_to(m_cols)

        steps.update(1)

        steps.set_description("build_whitelist_20d_pit:build_sql")

        # On conserve toutes les colonnes de la table qualité.
        # On force cependant:
        # - symbol normalisé en uppercase trim
        # - as_of_date canonique
        sql = f"""
        CREATE OR REPLACE TABLE research_universe_whitelist_20d_pit AS
        WITH q AS (
            SELECT
                CAST({q_date_col} AS DATE) AS as_of_date,
                UPPER(TRIM(symbol)) AS symbol,
                *
            FROM research_universe_quality_20d
            WHERE symbol IS NOT NULL
        ),
        m AS (
            SELECT
                UPPER(TRIM(symbol)) AS symbol,
                CAST(COALESCE({effective_from_expr}, DATE '1900-01-01') AS DATE) AS effective_from,
                CAST(COALESCE({effective_to_expr}, DATE '2999-12-31') AS DATE) AS effective_to,
                *
            FROM universe_membership_history
            WHERE symbol IS NOT NULL
              AND {membership_predicate}
        )
        SELECT DISTINCT
            q.*
        FROM q
        INNER JOIN m
            ON q.symbol = m.symbol
           AND q.as_of_date >= m.effective_from
           AND q.as_of_date <  m.effective_to
        """
        steps.update(1)

        steps.set_description("build_whitelist_20d_pit:execute")
        con.execute("DROP TABLE IF EXISTS research_universe_whitelist_20d_pit")
        con.execute(sql)
        steps.update(1)

        steps.set_description("build_whitelist_20d_pit:metrics")
        metrics_row = con.execute(
            """
            SELECT
                COUNT(*) AS rows_total,
                COUNT(DISTINCT symbol) AS symbols_total,
                MIN(as_of_date) AS min_date,
                MAX(as_of_date) AS max_date
            FROM research_universe_whitelist_20d_pit
            """
        ).fetchone()
        steps.update(1)

        steps.set_description("build_whitelist_20d_pit:done")
        result = {
            "status": "SUCCESS",
            "table_name": "research_universe_whitelist_20d_pit",
            "rows_total": int(metrics_row[0]),
            "symbols_total": int(metrics_row[1]),
            "min_date": str(metrics_row[2]) if metrics_row[2] is not None else None,
            "max_date": str(metrics_row[3]) if metrics_row[3] is not None else None,
            "quality_date_column": q_date_col,
            "membership_predicate": membership_predicate,
            "effective_from_expr": effective_from_expr,
            "effective_to_expr": effective_to_expr,
        }
        print(json.dumps(result, indent=2))
        steps.update(1)

        steps.close()
        return 0

    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
