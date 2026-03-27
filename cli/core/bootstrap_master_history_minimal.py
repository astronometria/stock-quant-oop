#!/usr/bin/env python3

from __future__ import annotations

# =============================================================================
# Bootstrap minimal des tables master/history manquantes.
#
# Objectif:
# - débloquer le repo quand instrument_master / universe_membership_history sont vides
# - rester SQL-first
# - être explicite sur le fait qu'il s'agit d'un fallback pragmatique,
#   et non d'une reconstruction PIT institutionnelle complète
#
# Ce script:
# 1) peuple instrument_master si vide
#    - source principale: price_bars_adjusted
#    - enrichissement facultatif depuis symbol_reference si disponible
# 2) peuple universe_membership_history si vide
#    - source: research_universe_quality_20d
#    - effective_from = première date vue dans research_universe_quality_20d
#    - effective_to = NULL
#    - membership_status = 'ACTIVE'
#
# Hypothèse importante:
# - ce bootstrap crée une pseudo-history cohérente pour débloquer les pipelines,
#   mais ne remplace pas une vraie historisation listing/ticker/survivorship.
# =============================================================================

import argparse
import json
from pathlib import Path

import duckdb
from tqdm import tqdm


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Bootstrap minimal instrument_master and universe_membership_history.")
    p.add_argument("--db-path", required=True, help="Path to DuckDB database.")
    return p.parse_args()


def table_exists(con: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    row = con.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_name = ?
        """,
        [table_name],
    ).fetchone()
    return bool(row and int(row[0]) > 0)


def table_count(con: duckdb.DuckDBPyConnection, table_name: str) -> int:
    return int(con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0])


def bootstrap_instrument_master(con: duckdb.DuckDBPyConnection) -> dict:
    if not table_exists(con, "instrument_master"):
        raise RuntimeError("Missing required table: instrument_master")
    if not table_exists(con, "price_bars_adjusted"):
        raise RuntimeError("Missing required table: price_bars_adjusted")

    before = table_count(con, "instrument_master")
    if before > 0:
        return {
            "skipped": True,
            "reason": "instrument_master already has rows",
            "rows_before": before,
            "rows_after": before,
        }

    has_symbol_reference = table_exists(con, "symbol_reference")
    has_company_master = table_exists(con, "company_master")

    # -------------------------------------------------------------------------
    # Strategy:
    # - one row per symbol from price_bars_adjusted
    # - instrument_id defaults to symbol
    # - company_id enriched from symbol_reference when available
    # - name/issuer enrichment left minimal because downstream mostly needs keys
    # -------------------------------------------------------------------------
    if has_symbol_reference:
        sql = """
        INSERT INTO instrument_master (
            instrument_id,
            company_id,
            symbol,
            issuer_name,
            security_type,
            primary_exchange,
            is_active,
            created_at,
            updated_at
        )
        WITH symbols AS (
            SELECT DISTINCT
                UPPER(TRIM(symbol)) AS symbol
            FROM price_bars_adjusted
            WHERE symbol IS NOT NULL
        ),
        ref AS (
            SELECT
                UPPER(TRIM(symbol)) AS symbol,
                company_id
            FROM symbol_reference
            WHERE symbol IS NOT NULL
        )
        SELECT
            s.symbol AS instrument_id,
            COALESCE(r.company_id, s.symbol) AS company_id,
            s.symbol,
            CAST(NULL AS VARCHAR) AS issuer_name,
            'COMMON_STOCK' AS security_type,
            CAST(NULL AS VARCHAR) AS primary_exchange,
            TRUE AS is_active,
            CURRENT_TIMESTAMP AS created_at,
            CURRENT_TIMESTAMP AS updated_at
        FROM symbols s
        LEFT JOIN ref r
            ON s.symbol = r.symbol
        ORDER BY s.symbol
        """
    else:
        sql = """
        INSERT INTO instrument_master (
            instrument_id,
            company_id,
            symbol,
            issuer_name,
            security_type,
            primary_exchange,
            is_active,
            created_at,
            updated_at
        )
        SELECT DISTINCT
            UPPER(TRIM(symbol)) AS instrument_id,
            UPPER(TRIM(symbol)) AS company_id,
            UPPER(TRIM(symbol)) AS symbol,
            CAST(NULL AS VARCHAR) AS issuer_name,
            'COMMON_STOCK' AS security_type,
            CAST(NULL AS VARCHAR) AS primary_exchange,
            TRUE AS is_active,
            CURRENT_TIMESTAMP AS created_at,
            CURRENT_TIMESTAMP AS updated_at
        FROM price_bars_adjusted
        WHERE symbol IS NOT NULL
        ORDER BY UPPER(TRIM(symbol))
        """
    con.execute(sql)

    after = table_count(con, "instrument_master")
    return {
        "skipped": False,
        "rows_before": before,
        "rows_after": after,
        "rows_inserted": after - before,
        "used_symbol_reference": has_symbol_reference,
        "used_company_master": has_company_master,
    }


def bootstrap_universe_membership_history(con: duckdb.DuckDBPyConnection) -> dict:
    if not table_exists(con, "universe_membership_history"):
        raise RuntimeError("Missing required table: universe_membership_history")
    if not table_exists(con, "research_universe_quality_20d"):
        raise RuntimeError("Missing required table: research_universe_quality_20d")

    before = table_count(con, "universe_membership_history")
    if before > 0:
        return {
            "skipped": True,
            "reason": "universe_membership_history already has rows",
            "rows_before": before,
            "rows_after": before,
        }

    con.execute("""
        INSERT INTO universe_membership_history (
            instrument_id,
            company_id,
            symbol,
            universe_name,
            effective_from,
            effective_to,
            membership_status,
            reason,
            source_name,
            created_at
        )
        WITH q AS (
            SELECT
                UPPER(TRIM(symbol)) AS symbol,
                MIN(as_of_date) AS effective_from
            FROM research_universe_quality_20d
            WHERE symbol IS NOT NULL
            GROUP BY UPPER(TRIM(symbol))
        ),
        i AS (
            SELECT
                UPPER(TRIM(symbol)) AS symbol,
                instrument_id,
                company_id
            FROM instrument_master
            WHERE symbol IS NOT NULL
        )
        SELECT
            COALESCE(i.instrument_id, q.symbol) AS instrument_id,
            COALESCE(i.company_id, q.symbol) AS company_id,
            q.symbol,
            'research_universe_quality_20d_bootstrap' AS universe_name,
            q.effective_from,
            NULL AS effective_to,
            'ACTIVE' AS membership_status,
            'bootstrap_from_research_universe_quality_20d' AS reason,
            'bootstrap_master_history_minimal.py' AS source_name,
            CURRENT_TIMESTAMP AS created_at
        FROM q
        LEFT JOIN i
            ON q.symbol = i.symbol
        ORDER BY q.symbol
    """)

    after = table_count(con, "universe_membership_history")
    return {
        "skipped": False,
        "rows_before": before,
        "rows_after": after,
        "rows_inserted": after - before,
    }


def main() -> int:
    args = parse_args()
    db_path = Path(args.db_path).expanduser().resolve()

    con = duckdb.connect(str(db_path))
    steps = tqdm(total=4, desc="bootstrap_master_history", unit="step")

    try:
        steps.set_description("bootstrap_master_history:validate")
        required = [
            "instrument_master",
            "universe_membership_history",
            "price_bars_adjusted",
            "research_universe_quality_20d",
        ]
        missing = [t for t in required if not table_exists(con, t)]
        if missing:
            raise RuntimeError(f"Missing required tables: {missing}")
        steps.update(1)

        steps.set_description("bootstrap_master_history:instrument_master")
        instrument_stats = bootstrap_instrument_master(con)
        steps.update(1)

        steps.set_description("bootstrap_master_history:membership_history")
        membership_stats = bootstrap_universe_membership_history(con)
        steps.update(1)

        steps.set_description("bootstrap_master_history:summary")
        summary = {
            "status": "SUCCESS",
            "db_path": str(db_path),
            "instrument_master": instrument_stats,
            "universe_membership_history": membership_stats,
            "notes": [
                "This is a pragmatic bootstrap, not a full institutional PIT reconstruction.",
                "effective_from comes from MIN(as_of_date) in research_universe_quality_20d.",
                "effective_to is left NULL, so membership remains active forward.",
                "ticker_history remains empty and should be rebuilt separately for strict PIT.",
            ],
        }
        print(json.dumps(summary, indent=2))
        steps.update(1)

        steps.close()
        return 0
    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
