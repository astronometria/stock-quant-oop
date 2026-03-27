#!/usr/bin/env python3

from __future__ import annotations

# =============================================================================
# Réparation ciblée du builder research_universe.
#
# Constat:
# - l'ancien script importait ResearchUniverseService, qui n'existe plus
# - plusieurs parties du repo attendent une table research_universe
#
# Cette version reconstruit une table research_universe simple, stable et
# SQL-first à partir de:
# - price_bars_adjusted
# - universe_membership_history (si disponible)
#
# But:
# - remettre le repo dans un état exécutable
# - fournir une table d'univers de base auditable
# =============================================================================

import argparse
import json
from pathlib import Path

import duckdb
from tqdm import tqdm


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build research_universe from adjusted price bars.")
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


def main() -> int:
    args = parse_args()
    db_path = str(Path(args.db_path).expanduser().resolve())

    con = duckdb.connect(db_path)
    steps = tqdm(total=4, desc="build_research_universe", unit="step")

    try:
        steps.set_description("build_research_universe:validate")
        if not table_exists(con, "price_bars_adjusted"):
            raise RuntimeError("Missing required table: price_bars_adjusted")
        steps.update(1)

        steps.set_description("build_research_universe:build_table")

        has_membership = table_exists(con, "universe_membership_history")

        if has_membership:
            sql = """
            CREATE OR REPLACE TABLE research_universe AS
            WITH latest_bar AS (
                SELECT
                    symbol,
                    instrument_id,
                    MAX(bar_date) AS last_bar_date
                FROM price_bars_adjusted
                GROUP BY symbol, instrument_id
            ),
            latest_bar_enriched AS (
                SELECT
                    p.symbol,
                    p.instrument_id,
                    p.bar_date AS last_bar_date,
                    p.adj_close AS latest_adj_close,
                    (p.adj_close * p.volume) AS latest_dollar_volume
                FROM price_bars_adjusted p
                INNER JOIN latest_bar l
                    ON p.symbol = l.symbol
                   AND p.instrument_id = l.instrument_id
                   AND p.bar_date = l.last_bar_date
            ),
            price_summary AS (
                SELECT
                    symbol,
                    instrument_id,
                    MIN(bar_date) AS first_bar_date,
                    MAX(bar_date) AS last_bar_date,
                    COUNT(*) AS total_bars
                FROM price_bars_adjusted
                GROUP BY symbol, instrument_id
            ),
            membership_summary AS (
                SELECT
                    UPPER(TRIM(symbol)) AS symbol,
                    COUNT(*) AS membership_rows,
                    MIN(COALESCE(effective_from, DATE '1900-01-01')) AS membership_min_date,
                    MAX(COALESCE(effective_to, DATE '2999-12-31')) AS membership_max_date
                FROM universe_membership_history
                WHERE symbol IS NOT NULL
                GROUP BY UPPER(TRIM(symbol))
            )
            SELECT
                p.instrument_id,
                p.symbol,
                p.first_bar_date,
                p.last_bar_date,
                p.total_bars,
                l.latest_adj_close,
                l.latest_dollar_volume,
                COALESCE(m.membership_rows, 0) AS membership_rows,
                m.membership_min_date,
                m.membership_max_date,
                CURRENT_TIMESTAMP AS created_at
            FROM price_summary p
            LEFT JOIN latest_bar_enriched l
                ON p.symbol = l.symbol
               AND p.instrument_id = l.instrument_id
            LEFT JOIN membership_summary m
                ON p.symbol = m.symbol
            ORDER BY p.symbol
            """
        else:
            sql = """
            CREATE OR REPLACE TABLE research_universe AS
            WITH latest_bar AS (
                SELECT
                    symbol,
                    instrument_id,
                    MAX(bar_date) AS last_bar_date
                FROM price_bars_adjusted
                GROUP BY symbol, instrument_id
            ),
            latest_bar_enriched AS (
                SELECT
                    p.symbol,
                    p.instrument_id,
                    p.bar_date AS last_bar_date,
                    p.adj_close AS latest_adj_close,
                    (p.adj_close * p.volume) AS latest_dollar_volume
                FROM price_bars_adjusted p
                INNER JOIN latest_bar l
                    ON p.symbol = l.symbol
                   AND p.instrument_id = l.instrument_id
                   AND p.bar_date = l.last_bar_date
            ),
            price_summary AS (
                SELECT
                    symbol,
                    instrument_id,
                    MIN(bar_date) AS first_bar_date,
                    MAX(bar_date) AS last_bar_date,
                    COUNT(*) AS total_bars
                FROM price_bars_adjusted
                GROUP BY symbol, instrument_id
            )
            SELECT
                p.instrument_id,
                p.symbol,
                p.first_bar_date,
                p.last_bar_date,
                p.total_bars,
                l.latest_adj_close,
                l.latest_dollar_volume,
                0 AS membership_rows,
                NULL AS membership_min_date,
                NULL AS membership_max_date,
                CURRENT_TIMESTAMP AS created_at
            FROM price_summary p
            LEFT JOIN latest_bar_enriched l
                ON p.symbol = l.symbol
               AND p.instrument_id = l.instrument_id
            ORDER BY p.symbol
            """
        con.execute(sql)
        steps.update(1)

        steps.set_description("build_research_universe:metrics")
        row = con.execute(
            """
            SELECT
                COUNT(*) AS rows_total,
                COUNT(DISTINCT symbol) AS symbols_total,
                MIN(first_bar_date) AS min_date,
                MAX(last_bar_date) AS max_date
            FROM research_universe
            """
        ).fetchone()
        steps.update(1)

        steps.set_description("build_research_universe:done")
        result = {
            "status": "SUCCESS",
            "db_path": db_path,
            "table_name": "research_universe",
            "rows_total": int(row[0]),
            "symbols_total": int(row[1]),
            "min_date": str(row[2]) if row[2] is not None else None,
            "max_date": str(row[3]) if row[3] is not None else None,
            "used_membership_history": has_membership,
        }
        print(json.dumps(result, indent=2))
        steps.update(1)

        steps.close()
        return 0

    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
