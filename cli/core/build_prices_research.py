#!/usr/bin/env python3

from __future__ import annotations

# =============================================================================
# Réparation ciblée du builder de prix "research".
#
# Constat:
# - init_prices_research_foundation.py crée déjà les tables de fondation
# - l'ancien build_prices_research.py réessayait de repasser par un manager
#   de schéma cassé dans ce contexte
# - résultat: crash avant même de charger les prix
#
# Cette version:
# - suppose que la fondation a déjà été initialisée
# - vérifie la présence des tables attendues
# - charge price_bars_unadjusted / price_bars_adjusted / price_quality_flags
# - reste SQL-first, sans gros DataFrame Python
# =============================================================================

import argparse
import json
from pathlib import Path

import duckdb
from tqdm import tqdm


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build research price tables from price_history or price_source_daily_raw.")
    parser.add_argument("--db-path", required=True, help="Path to DuckDB database file.")
    parser.add_argument(
        "--prefer-source-table",
        default="price_history",
        choices=["price_history", "price_source_daily_raw"],
        help="Preferred source table when multiple valid tables exist.",
    )
    parser.add_argument("--verbose", action="store_true", help="Print extra diagnostics.")
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


def table_count(con: duckdb.DuckDBPyConnection, table_name: str) -> int:
    return int(con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0])


def choose_source_table(con: duckdb.DuckDBPyConnection, preferred: str) -> str:
    candidates = [preferred] + [t for t in ["price_history", "price_source_daily_raw"] if t != preferred]
    for name in candidates:
        if table_exists(con, name):
            if table_count(con, name) > 0:
                return name
    raise RuntimeError("No usable source table found among: price_history, price_source_daily_raw")


def ensure_foundation_tables(con: duckdb.DuckDBPyConnection) -> None:
    required = [
        "price_bars_unadjusted",
        "price_bars_adjusted",
        "price_quality_flags",
        "instrument_master",
    ]
    missing = [t for t in required if not table_exists(con, t)]
    if missing:
        raise RuntimeError(
            "Missing required foundation tables. Run cli/core/init_prices_research_foundation.py first. "
            f"Missing: {missing}"
        )


def source_sql(source_table: str) -> str:
    return f"""
    SELECT
        UPPER(TRIM(symbol)) AS symbol,
        CAST(price_date AS DATE) AS bar_date,
        CAST(open AS DOUBLE) AS open,
        CAST(high AS DOUBLE) AS high,
        CAST(low AS DOUBLE) AS low,
        CAST(close AS DOUBLE) AS close,
        CAST(volume AS BIGINT) AS volume,
        CAST(source_name AS VARCHAR) AS source_name
    FROM {source_table}
    WHERE
        symbol IS NOT NULL
        AND TRIM(symbol) <> ''
        AND price_date IS NOT NULL
        AND open IS NOT NULL
        AND high IS NOT NULL
        AND low IS NOT NULL
        AND close IS NOT NULL
        AND volume IS NOT NULL
    """


def build_tables(con: duckdb.DuckDBPyConnection, source_table: str) -> dict:
    # -------------------------------------------------------------------------
    # Base temporaire normalisée.
    # On enrichit avec instrument_id quand instrument_master contient le symbole.
    # Fallback: instrument_id = symbol.
    # -------------------------------------------------------------------------
    con.execute("DROP TABLE IF EXISTS tmp_prices_research_base")
    con.execute(
        f"""
        CREATE TEMP TABLE tmp_prices_research_base AS
        WITH src AS (
            {source_sql(source_table)}
        )
        SELECT
            COALESCE(im.instrument_id, src.symbol) AS instrument_id,
            src.symbol,
            src.bar_date,
            src.open,
            src.high,
            src.low,
            src.close,
            src.volume,
            src.source_name,
            CURRENT_TIMESTAMP AS created_at
        FROM src
        LEFT JOIN instrument_master im
            ON UPPER(TRIM(im.symbol)) = src.symbol
        """
    )

    # -------------------------------------------------------------------------
    # Barres non ajustées
    # -------------------------------------------------------------------------
    con.execute("DELETE FROM price_bars_unadjusted")
    con.execute(
        """
        INSERT INTO price_bars_unadjusted (
            instrument_id,
            symbol,
            bar_date,
            open,
            high,
            low,
            close,
            volume,
            source_name,
            created_at
        )
        SELECT
            instrument_id,
            symbol,
            bar_date,
            open,
            high,
            low,
            close,
            volume,
            source_name,
            created_at
        FROM tmp_prices_research_base
        ORDER BY symbol, bar_date
        """
    )

    # -------------------------------------------------------------------------
    # Barres ajustées
    #
    # Phase 1 volontairement simple:
    # - adj_* = raw_*
    # - adjustment_factor = 1.0
    #
    # Cela remet en route tous les scripts downstream qui attendent
    # price_bars_adjusted, sans prétendre résoudre déjà toutes les corporate
    # actions.
    # -------------------------------------------------------------------------
    con.execute("DELETE FROM price_bars_adjusted")
    con.execute(
        """
        INSERT INTO price_bars_adjusted (
            instrument_id,
            symbol,
            bar_date,
            adj_open,
            adj_high,
            adj_low,
            adj_close,
            volume,
            adjustment_factor,
            source_name,
            created_at
        )
        SELECT
            instrument_id,
            symbol,
            bar_date,
            open AS adj_open,
            high AS adj_high,
            low AS adj_low,
            close AS adj_close,
            volume,
            1.0 AS adjustment_factor,
            source_name,
            created_at
        FROM tmp_prices_research_base
        ORDER BY symbol, bar_date
        """
    )

    # -------------------------------------------------------------------------
    # Flags qualité simples et auditables
    # -------------------------------------------------------------------------
    con.execute("DELETE FROM price_quality_flags")
    con.execute(
        """
        INSERT INTO price_quality_flags (
            instrument_id,
            price_date,
            flag_type,
            flag_value,
            source_name,
            created_at
        )
        WITH ordered AS (
            SELECT
                instrument_id,
                symbol,
                bar_date,
                close,
                volume,
                source_name,
                LAG(close, 1) OVER (
                    PARTITION BY symbol
                    ORDER BY bar_date
                ) AS prev_close
            FROM tmp_prices_research_base
        ),
        flags AS (
            SELECT
                instrument_id,
                bar_date AS price_date,
                'close_nonpositive' AS flag_type,
                CAST(close AS DOUBLE) AS flag_value,
                source_name,
                CURRENT_TIMESTAMP AS created_at
            FROM ordered
            WHERE close <= 0

            UNION ALL

            SELECT
                instrument_id,
                bar_date AS price_date,
                'volume_negative' AS flag_type,
                CAST(volume AS DOUBLE) AS flag_value,
                source_name,
                CURRENT_TIMESTAMP AS created_at
            FROM ordered
            WHERE volume < 0

            UNION ALL

            SELECT
                instrument_id,
                bar_date AS price_date,
                'raw_return_abs_gt_80pct' AS flag_type,
                ABS((close / NULLIF(prev_close, 0)) - 1.0) AS flag_value,
                source_name,
                CURRENT_TIMESTAMP AS created_at
            FROM ordered
            WHERE
                prev_close IS NOT NULL
                AND prev_close > 0
                AND ABS((close / NULLIF(prev_close, 0)) - 1.0) > 0.80
        )
        SELECT
            instrument_id,
            price_date,
            flag_type,
            flag_value,
            source_name,
            created_at
        FROM flags
        """
    )

    row = con.execute(
        """
        SELECT
            (SELECT COUNT(*) FROM price_bars_unadjusted) AS unadjusted_rows,
            (SELECT COUNT(*) FROM price_bars_adjusted) AS adjusted_rows,
            (SELECT COUNT(*) FROM price_quality_flags) AS quality_rows,
            (SELECT COUNT(DISTINCT symbol) FROM price_bars_adjusted) AS symbols_total,
            (SELECT MIN(bar_date) FROM price_bars_adjusted) AS min_date,
            (SELECT MAX(bar_date) FROM price_bars_adjusted) AS max_date
        """
    ).fetchone()

    return {
        "price_bars_unadjusted_rows": int(row[0]),
        "price_bars_adjusted_rows": int(row[1]),
        "price_quality_flags_rows": int(row[2]),
        "symbols_total": int(row[3]),
        "min_date": str(row[4]) if row[4] is not None else None,
        "max_date": str(row[5]) if row[5] is not None else None,
    }


def main() -> int:
    args = parse_args()
    db_path = str(Path(args.db_path).expanduser().resolve())

    steps = tqdm(total=5, desc="build_prices_research", unit="step")
    con = duckdb.connect(db_path)

    try:
        steps.set_description("build_prices_research:check_foundation")
        ensure_foundation_tables(con)
        steps.update(1)

        steps.set_description("build_prices_research:choose_source")
        source_table = choose_source_table(con, args.prefer_source_table)
        source_rows = table_count(con, source_table)
        steps.update(1)

        steps.set_description("build_prices_research:build_tables")
        summary = build_tables(con, source_table)
        steps.update(1)

        steps.set_description("build_prices_research:post_checks")
        result = {
            "status": "SUCCESS",
            "db_path": db_path,
            "source_table": source_table,
            "source_rows": source_rows,
            "summary": summary,
        }
        steps.update(1)

        steps.set_description("build_prices_research:done")
        print(json.dumps(result, indent=2))
        steps.update(1)
        steps.close()
        return 0

    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
