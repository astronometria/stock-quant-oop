#!/usr/bin/env python3
from __future__ import annotations

"""
Rebuild research_features_daily (SQL-first, research-grade)

Objectif
--------
Reconstruire research_features_daily à partir de sources canoniques
disponibles dans la DB live, sans hardcoder à l'aveugle les noms de
colonnes si le schéma diffère légèrement entre tables/environnements.

Sources visées
--------------
- price_history
- short_features_daily

Features reconstruites
----------------------
- rsi_14
- short_volume_ratio

Notes
-----
- Le calcul RSI ici est volontairement simple mais PIT-safe.
- Le script détecte le nom de colonne date et prix dans price_history.
- Il reste SQL-first.
"""

import argparse
from pathlib import Path

import duckdb


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-path", default="/home/marty/stock-quant-oop/market.duckdb")
    parser.add_argument("--memory-limit", default="24GB")
    parser.add_argument("--threads", type=int, default=6)
    parser.add_argument("--temp-dir", default="/home/marty/stock-quant-oop/tmp")
    parser.add_argument("--verbose", action="store_true")
    return parser.parse_args()


def pick_first(existing: set[str], candidates: list[str], label: str) -> str:
    for candidate in candidates:
        if candidate in existing:
            return candidate
    raise RuntimeError(
        f"Could not find a suitable {label} column. "
        f"Available columns: {sorted(existing)}; candidates tried: {candidates}"
    )


def main() -> int:
    args = parse_args()
    db_path = Path(args.db_path).expanduser().resolve()

    con = duckdb.connect(str(db_path))
    try:
        if args.memory_limit:
            con.execute(f"PRAGMA memory_limit='{args.memory_limit}'")
        if args.threads:
            con.execute(f"PRAGMA threads={int(args.threads)}")
        if args.temp_dir:
            temp_dir_sql = str(Path(args.temp_dir).expanduser().resolve()).replace("'", "''")
            con.execute(f"PRAGMA temp_directory='{temp_dir_sql}'")

        price_cols = {
            str(row[1]).strip()
            for row in con.execute("PRAGMA table_info('price_history')").fetchall()
        }
        short_cols = {
            str(row[1]).strip()
            for row in con.execute("PRAGMA table_info('short_features_daily')").fetchall()
        }

        price_date_col = pick_first(price_cols, ["as_of_date", "price_date", "bar_date", "date"], "price date")
        price_col = pick_first(price_cols, ["close", "adj_close", "adjusted_close"], "price")
        price_symbol_col = pick_first(price_cols, ["symbol"], "price symbol")

        short_date_col = pick_first(short_cols, ["as_of_date", "price_date", "bar_date", "date"], "short date")
        short_symbol_col = pick_first(short_cols, ["symbol"], "short symbol")
        short_ratio_col = pick_first(short_cols, ["short_volume_ratio"], "short volume ratio")

        if args.verbose:
            print(f"[rebuild] db_path={db_path}", flush=True)
            print(f"[rebuild] price_date_col={price_date_col}", flush=True)
            print(f"[rebuild] price_col={price_col}", flush=True)
            print(f"[rebuild] price_symbol_col={price_symbol_col}", flush=True)
            print(f"[rebuild] short_date_col={short_date_col}", flush=True)
            print(f"[rebuild] short_symbol_col={short_symbol_col}", flush=True)
            print(f"[rebuild] short_ratio_col={short_ratio_col}", flush=True)

        print("[rebuild] dropping table...", flush=True)
        con.execute("DROP TABLE IF EXISTS research_features_daily")

        print("[rebuild] creating table...", flush=True)
        con.execute(
            f"""
            CREATE TABLE research_features_daily AS
            WITH price_base AS (
                SELECT
                    CAST({price_symbol_col} AS VARCHAR) AS symbol,
                    CAST({price_date_col} AS DATE) AS as_of_date,
                    CAST({price_col} AS DOUBLE) AS close
                FROM price_history
                WHERE {price_symbol_col} IS NOT NULL
                  AND {price_date_col} IS NOT NULL
                  AND {price_col} IS NOT NULL
            ),
            price_delta AS (
                SELECT
                    symbol,
                    as_of_date,
                    close,
                    close - LAG(close) OVER (
                        PARTITION BY symbol
                        ORDER BY as_of_date
                    ) AS delta
                FROM price_base
            ),
            rsi_roll AS (
                SELECT
                    symbol,
                    as_of_date,
                    AVG(CASE WHEN delta > 0 THEN delta ELSE 0 END) OVER (
                        PARTITION BY symbol
                        ORDER BY as_of_date
                        ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
                    ) AS avg_gain_14,
                    AVG(CASE WHEN delta < 0 THEN -delta ELSE 0 END) OVER (
                        PARTITION BY symbol
                        ORDER BY as_of_date
                        ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
                    ) AS avg_loss_14
                FROM price_delta
            ),
            rsi_final AS (
                SELECT
                    symbol,
                    as_of_date,
                    CASE
                        WHEN avg_loss_14 IS NULL OR avg_gain_14 IS NULL THEN NULL
                        WHEN avg_loss_14 = 0 AND avg_gain_14 = 0 THEN 50.0
                        WHEN avg_loss_14 = 0 THEN 100.0
                        ELSE 100 - (100 / (1 + (avg_gain_14 / avg_loss_14)))
                    END AS rsi_14
                FROM rsi_roll
            ),
            short_base AS (
                SELECT
                    CAST({short_symbol_col} AS VARCHAR) AS symbol,
                    CAST({short_date_col} AS DATE) AS as_of_date,
                    CAST({short_ratio_col} AS DOUBLE) AS short_volume_ratio
                FROM short_features_daily
                WHERE {short_symbol_col} IS NOT NULL
                  AND {short_date_col} IS NOT NULL
            )
            SELECT
                NULL::VARCHAR AS instrument_id,
                NULL::VARCHAR AS company_id,
                p.symbol,
                p.as_of_date,
                NULL::DOUBLE AS close_to_sma_20,
                r.rsi_14,
                NULL::DOUBLE AS revenue,
                NULL::DOUBLE AS net_income,
                NULL::DOUBLE AS net_margin,
                NULL::DOUBLE AS debt_to_equity,
                NULL::DOUBLE AS return_on_assets,
                NULL::DOUBLE AS short_interest,
                NULL::DOUBLE AS days_to_cover,
                s.short_volume_ratio,
                NULL::DOUBLE AS short_interest_change_pct,
                NULL::DOUBLE AS short_squeeze_score,
                NULL::DOUBLE AS short_pressure_zscore,
                NULL::DOUBLE AS days_to_cover_zscore,
                NULL::BIGINT AS article_count_1d,
                NULL::BIGINT AS unique_cluster_count_1d,
                NULL::DOUBLE AS avg_link_confidence,
                'rebuild_research_features.py' AS source_name,
                CURRENT_TIMESTAMP AS created_at
            FROM price_base p
            LEFT JOIN rsi_final r
              ON r.symbol = p.symbol
             AND r.as_of_date = p.as_of_date
            LEFT JOIN short_base s
              ON UPPER(TRIM(s.symbol)) = UPPER(TRIM(p.symbol))
             AND s.as_of_date = p.as_of_date
            """
        )

        print("[rebuild] done", flush=True)

        row = con.execute(
            """
            SELECT
                COUNT(*) AS total_rows,
                COUNT(rsi_14) AS rsi_rows,
                COUNT(short_volume_ratio) AS short_rows,
                MIN(as_of_date) AS min_date,
                MAX(as_of_date) AS max_date
            FROM research_features_daily
            """
        ).fetchone()

        print(f"[rebuild] coverage={row}", flush=True)
        return 0
    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
