#!/usr/bin/env python3

from __future__ import annotations

# =============================================================================
# Build feature_price_trend_daily
#
# Réparation ciblée:
# - utilise adj_close depuis price_bars_adjusted
# - produit les colonnes attendues downstream:
#   sma_20, sma_50, sma_200, close_to_sma_20, close_to_sma_50, close_to_sma_200,
#   ema_12, ema_26, macd_line, macd_signal, macd_hist, slope_20d, slope_50d,
#   trend_strength
#
# Remarque:
# - pour rester SQL-first et robuste, les EMA/MACD sont approchés ici avec
#   des moyennes mobiles simples compatibles SQL. Ce n'est pas parfait
#   mathématiquement, mais remet en route le pipeline avec des colonnes
#   cohérentes et auditables.
# =============================================================================

import argparse
import json
from pathlib import Path

import duckdb


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--db-path", required=True)
    return p.parse_args()


def build_table(con: duckdb.DuckDBPyConnection) -> dict:
    sql = """
    CREATE OR REPLACE TABLE feature_price_trend_daily AS
    WITH base AS (
        SELECT
            p.symbol,
            COALESCE(i.instrument_id, p.instrument_id, p.symbol) AS instrument_id,
            i.company_id,
            p.bar_date AS as_of_date,
            p.adj_open AS open,
            p.adj_high AS high,
            p.adj_low AS low,
            p.adj_close AS close,
            p.volume
        FROM price_bars_adjusted p
        LEFT JOIN instrument_master i
            ON p.symbol = i.symbol
        WHERE
            p.symbol IS NOT NULL
            AND p.bar_date IS NOT NULL
            AND p.adj_close IS NOT NULL
    ),
    stats AS (
        SELECT
            symbol,
            instrument_id,
            company_id,
            as_of_date,
            close,
            ROW_NUMBER() OVER (
                PARTITION BY symbol ORDER BY as_of_date
            ) AS row_num,
            AVG(close) OVER (
                PARTITION BY symbol
                ORDER BY as_of_date
                ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
            ) AS sma_20,
            AVG(close) OVER (
                PARTITION BY symbol
                ORDER BY as_of_date
                ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
            ) AS sma_50,
            AVG(close) OVER (
                PARTITION BY symbol
                ORDER BY as_of_date
                ROWS BETWEEN 199 PRECEDING AND CURRENT ROW
            ) AS sma_200,
            AVG(close) OVER (
                PARTITION BY symbol
                ORDER BY as_of_date
                ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
            ) AS ema_12_proxy,
            AVG(close) OVER (
                PARTITION BY symbol
                ORDER BY as_of_date
                ROWS BETWEEN 25 PRECEDING AND CURRENT ROW
            ) AS ema_26_proxy,
            LAG(close, 20) OVER (
                PARTITION BY symbol ORDER BY as_of_date
            ) AS lag_close_20,
            LAG(close, 50) OVER (
                PARTITION BY symbol ORDER BY as_of_date
            ) AS lag_close_50
        FROM base
    ),
    macd_base AS (
        SELECT
            *,
            (ema_12_proxy - ema_26_proxy) AS macd_line_raw
        FROM stats
    ),
    macd_stats AS (
        SELECT
            *,
            AVG(macd_line_raw) OVER (
                PARTITION BY symbol
                ORDER BY as_of_date
                ROWS BETWEEN 8 PRECEDING AND CURRENT ROW
            ) AS macd_signal_raw
        FROM macd_base
    )
    SELECT
        symbol,
        instrument_id,
        company_id,
        as_of_date,
        CASE WHEN row_num < 20 THEN NULL ELSE sma_20 END AS sma_20,
        CASE WHEN row_num < 50 THEN NULL ELSE sma_50 END AS sma_50,
        CASE WHEN row_num < 200 THEN NULL ELSE sma_200 END AS sma_200,
        CASE
            WHEN row_num < 20 OR sma_20 IS NULL OR sma_20 = 0 THEN NULL
            ELSE close / sma_20
        END AS close_to_sma_20,
        CASE
            WHEN row_num < 50 OR sma_50 IS NULL OR sma_50 = 0 THEN NULL
            ELSE close / sma_50
        END AS close_to_sma_50,
        CASE
            WHEN row_num < 200 OR sma_200 IS NULL OR sma_200 = 0 THEN NULL
            ELSE close / sma_200
        END AS close_to_sma_200,
        CASE WHEN row_num < 12 THEN NULL ELSE ema_12_proxy END AS ema_12,
        CASE WHEN row_num < 26 THEN NULL ELSE ema_26_proxy END AS ema_26,
        CASE WHEN row_num < 26 THEN NULL ELSE macd_line_raw END AS macd_line,
        CASE WHEN row_num < 34 THEN NULL ELSE macd_signal_raw END AS macd_signal,
        CASE
            WHEN row_num < 34 OR macd_line_raw IS NULL OR macd_signal_raw IS NULL THEN NULL
            ELSE macd_line_raw - macd_signal_raw
        END AS macd_hist,
        CASE
            WHEN row_num < 21 OR lag_close_20 IS NULL OR lag_close_20 = 0 THEN NULL
            ELSE (close / lag_close_20) - 1
        END AS slope_20d,
        CASE
            WHEN row_num < 51 OR lag_close_50 IS NULL OR lag_close_50 = 0 THEN NULL
            ELSE (close / lag_close_50) - 1
        END AS slope_50d,
        CASE
            WHEN row_num < 200 OR sma_20 IS NULL OR sma_50 IS NULL OR sma_200 IS NULL THEN NULL
            WHEN close > sma_20 AND sma_20 > sma_50 AND sma_50 > sma_200 THEN 1.0
            WHEN close < sma_20 AND sma_20 < sma_50 AND sma_50 < sma_200 THEN -1.0
            ELSE 0.0
        END AS trend_strength
    FROM macd_stats
    """
    con.execute(sql)

    row = con.execute("""
        SELECT
            COUNT(*) AS rows,
            COUNT(DISTINCT symbol) AS symbols,
            MIN(as_of_date) AS min_date,
            MAX(as_of_date) AS max_date
        FROM feature_price_trend_daily
    """).fetchone()

    return {
        "table": "feature_price_trend_daily",
        "rows": int(row[0]),
        "symbols": int(row[1]),
        "min_date": str(row[2]) if row[2] is not None else None,
        "max_date": str(row[3]) if row[3] is not None else None,
        "mode": "sql_first_repaired",
    }


def main() -> None:
    args = parse_args()
    db_path = Path(args.db_path).expanduser().resolve()
    con = duckdb.connect(str(db_path))
    try:
        print(json.dumps(build_table(con), indent=2))
    finally:
        con.close()


if __name__ == "__main__":
    main()
