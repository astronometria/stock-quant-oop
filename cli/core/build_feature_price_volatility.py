#!/usr/bin/env python3

from __future__ import annotations

# =============================================================================
# Build feature_price_volatility_daily
#
# Réparation ciblée:
# - utilise adj_open / adj_high / adj_low / adj_close
# - produit les colonnes attendues downstream:
#   atr_14, atr_pct_14, volatility_5, volatility_10, volatility_20, volatility_60,
#   realized_vol_20d, parkinson_vol_20d, garman_klass_vol_20d,
#   bollinger_zscore_20, bollinger_bandwidth_20
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
    CREATE OR REPLACE TABLE feature_price_volatility_daily AS
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
            AND p.adj_open IS NOT NULL
            AND p.adj_high IS NOT NULL
            AND p.adj_low IS NOT NULL
    ),
    enriched AS (
        SELECT
            symbol,
            instrument_id,
            company_id,
            as_of_date,
            open,
            high,
            low,
            close,
            ROW_NUMBER() OVER (
                PARTITION BY symbol ORDER BY as_of_date
            ) AS row_num,
            LAG(close, 1) OVER (
                PARTITION BY symbol ORDER BY as_of_date
            ) AS prev_close,
            CASE
                WHEN LAG(close, 1) OVER (PARTITION BY symbol ORDER BY as_of_date) IS NULL THEN NULL
                WHEN LAG(close, 1) OVER (PARTITION BY symbol ORDER BY as_of_date) = 0 THEN NULL
                WHEN close <= 0 THEN NULL
                ELSE LN(close / LAG(close, 1) OVER (PARTITION BY symbol ORDER BY as_of_date))
            END AS log_ret_1d,
            GREATEST(
                high - low,
                ABS(high - COALESCE(LAG(close, 1) OVER (PARTITION BY symbol ORDER BY as_of_date), close)),
                ABS(low - COALESCE(LAG(close, 1) OVER (PARTITION BY symbol ORDER BY as_of_date), close))
            ) AS true_range
        FROM base
    ),
    stats AS (
        SELECT
            *,
            AVG(true_range) OVER (
                PARTITION BY symbol
                ORDER BY as_of_date
                ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
            ) AS atr_14_raw,
            STDDEV_SAMP(log_ret_1d) OVER (
                PARTITION BY symbol
                ORDER BY as_of_date
                ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
            ) AS volatility_5_raw,
            STDDEV_SAMP(log_ret_1d) OVER (
                PARTITION BY symbol
                ORDER BY as_of_date
                ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
            ) AS volatility_10_raw,
            STDDEV_SAMP(log_ret_1d) OVER (
                PARTITION BY symbol
                ORDER BY as_of_date
                ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
            ) AS volatility_20_raw,
            STDDEV_SAMP(log_ret_1d) OVER (
                PARTITION BY symbol
                ORDER BY as_of_date
                ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
            ) AS volatility_60_raw,
            AVG(close) OVER (
                PARTITION BY symbol
                ORDER BY as_of_date
                ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
            ) AS sma_20,
            STDDEV_SAMP(close) OVER (
                PARTITION BY symbol
                ORDER BY as_of_date
                ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
            ) AS close_std_20,
            AVG(POWER(LN(high / NULLIF(low, 0)), 2)) OVER (
                PARTITION BY symbol
                ORDER BY as_of_date
                ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
            ) AS parkinson_term_20,
            AVG(
                0.5 * POWER(LN(high / NULLIF(low, 0)), 2)
                - (2 * LN(2) - 1) * POWER(LN(close / NULLIF(open, 0)), 2)
            ) OVER (
                PARTITION BY symbol
                ORDER BY as_of_date
                ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
            ) AS gk_term_20
        FROM enriched
    )
    SELECT
        symbol,
        instrument_id,
        company_id,
        as_of_date,
        CASE WHEN row_num < 14 THEN NULL ELSE atr_14_raw END AS atr_14,
        CASE
            WHEN row_num < 14 OR atr_14_raw IS NULL OR close IS NULL OR close = 0 THEN NULL
            ELSE atr_14_raw / close
        END AS atr_pct_14,
        CASE WHEN row_num < 5 THEN NULL ELSE volatility_5_raw END AS volatility_5,
        CASE WHEN row_num < 10 THEN NULL ELSE volatility_10_raw END AS volatility_10,
        CASE WHEN row_num < 20 THEN NULL ELSE volatility_20_raw END AS volatility_20,
        CASE WHEN row_num < 60 THEN NULL ELSE volatility_60_raw END AS volatility_60,
        CASE
            WHEN row_num < 20 OR volatility_20_raw IS NULL THEN NULL
            ELSE volatility_20_raw * SQRT(252)
        END AS realized_vol_20d,
        CASE
            WHEN row_num < 20 OR parkinson_term_20 IS NULL OR parkinson_term_20 < 0 THEN NULL
            ELSE SQRT(parkinson_term_20 / (4 * LN(2))) * SQRT(252)
        END AS parkinson_vol_20d,
        CASE
            WHEN row_num < 20 OR gk_term_20 IS NULL OR gk_term_20 < 0 THEN NULL
            ELSE SQRT(gk_term_20) * SQRT(252)
        END AS garman_klass_vol_20d,
        CASE
            WHEN row_num < 20 OR sma_20 IS NULL OR close_std_20 IS NULL OR close_std_20 = 0 THEN NULL
            ELSE (close - sma_20) / close_std_20
        END AS bollinger_zscore_20,
        CASE
            WHEN row_num < 20 OR sma_20 IS NULL OR sma_20 = 0 OR close_std_20 IS NULL THEN NULL
            ELSE (4 * close_std_20) / sma_20
        END AS bollinger_bandwidth_20
    FROM stats
    """
    con.execute(sql)

    row = con.execute("""
        SELECT
            COUNT(*) AS rows,
            COUNT(DISTINCT symbol) AS symbols,
            MIN(as_of_date) AS min_date,
            MAX(as_of_date) AS max_date
        FROM feature_price_volatility_daily
    """).fetchone()

    return {
        "table": "feature_price_volatility_daily",
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
