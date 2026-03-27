#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import duckdb


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--db-path", required=True)
    return p.parse_args()


def build_table(con):
    sql = """
    CREATE OR REPLACE TABLE feature_price_volatility_daily AS
    WITH base AS (
        SELECT
            p.symbol,
            i.instrument_id,
            i.company_id,
            p.bar_date AS as_of_date,
            p.adj_open,
            p.adj_high,
            p.adj_low,
            p.adj_close AS close,
            p.volume
        FROM price_bars_adjusted p
        LEFT JOIN instrument_master i USING (symbol)
        WHERE p.symbol IS NOT NULL
          AND p.bar_date IS NOT NULL
          AND p.adj_close IS NOT NULL
          AND p.adj_open IS NOT NULL
          AND p.adj_high IS NOT NULL
          AND p.adj_low IS NOT NULL
          AND p.adj_high > 0
          AND p.adj_low > 0
    ),

    lagged AS (
        SELECT
            symbol,
            instrument_id,
            company_id,
            as_of_date,
            open,
            high,
            low,
            close,
            volume,

            ROW_NUMBER() OVER (
                PARTITION BY symbol
                ORDER BY as_of_date
            ) AS row_num,

            LAG(close, 1) OVER (
                PARTITION BY symbol
                ORDER BY as_of_date
            ) AS lag_close_1
        FROM base
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
            volume,
            row_num,
            lag_close_1,

            CASE
                WHEN lag_close_1 IS NULL OR lag_close_1 = 0 THEN NULL
                ELSE (close / lag_close_1) - 1
            END AS returns_1d,

            CASE
                WHEN lag_close_1 IS NULL THEN high - low
                ELSE GREATEST(
                    high - low,
                    ABS(high - lag_close_1),
                    ABS(low - lag_close_1)
                )
            END AS true_range,

            CASE
                WHEN lag_close_1 IS NULL OR lag_close_1 <= 0 OR close <= 0 THEN NULL
                ELSE LN(close / lag_close_1)
            END AS log_return_1d,

            CASE
                WHEN high <= 0 OR low <= 0 THEN NULL
                ELSE LN(high / low)
            END AS log_high_low,

            CASE
                WHEN open <= 0 OR close <= 0 THEN NULL
                ELSE LN(close / open)
            END AS log_close_open
        FROM lagged
    ),

    stats AS (
        SELECT
            symbol,
            instrument_id,
            company_id,
            as_of_date,
            close,
            row_num,

            AVG(true_range) OVER (
                PARTITION BY symbol
                ORDER BY as_of_date
                ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
            ) AS atr_14_raw,

            STDDEV_SAMP(returns_1d) OVER (
                PARTITION BY symbol
                ORDER BY as_of_date
                ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
            ) AS volatility_5_raw,

            STDDEV_SAMP(returns_1d) OVER (
                PARTITION BY symbol
                ORDER BY as_of_date
                ROWS BETWEEN 9 PRECEDING AND CURRENT ROW
            ) AS volatility_10_raw,

            STDDEV_SAMP(returns_1d) OVER (
                PARTITION BY symbol
                ORDER BY as_of_date
                ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
            ) AS volatility_20_raw,

            STDDEV_SAMP(returns_1d) OVER (
                PARTITION BY symbol
                ORDER BY as_of_date
                ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
            ) AS volatility_60_raw,

            STDDEV_SAMP(log_return_1d) OVER (
                PARTITION BY symbol
                ORDER BY as_of_date
                ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
            ) AS realized_vol_20d_raw,

            AVG(POWER(log_high_low, 2)) OVER (
                PARTITION BY symbol
                ORDER BY as_of_date
                ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
            ) AS parkinson_var_20d_raw,

            AVG(
                (POWER(log_high_low, 2) / 2.0)
                - ((2.0 * LN(2.0) - 1.0) * POWER(log_close_open, 2))
            ) OVER (
                PARTITION BY symbol
                ORDER BY as_of_date
                ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
            ) AS garman_klass_var_20d_raw,

            AVG(close) OVER (
                PARTITION BY symbol
                ORDER BY as_of_date
                ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
            ) AS mean_close_20_raw,

            STDDEV_SAMP(close) OVER (
                PARTITION BY symbol
                ORDER BY as_of_date
                ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
            ) AS std_close_20_raw
        FROM enriched
    )

    SELECT
        symbol,
        instrument_id,
        company_id,
        as_of_date,
        close,

        CASE
            WHEN row_num < 14 THEN NULL
            ELSE atr_14_raw
        END AS atr_14,

        CASE
            WHEN row_num < 14 OR close = 0 OR atr_14_raw IS NULL THEN NULL
            ELSE atr_14_raw / close
        END AS atr_pct_14,

        CASE
            WHEN row_num < 5 THEN NULL
            ELSE volatility_5_raw
        END AS volatility_5,

        CASE
            WHEN row_num < 10 THEN NULL
            ELSE volatility_10_raw
        END AS volatility_10,

        CASE
            WHEN row_num < 20 THEN NULL
            ELSE volatility_20_raw
        END AS volatility_20,

        CASE
            WHEN row_num < 60 THEN NULL
            ELSE volatility_60_raw
        END AS volatility_60,

        CASE
            WHEN row_num < 20 THEN NULL
            ELSE realized_vol_20d_raw
        END AS realized_vol_20d,

        CASE
            WHEN row_num < 20 OR parkinson_var_20d_raw IS NULL THEN NULL
            ELSE SQRT(GREATEST(parkinson_var_20d_raw, 0.0))
        END AS parkinson_vol_20d,

        CASE
            WHEN row_num < 20 OR garman_klass_var_20d_raw IS NULL THEN NULL
            ELSE SQRT(GREATEST(garman_klass_var_20d_raw, 0.0))
        END AS garman_klass_vol_20d,

        CASE
            WHEN row_num < 20 OR std_close_20_raw IS NULL OR std_close_20_raw = 0 THEN NULL
            ELSE (close - mean_close_20_raw) / std_close_20_raw
        END AS bollinger_zscore_20,

        CASE
            WHEN row_num < 20 OR mean_close_20_raw IS NULL OR mean_close_20_raw = 0 OR std_close_20_raw IS NULL THEN NULL
            ELSE (4.0 * std_close_20_raw) / mean_close_20_raw
        END AS bollinger_bandwidth_20

    FROM stats
    """

    con.execute(sql)

    row = con.execute("""
        SELECT
            COUNT(*) AS rows,
            COUNT(DISTINCT symbol) AS symbols,
            MIN(as_of_date) AS min_date,
            MAX(as_of_date) AS max_date,
            COUNT(atr_14) AS atr_14_rows,
            COUNT(atr_pct_14) AS atr_pct_14_rows,
            COUNT(volatility_5) AS volatility_5_rows,
            COUNT(volatility_10) AS volatility_10_rows,
            COUNT(volatility_20) AS volatility_20_rows,
            COUNT(volatility_60) AS volatility_60_rows,
            COUNT(realized_vol_20d) AS realized_vol_20d_rows,
            COUNT(parkinson_vol_20d) AS parkinson_vol_20d_rows,
            COUNT(garman_klass_vol_20d) AS garman_klass_vol_20d_rows,
            COUNT(bollinger_zscore_20) AS bollinger_zscore_20_rows,
            COUNT(bollinger_bandwidth_20) AS bollinger_bandwidth_20_rows
        FROM feature_price_volatility_daily
    """).fetchone()

    return {
        "table": "feature_price_volatility_daily",
        "rows": row[0],
        "symbols": row[1],
        "min_date": str(row[2]),
        "max_date": str(row[3]),
        "atr_14_rows": row[4],
        "atr_pct_14_rows": row[5],
        "volatility_5_rows": row[6],
        "volatility_10_rows": row[7],
        "volatility_20_rows": row[8],
        "volatility_60_rows": row[9],
        "realized_vol_20d_rows": row[10],
        "parkinson_vol_20d_rows": row[11],
        "garman_klass_vol_20d_rows": row[12],
        "bollinger_zscore_20_rows": row[13],
        "bollinger_bandwidth_20_rows": row[14],
        "strict_mode": True,
    }


def main():
    args = parse_args()
    con = duckdb.connect(args.db_path)
    try:
        print(json.dumps(build_table(con), indent=2))
    finally:
        con.close()


if __name__ == "__main__":
    main()
