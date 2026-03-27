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
    CREATE OR REPLACE TABLE feature_price_trend_daily AS
    WITH base AS (
        SELECT
            p.symbol,
            i.instrument_id,
            i.company_id,
            p.bar_date AS as_of_date,
            p.adj_close AS close,
            p.adj_open,
            p.adj_high,
            p.adj_low,
            p.volume
        FROM price_bars_adjusted p
        LEFT JOIN instrument_master i USING (symbol)
        WHERE p.symbol IS NOT NULL
          AND p.bar_date IS NOT NULL
          AND p.adj_close IS NOT NULL
    ),

    ma_base AS (
        SELECT
            symbol,
            instrument_id,
            company_id,
            as_of_date,
            close,
            open,
            high,
            low,
            volume,

            ROW_NUMBER() OVER (
                PARTITION BY symbol
                ORDER BY as_of_date
            ) AS row_num,

            AVG(close) OVER (
                PARTITION BY symbol
                ORDER BY as_of_date
                ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
            ) AS sma_20_raw,

            AVG(close) OVER (
                PARTITION BY symbol
                ORDER BY as_of_date
                ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
            ) AS sma_50_raw,

            AVG(close) OVER (
                PARTITION BY symbol
                ORDER BY as_of_date
                ROWS BETWEEN 199 PRECEDING AND CURRENT ROW
            ) AS sma_200_raw,

            AVG(close) OVER (
                PARTITION BY symbol
                ORDER BY as_of_date
                ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
            ) AS ema_12_raw,

            AVG(close) OVER (
                PARTITION BY symbol
                ORDER BY as_of_date
                ROWS BETWEEN 25 PRECEDING AND CURRENT ROW
            ) AS ema_26_raw,

            AVG(close) OVER (
                PARTITION BY symbol
                ORDER BY as_of_date
                ROWS BETWEEN 39 PRECEDING AND 20 PRECEDING
            ) AS sma_20_shift_raw,

            AVG(close) OVER (
                PARTITION BY symbol
                ORDER BY as_of_date
                ROWS BETWEEN 99 PRECEDING AND CURRENT ROW
            ) AS sma_100_raw
        FROM base
    ),

    strict_ma AS (
        SELECT
            symbol,
            instrument_id,
            company_id,
            as_of_date,
            close,
            row_num,

            CASE WHEN row_num < 20 THEN NULL ELSE sma_20_raw END AS sma_20,
            CASE WHEN row_num < 50 THEN NULL ELSE sma_50_raw END AS sma_50,
            CASE WHEN row_num < 200 THEN NULL ELSE sma_200_raw END AS sma_200,
            CASE WHEN row_num < 12 THEN NULL ELSE ema_12_raw END AS ema_12,
            CASE WHEN row_num < 26 THEN NULL ELSE ema_26_raw END AS ema_26,
            CASE WHEN row_num < 40 THEN NULL ELSE sma_20_shift_raw END AS sma_20_shift,
            CASE WHEN row_num < 100 THEN NULL ELSE sma_100_raw END AS sma_100
        FROM ma_base
    ),

    macd_line_base AS (
        SELECT
            symbol,
            instrument_id,
            company_id,
            as_of_date,
            close,
            row_num,
            sma_20,
            sma_50,
            sma_200,
            ema_12,
            ema_26,
            sma_20_shift,
            sma_100,
            CASE
                WHEN ema_12 IS NULL OR ema_26 IS NULL THEN NULL
                ELSE ema_12 - ema_26
            END AS macd_line
        FROM strict_ma
    ),

    macd_signal_base AS (
        SELECT
            symbol,
            instrument_id,
            company_id,
            as_of_date,
            close,
            row_num,
            sma_20,
            sma_50,
            sma_200,
            ema_12,
            ema_26,
            sma_20_shift,
            sma_100,
            macd_line,
            CASE
                WHEN row_num < 34 THEN NULL
                ELSE AVG(macd_line) OVER (
                    PARTITION BY symbol
                    ORDER BY as_of_date
                    ROWS BETWEEN 8 PRECEDING AND CURRENT ROW
                )
            END AS macd_signal
        FROM macd_line_base
    )

    SELECT
        symbol,
        instrument_id,
        company_id,
        as_of_date,
        close,

        sma_20,
        sma_50,
        sma_200,

        CASE
            WHEN sma_20 IS NULL OR sma_20 = 0 THEN NULL
            ELSE (close / sma_20) - 1
        END AS close_to_sma_20,

        CASE
            WHEN sma_50 IS NULL OR sma_50 = 0 THEN NULL
            ELSE (close / sma_50) - 1
        END AS close_to_sma_50,

        CASE
            WHEN sma_200 IS NULL OR sma_200 = 0 THEN NULL
            ELSE (close / sma_200) - 1
        END AS close_to_sma_200,

        ema_12,
        ema_26,

        macd_line,
        macd_signal,

        CASE
            WHEN macd_line IS NULL OR macd_signal IS NULL THEN NULL
            ELSE macd_line - macd_signal
        END AS macd_hist,

        CASE
            WHEN sma_20 IS NULL OR sma_20_shift IS NULL THEN NULL
            ELSE sma_20 - sma_20_shift
        END AS slope_20d,

        CASE
            WHEN sma_50 IS NULL OR sma_100 IS NULL THEN NULL
            ELSE sma_50 - sma_100
        END AS slope_50d,

        CASE
            WHEN sma_20 IS NULL OR sma_200 IS NULL OR sma_200 = 0 THEN NULL
            ELSE (sma_20 - sma_200) / sma_200
        END AS trend_strength

    FROM macd_signal_base
    """

    con.execute(sql)

    row = con.execute("""
        SELECT
            COUNT(*) AS rows,
            COUNT(DISTINCT symbol) AS symbols,
            MIN(as_of_date) AS min_date,
            MAX(as_of_date) AS max_date,
            COUNT(sma_20) AS sma_20_rows,
            COUNT(sma_50) AS sma_50_rows,
            COUNT(sma_200) AS sma_200_rows,
            COUNT(ema_12) AS ema_12_rows,
            COUNT(ema_26) AS ema_26_rows,
            COUNT(macd_line) AS macd_line_rows,
            COUNT(macd_signal) AS macd_signal_rows,
            COUNT(macd_hist) AS macd_hist_rows,
            COUNT(slope_20d) AS slope_20d_rows,
            COUNT(slope_50d) AS slope_50d_rows,
            COUNT(trend_strength) AS trend_strength_rows
        FROM feature_price_trend_daily
    """).fetchone()

    return {
        "table": "feature_price_trend_daily",
        "rows": row[0],
        "symbols": row[1],
        "min_date": str(row[2]),
        "max_date": str(row[3]),
        "sma_20_rows": row[4],
        "sma_50_rows": row[5],
        "sma_200_rows": row[6],
        "ema_12_rows": row[7],
        "ema_26_rows": row[8],
        "macd_line_rows": row[9],
        "macd_signal_rows": row[10],
        "macd_hist_rows": row[11],
        "slope_20d_rows": row[12],
        "slope_50d_rows": row[13],
        "trend_strength_rows": row[14],
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
