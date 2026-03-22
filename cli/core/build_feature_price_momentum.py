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
    CREATE OR REPLACE TABLE feature_price_momentum_daily AS
    WITH base AS (
        SELECT
            p.symbol,
            i.instrument_id,
            i.company_id,
            p.bar_date AS as_of_date,
            p.open,
            p.high,
            p.low,
            p.adj_close AS close,
            p.volume
        FROM price_bars_adjusted p
        LEFT JOIN instrument_master i USING (symbol)
        WHERE p.symbol IS NOT NULL
          AND p.bar_date IS NOT NULL
          AND p.adj_close IS NOT NULL
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
            ) AS lag_close_1,

            LAG(close, 5) OVER (
                PARTITION BY symbol
                ORDER BY as_of_date
            ) AS lag_close_5,

            LAG(close, 10) OVER (
                PARTITION BY symbol
                ORDER BY as_of_date
            ) AS lag_close_10,

            LAG(close, 20) OVER (
                PARTITION BY symbol
                ORDER BY as_of_date
            ) AS lag_close_20,

            LAG(close, 40) OVER (
                PARTITION BY symbol
                ORDER BY as_of_date
            ) AS lag_close_40,

            LAG(close, 60) OVER (
                PARTITION BY symbol
                ORDER BY as_of_date
            ) AS lag_close_60,

            CASE
                WHEN LAG(close, 1) OVER (
                    PARTITION BY symbol
                    ORDER BY as_of_date
                ) IS NULL THEN NULL
                ELSE GREATEST(
                    close - LAG(close, 1) OVER (
                        PARTITION BY symbol
                        ORDER BY as_of_date
                    ),
                    0
                )
            END AS gain_1d,

            CASE
                WHEN LAG(close, 1) OVER (
                    PARTITION BY symbol
                    ORDER BY as_of_date
                ) IS NULL THEN NULL
                ELSE GREATEST(
                    LAG(close, 1) OVER (
                        PARTITION BY symbol
                        ORDER BY as_of_date
                    ) - close,
                    0
                )
            END AS loss_1d,

            MAX(high) OVER (
                PARTITION BY symbol
                ORDER BY as_of_date
                ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
            ) AS high_14,

            MIN(low) OVER (
                PARTITION BY symbol
                ORDER BY as_of_date
                ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
            ) AS low_14,

            MIN(low) OVER (
                PARTITION BY symbol
                ORDER BY as_of_date
                ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
            ) AS low_20,

            MAX(high) OVER (
                PARTITION BY symbol
                ORDER BY as_of_date
                ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
            ) AS high_20,

            MAX(close) OVER (
                PARTITION BY symbol
                ORDER BY as_of_date
                ROWS BETWEEN 251 PRECEDING AND CURRENT ROW
            ) AS high_252
        FROM base
    ),

    stats AS (
        SELECT
            symbol,
            instrument_id,
            company_id,
            as_of_date,
            close,
            row_num,
            lag_close_1,
            lag_close_5,
            lag_close_10,
            lag_close_20,
            lag_close_40,
            lag_close_60,
            high_14,
            low_14,
            low_20,
            high_20,
            high_252,

            AVG(gain_1d) OVER (
                PARTITION BY symbol
                ORDER BY as_of_date
                ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
            ) AS avg_gain_14,

            AVG(loss_1d) OVER (
                PARTITION BY symbol
                ORDER BY as_of_date
                ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
            ) AS avg_loss_14
        FROM lagged
    )

    SELECT
        symbol,
        instrument_id,
        company_id,
        as_of_date,
        close,

        CASE
            WHEN row_num < 2 OR lag_close_1 IS NULL OR lag_close_1 = 0 THEN NULL
            ELSE (close / lag_close_1) - 1
        END AS returns_1d,

        CASE
            WHEN row_num < 6 OR lag_close_5 IS NULL OR lag_close_5 = 0 THEN NULL
            ELSE (close / lag_close_5) - 1
        END AS returns_5d,

        CASE
            WHEN row_num < 11 OR lag_close_10 IS NULL OR lag_close_10 = 0 THEN NULL
            ELSE (close / lag_close_10) - 1
        END AS returns_10d,

        CASE
            WHEN row_num < 21 OR lag_close_20 IS NULL OR lag_close_20 = 0 THEN NULL
            ELSE (close / lag_close_20) - 1
        END AS returns_20d,

        CASE
            WHEN row_num < 61 OR lag_close_60 IS NULL OR lag_close_60 = 0 THEN NULL
            ELSE (close / lag_close_60) - 1
        END AS returns_60d,

        CASE
            WHEN row_num < 2 OR lag_close_1 IS NULL OR lag_close_1 <= 0 OR close <= 0 THEN NULL
            ELSE LN(close / lag_close_1)
        END AS returns_log_1d,

        CASE
            WHEN row_num < 21 OR lag_close_20 IS NULL OR lag_close_20 = 0 THEN NULL
            ELSE (close / lag_close_20) - 1
        END AS momentum_20d,

        CASE
            WHEN row_num < 41
              OR lag_close_20 IS NULL OR lag_close_20 = 0
              OR lag_close_40 IS NULL OR lag_close_40 = 0
            THEN NULL
            ELSE
                ((close / lag_close_20) - 1)
                -
                ((lag_close_20 / lag_close_40) - 1)
        END AS momentum_acceleration_20d,

        CASE
            WHEN row_num < 15 THEN NULL
            WHEN avg_loss_14 IS NULL THEN NULL
            WHEN avg_loss_14 = 0 AND avg_gain_14 = 0 THEN 50
            WHEN avg_loss_14 = 0 THEN 100
            ELSE 100 - (100 / (1 + (avg_gain_14 / avg_loss_14)))
        END AS rsi_14,

        CASE
            WHEN row_num < 14 THEN NULL
            WHEN (high_14 - low_14) = 0 THEN NULL
            ELSE -100 * ((high_14 - close) / (high_14 - low_14))
        END AS williams_r_14,

        CASE
            WHEN row_num < 14 THEN NULL
            WHEN (high_14 - low_14) = 0 THEN NULL
            ELSE (close - low_14) / (high_14 - low_14)
        END AS stoch_k_14,

        CASE
            WHEN row_num < 252 THEN NULL
            WHEN high_252 IS NULL OR high_252 = 0 THEN NULL
            ELSE (close / high_252) - 1
        END AS distance_from_252d_high

    FROM stats
    """

    con.execute(sql)

    row = con.execute("""
        SELECT
            COUNT(*) AS rows,
            COUNT(DISTINCT symbol) AS symbols,
            MIN(as_of_date) AS min_date,
            MAX(as_of_date) AS max_date
        FROM feature_price_momentum_daily
    """).fetchone()

    return {
        "table": "feature_price_momentum_daily",
        "rows": row[0],
        "symbols": row[1],
        "min_date": str(row[2]),
        "max_date": str(row[3]),
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
