"""
Trend feature builder (STRICT warm-up).

Source:
- price_source_daily_raw

Règle:
- SMA / EMA proxy / MACD ont un warm-up strict
- avant assez d'observations, la feature retourne NULL
"""

from __future__ import annotations

import argparse
import json

import duckdb

from stock_quant.features.price_trend.registry import ALL_INDICATORS


def build_table(connection: duckdb.DuckDBPyConnection) -> dict:
    connection.execute("DROP TABLE IF EXISTS feature_price_trend_daily")

    connection.execute("""
        CREATE TABLE feature_price_trend_daily AS
        WITH base AS (
            SELECT
                symbol,
                price_date AS as_of_date,
                close
            FROM price_source_daily_raw
        ),
        ma_base AS (
            SELECT
                symbol,
                as_of_date,
                close,

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
                ) AS ema_26_raw
            FROM base
        ),
        strict_ma AS (
            SELECT
                symbol,
                as_of_date,
                close,
                row_num,

                CASE WHEN row_num < 20 THEN NULL ELSE sma_20_raw END AS sma_20,
                CASE WHEN row_num < 50 THEN NULL ELSE sma_50_raw END AS sma_50,
                CASE WHEN row_num < 200 THEN NULL ELSE sma_200_raw END AS sma_200,
                CASE WHEN row_num < 12 THEN NULL ELSE ema_12_raw END AS ema_12,
                CASE WHEN row_num < 26 THEN NULL ELSE ema_26_raw END AS ema_26
            FROM ma_base
        ),
        macd_line_base AS (
            SELECT
                symbol,
                as_of_date,
                close,
                row_num,
                sma_20,
                sma_50,
                sma_200,
                ema_12,
                ema_26,
                CASE
                    WHEN ema_12 IS NULL OR ema_26 IS NULL THEN NULL
                    ELSE ema_12 - ema_26
                END AS macd_line
            FROM strict_ma
        ),
        macd_signal_base AS (
            SELECT
                symbol,
                as_of_date,
                close,
                row_num,
                sma_20,
                sma_50,
                sma_200,
                ema_12,
                ema_26,
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
            END AS macd_hist

        FROM macd_signal_base
    """)

    row = connection.execute("""
        SELECT
            COUNT(*) AS rows,
            MIN(as_of_date),
            MAX(as_of_date),
            COUNT(DISTINCT symbol)
        FROM feature_price_trend_daily
    """).fetchone()

    return {
        "table": "feature_price_trend_daily",
        "rows": row[0],
        "min_date": str(row[1]),
        "max_date": str(row[2]),
        "symbol_count": row[3],
        "indicators": [spec.name for spec in ALL_INDICATORS],
        "strict_mode": True,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-path", required=True)
    args = parser.parse_args()

    con = duckdb.connect(args.db_path)
    try:
        metrics = build_table(con)
        print(json.dumps(metrics, indent=2))
    finally:
        con.close()


if __name__ == "__main__":
    main()
