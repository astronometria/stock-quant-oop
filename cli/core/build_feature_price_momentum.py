"""
Momentum feature builder (STRICT warm-up).

Source:
- price_source_daily_raw

Règle:
- toutes les fenêtres ont un warm-up strict
- avant assez d'observations, la feature retourne NULL
"""

from __future__ import annotations

import argparse
import json

import duckdb

from stock_quant.features.price_momentum.registry import ALL_INDICATORS


def build_table(connection: duckdb.DuckDBPyConnection) -> dict:
    connection.execute("DROP TABLE IF EXISTS feature_price_momentum_daily")

    connection.execute("""
        CREATE TABLE feature_price_momentum_daily AS
        WITH base AS (
            SELECT
                symbol,
                price_date AS as_of_date,
                close,
                high,
                low,
                volume
            FROM price_source_daily_raw
        ),
        lagged AS (
            SELECT
                symbol,
                as_of_date,
                close,
                high,
                low,
                volume,

                ROW_NUMBER() OVER (
                    PARTITION BY symbol
                    ORDER BY as_of_date
                ) AS row_num,

                LAG(close, 1) OVER (
                    PARTITION BY symbol
                    ORDER BY as_of_date
                ) AS close_lag_1,

                LAG(close, 5) OVER (
                    PARTITION BY symbol
                    ORDER BY as_of_date
                ) AS close_lag_5,

                LAG(close, 10) OVER (
                    PARTITION BY symbol
                    ORDER BY as_of_date
                ) AS close_lag_10,

                LAG(close, 20) OVER (
                    PARTITION BY symbol
                    ORDER BY as_of_date
                ) AS close_lag_20,

                LAG(close, 60) OVER (
                    PARTITION BY symbol
                    ORDER BY as_of_date
                ) AS close_lag_60,

                CASE
                    WHEN LAG(close, 1) OVER (
                        PARTITION BY symbol
                        ORDER BY as_of_date
                    ) IS NULL
                    THEN NULL
                    ELSE GREATEST(
                        close - LAG(close, 1) OVER (
                            PARTITION BY symbol
                            ORDER BY as_of_date
                        ),
                        0
                    )
                END AS gain,

                CASE
                    WHEN LAG(close, 1) OVER (
                        PARTITION BY symbol
                        ORDER BY as_of_date
                    ) IS NULL
                    THEN NULL
                    ELSE GREATEST(
                        LAG(close, 1) OVER (
                            PARTITION BY symbol
                            ORDER BY as_of_date
                        ) - close,
                        0
                    )
                END AS loss,

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

                MAX(close) OVER (
                    PARTITION BY symbol
                    ORDER BY as_of_date
                    ROWS BETWEEN 251 PRECEDING AND CURRENT ROW
                ) AS high_252
            FROM base
        ),
        enriched AS (
            SELECT
                symbol,
                as_of_date,
                close,
                row_num,

                CASE
                    WHEN row_num < 2 OR close_lag_1 IS NULL OR close_lag_1 = 0 THEN NULL
                    ELSE (close / close_lag_1) - 1
                END AS returns_1d,

                CASE
                    WHEN row_num < 6 OR close_lag_5 IS NULL OR close_lag_5 = 0 THEN NULL
                    ELSE (close / close_lag_5) - 1
                END AS returns_5d,

                CASE
                    WHEN row_num < 11 OR close_lag_10 IS NULL OR close_lag_10 = 0 THEN NULL
                    ELSE (close / close_lag_10) - 1
                END AS returns_10d,

                CASE
                    WHEN row_num < 21 OR close_lag_20 IS NULL OR close_lag_20 = 0 THEN NULL
                    ELSE (close / close_lag_20) - 1
                END AS returns_20d,

                CASE
                    WHEN row_num < 61 OR close_lag_60 IS NULL OR close_lag_60 = 0 THEN NULL
                    ELSE (close / close_lag_60) - 1
                END AS returns_60d,

                CASE
                    WHEN row_num < 15 THEN NULL
                    ELSE AVG(gain) OVER (
                        PARTITION BY symbol
                        ORDER BY as_of_date
                        ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
                    )
                END AS avg_gain_14,

                CASE
                    WHEN row_num < 15 THEN NULL
                    ELSE AVG(loss) OVER (
                        PARTITION BY symbol
                        ORDER BY as_of_date
                        ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
                    )
                END AS avg_loss_14,

                CASE
                    WHEN row_num < 14 THEN NULL
                    ELSE high_14
                END AS high_14_strict,

                CASE
                    WHEN row_num < 14 THEN NULL
                    ELSE low_14
                END AS low_14_strict,

                CASE
                    WHEN row_num < 252 THEN NULL
                    ELSE high_252
                END AS high_252_strict
            FROM lagged
        )
        SELECT
            symbol,
            as_of_date,
            close,
            returns_1d,
            returns_5d,
            returns_10d,
            returns_20d,
            returns_60d,

            CASE
                WHEN avg_loss_14 IS NULL THEN NULL
                WHEN avg_loss_14 = 0 AND avg_gain_14 = 0 THEN 50
                WHEN avg_loss_14 = 0 THEN 100
                ELSE 100 - (100 / (1 + (avg_gain_14 / avg_loss_14)))
            END AS rsi_14,

            CASE
                WHEN high_14_strict IS NULL OR low_14_strict IS NULL THEN NULL
                WHEN (high_14_strict - low_14_strict) = 0 THEN NULL
                ELSE -100 * ((high_14_strict - close) / (high_14_strict - low_14_strict))
            END AS williams_r_14,

            CASE
                WHEN high_252_strict IS NULL OR high_252_strict = 0 THEN NULL
                ELSE (close / high_252_strict) - 1
            END AS distance_from_252d_high

        FROM enriched
    """)

    row = connection.execute("""
        SELECT
            COUNT(*) AS rows,
            MIN(as_of_date),
            MAX(as_of_date),
            COUNT(DISTINCT symbol)
        FROM feature_price_momentum_daily
    """).fetchone()

    return {
        "table": "feature_price_momentum_daily",
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
