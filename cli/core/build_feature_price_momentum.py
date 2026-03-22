"""
Momentum feature builder (modular via registry).

IMPORTANT:
- SQL-first
- compatible avec price_history (state actuel du repo)
- utilise un registry externe d'indicateurs
"""

from __future__ import annotations

import argparse
import json

import duckdb

from stock_quant.features.price_momentum.registry import ALL_INDICATORS


def build_table(connection: duckdb.DuckDBPyConnection) -> dict:
    """
    Construit feature_price_momentum_daily à partir de price_history.
    """

    # =============================
    # 1. Base dataset (price_history)
    # =============================
    base_cte = """
    WITH base AS (
        SELECT
            symbol,
            price_date AS as_of_date,
            close,
            high,
            low,
            volume
        FROM price_history
    ),

    lagged AS (
        SELECT
            *,
            LAG(close, 1) OVER price_w AS close_lag_1,
            close - LAG(close, 1) OVER price_w AS delta_close,

            GREATEST(close - LAG(close, 1) OVER price_w, 0) AS gain,
            GREATEST(LAG(close, 1) OVER price_w - close, 0) AS loss
        FROM base
        WINDOW price_w AS (
            PARTITION BY symbol
            ORDER BY as_of_date
        )
    ),

    rsi_base AS (
        SELECT
            *,
            AVG(gain) OVER (
                PARTITION BY symbol
                ORDER BY as_of_date
                ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
            ) AS avg_gain_14,

            AVG(loss) OVER (
                PARTITION BY symbol
                ORDER BY as_of_date
                ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
            ) AS avg_loss_14
        FROM lagged
    )
    """

    # =============================
    # 2. Injecter les indicateurs
    # =============================
    select_expressions = [
        "symbol",
        "as_of_date",
        "close"
    ]

    for spec in ALL_INDICATORS:
        select_expressions.extend(spec.sql_select_expressions)

    select_sql = ",\n".join(select_expressions)

    final_sql = f"""
    {base_cte}
    SELECT
        {select_sql}
    FROM rsi_base
    """

    # =============================
    # 3. Write table
    # =============================
    connection.execute("DROP TABLE IF EXISTS feature_price_momentum_daily")

    connection.execute(f"""
        CREATE TABLE feature_price_momentum_daily AS
        {final_sql}
    """)

    # =============================
    # 4. Metrics
    # =============================
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
        "indicators": [spec.name for spec in ALL_INDICATORS]
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
