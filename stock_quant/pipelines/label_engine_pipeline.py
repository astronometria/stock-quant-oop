from __future__ import annotations

import duckdb
from datetime import datetime


def run(con: duckdb.DuckDBPyConnection) -> dict:

    metrics = {}

    # ============================================================
    # Forward return labels
    # ============================================================

    con.execute("""
    CREATE OR REPLACE TABLE return_labels_daily AS
    WITH base AS (
        SELECT
            instrument_id,
            symbol,
            bar_date AS as_of_date,
            adj_close
        FROM price_bars_adjusted
    ),
    returns AS (
        SELECT
            instrument_id,
            as_of_date,

            (LEAD(adj_close,1) OVER w / adj_close) - 1
                AS fwd_return_1d,

            (LEAD(adj_close,5) OVER w / adj_close) - 1
                AS fwd_return_5d,

            (LEAD(adj_close,20) OVER w / adj_close) - 1
                AS fwd_return_20d

        FROM base
        WINDOW w AS (
            PARTITION BY instrument_id
            ORDER BY as_of_date
        )
    )
    SELECT
        instrument_id,
        as_of_date,
        fwd_return_1d,
        fwd_return_5d,
        fwd_return_20d,
        'prices' AS source_name,
        CURRENT_TIMESTAMP AS created_at
    FROM returns
    """)

    metrics["return_label_rows"] = con.execute(
        "SELECT COUNT(*) FROM return_labels_daily"
    ).fetchone()[0]

    # ============================================================
    # Forward realized volatility labels (20d)
    # ============================================================

    con.execute("""
    CREATE OR REPLACE TABLE volatility_labels_daily AS
    WITH base AS (
        SELECT
            instrument_id,
            bar_date AS as_of_date,
            LN(adj_close /
               LAG(adj_close) OVER (
                   PARTITION BY instrument_id
                   ORDER BY bar_date
               )
            ) AS log_ret
        FROM price_bars_adjusted
    ),
    forward_vol AS (
        SELECT
            instrument_id,
            as_of_date,

            STDDEV_SAMP(log_ret) OVER (
                PARTITION BY instrument_id
                ORDER BY as_of_date
                ROWS BETWEEN 1 FOLLOWING AND 20 FOLLOWING
            ) AS realized_vol_20d

        FROM base
    )
    SELECT
        instrument_id,
        as_of_date,
        realized_vol_20d,
        'prices' AS source_name,
        CURRENT_TIMESTAMP AS created_at
    FROM forward_vol
    """)

    metrics["vol_label_rows"] = con.execute(
        "SELECT COUNT(*) FROM volatility_labels_daily"
    ).fetchone()[0]

    return metrics
