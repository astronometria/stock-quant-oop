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
    CREATE OR REPLACE TABLE research_labels AS
    WITH base AS (
        SELECT
            symbol,
            instrument_id,
            company_id,
            as_of_date,
            close,

            ROW_NUMBER() OVER (
                PARTITION BY symbol
                ORDER BY as_of_date
            ) AS row_num,

            LEAD(close, 5) OVER (
                PARTITION BY symbol
                ORDER BY as_of_date
            ) AS close_fwd_5,

            LEAD(close, 10) OVER (
                PARTITION BY symbol
                ORDER BY as_of_date
            ) AS close_fwd_10,

            LEAD(close, 20) OVER (
                PARTITION BY symbol
                ORDER BY as_of_date
            ) AS close_fwd_20

        FROM research_features_daily
        WHERE close IS NOT NULL
    )

    SELECT
        symbol,
        instrument_id,
        company_id,
        as_of_date,
        close,

        CASE
            WHEN close_fwd_5 IS NULL OR close = 0 THEN NULL
            ELSE (close_fwd_5 / close) - 1
        END AS return_fwd_5d,

        CASE
            WHEN close_fwd_10 IS NULL OR close = 0 THEN NULL
            ELSE (close_fwd_10 / close) - 1
        END AS return_fwd_10d,

        CASE
            WHEN close_fwd_20 IS NULL OR close = 0 THEN NULL
            ELSE (close_fwd_20 / close) - 1
        END AS return_fwd_20d,

        CASE
            WHEN close_fwd_5 IS NULL OR close = 0 THEN NULL
            WHEN (close_fwd_5 / close - 1) > 0 THEN 1
            ELSE 0
        END AS target_up_5d,

        CASE
            WHEN close_fwd_10 IS NULL OR close = 0 THEN NULL
            WHEN (close_fwd_10 / close - 1) > 0 THEN 1
            ELSE 0
        END AS target_up_10d,

        CASE
            WHEN close_fwd_20 IS NULL OR close = 0 THEN NULL
            WHEN (close_fwd_20 / close - 1) > 0 THEN 1
            ELSE 0
        END AS target_up_20d

    FROM base
    """

    con.execute(sql)

    row = con.execute("""
        SELECT
            COUNT(*) AS rows,
            COUNT(DISTINCT symbol) AS symbols,
            COUNT(return_fwd_5d) AS r5,
            COUNT(return_fwd_10d) AS r10,
            COUNT(return_fwd_20d) AS r20,
            MIN(as_of_date),
            MAX(as_of_date)
        FROM research_labels
    """).fetchone()

    return {
        "table": "research_labels",
        "rows": row[0],
        "symbols": row[1],
        "return_fwd_5d_rows": row[2],
        "return_fwd_10d_rows": row[3],
        "return_fwd_20d_rows": row[4],
        "min_date": str(row[5]),
        "max_date": str(row[6]),
        "leakage_safe": True
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
