#!/usr/bin/env python3
from __future__ import annotations

# =============================================================================
# Build research_labels
#
# This version adds a true 1-day forward label so the model can be trained on
# the same horizon used later by a next-day execution backtest.
#
# Key goals:
# - SQL-first
# - use canonical adjusted prices
# - partition by instrument_id
# - add strong data-quality guards
# - keep existing 5d / 10d / 20d labels
# =============================================================================

import argparse
import json
from pathlib import Path

import duckdb


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-path", required=True, help="Path to DuckDB database.")
    parser.add_argument(
        "--max-abs-return",
        type=float,
        default=5.0,
        help="Hard sanity cap. Forward returns above this abs value are nulled out.",
    )
    return parser.parse_args()


def build_table(con: duckdb.DuckDBPyConnection, max_abs_return: float) -> dict:
    sql = f"""
    CREATE OR REPLACE TABLE research_labels AS
    WITH canonical_price_base AS (
        SELECT
            f.instrument_id,
            f.company_id,
            f.symbol,
            f.as_of_date,
            p.adj_close AS adj_close
        FROM research_features_daily f
        INNER JOIN price_bars_adjusted p
            ON f.symbol = p.symbol
           AND f.as_of_date = p.bar_date
        WHERE f.instrument_id IS NOT NULL
          AND f.as_of_date IS NOT NULL
          AND p.adj_close IS NOT NULL
          AND p.adj_close > 0
    ),
    forward_prices AS (
        SELECT
            instrument_id,
            company_id,
            symbol,
            as_of_date,
            adj_close,
            LEAD(adj_close, 1) OVER (
                PARTITION BY instrument_id
                ORDER BY as_of_date
            ) AS adj_close_fwd_1,
            LEAD(adj_close, 5) OVER (
                PARTITION BY instrument_id
                ORDER BY as_of_date
            ) AS adj_close_fwd_5,
            LEAD(adj_close, 10) OVER (
                PARTITION BY instrument_id
                ORDER BY as_of_date
            ) AS adj_close_fwd_10,
            LEAD(adj_close, 20) OVER (
                PARTITION BY instrument_id
                ORDER BY as_of_date
            ) AS adj_close_fwd_20
        FROM canonical_price_base
    ),
    raw_returns AS (
        SELECT
            instrument_id,
            company_id,
            symbol,
            as_of_date,
            adj_close AS close,
            CASE
                WHEN adj_close_fwd_1 IS NULL OR adj_close <= 0 OR adj_close_fwd_1 <= 0 THEN NULL
                ELSE (adj_close_fwd_1 / adj_close) - 1
            END AS return_fwd_1d_raw,
            CASE
                WHEN adj_close_fwd_5 IS NULL OR adj_close <= 0 OR adj_close_fwd_5 <= 0 THEN NULL
                ELSE (adj_close_fwd_5 / adj_close) - 1
            END AS return_fwd_5d_raw,
            CASE
                WHEN adj_close_fwd_10 IS NULL OR adj_close <= 0 OR adj_close_fwd_10 <= 0 THEN NULL
                ELSE (adj_close_fwd_10 / adj_close) - 1
            END AS return_fwd_10d_raw,
            CASE
                WHEN adj_close_fwd_20 IS NULL OR adj_close <= 0 OR adj_close_fwd_20 <= 0 THEN NULL
                ELSE (adj_close_fwd_20 / adj_close) - 1
            END AS return_fwd_20d_raw
        FROM forward_prices
    )
    SELECT
        instrument_id,
        company_id,
        symbol,
        as_of_date,
        close,

        CASE
            WHEN return_fwd_1d_raw IS NULL THEN NULL
            WHEN ABS(return_fwd_1d_raw) > {max_abs_return} THEN NULL
            ELSE return_fwd_1d_raw
        END AS return_fwd_1d,

        CASE
            WHEN return_fwd_5d_raw IS NULL THEN NULL
            WHEN ABS(return_fwd_5d_raw) > {max_abs_return} THEN NULL
            ELSE return_fwd_5d_raw
        END AS return_fwd_5d,

        CASE
            WHEN return_fwd_10d_raw IS NULL THEN NULL
            WHEN ABS(return_fwd_10d_raw) > {max_abs_return} THEN NULL
            ELSE return_fwd_10d_raw
        END AS return_fwd_10d,

        CASE
            WHEN return_fwd_20d_raw IS NULL THEN NULL
            WHEN ABS(return_fwd_20d_raw) > {max_abs_return} THEN NULL
            ELSE return_fwd_20d_raw
        END AS return_fwd_20d,

        CASE
            WHEN return_fwd_1d_raw IS NULL THEN NULL
            WHEN ABS(return_fwd_1d_raw) > {max_abs_return} THEN NULL
            WHEN return_fwd_1d_raw > 0 THEN 1 ELSE 0
        END AS target_up_1d,

        CASE
            WHEN return_fwd_5d_raw IS NULL THEN NULL
            WHEN ABS(return_fwd_5d_raw) > {max_abs_return} THEN NULL
            WHEN return_fwd_5d_raw > 0 THEN 1 ELSE 0
        END AS target_up_5d,

        CASE
            WHEN return_fwd_10d_raw IS NULL THEN NULL
            WHEN ABS(return_fwd_10d_raw) > {max_abs_return} THEN NULL
            WHEN return_fwd_10d_raw > 0 THEN 1 ELSE 0
        END AS target_up_10d,

        CASE
            WHEN return_fwd_20d_raw IS NULL THEN NULL
            WHEN ABS(return_fwd_20d_raw) > {max_abs_return} THEN NULL
            WHEN return_fwd_20d_raw > 0 THEN 1 ELSE 0
        END AS target_up_20d
    FROM raw_returns
    """
    con.execute(sql)

    row = con.execute(
        """
        SELECT
            COUNT(*) AS rows_total,
            COUNT(return_fwd_1d) AS rows_1d,
            COUNT(return_fwd_5d) AS rows_5d,
            COUNT(return_fwd_10d) AS rows_10d,
            COUNT(return_fwd_20d) AS rows_20d,
            AVG(return_fwd_1d) AS avg_return_1d,
            MIN(return_fwd_1d) AS min_return_1d,
            MAX(return_fwd_1d) AS max_return_1d
        FROM research_labels
        """
    ).fetchone()

    return {
        "table_name": "research_labels",
        "rows_total": int(row[0]),
        "rows_1d": int(row[1]),
        "rows_5d": int(row[2]),
        "rows_10d": int(row[3]),
        "rows_20d": int(row[4]),
        "avg_return_1d": float(row[5]) if row[5] is not None else None,
        "min_return_1d": float(row[6]) if row[6] is not None else None,
        "max_return_1d": float(row[7]) if row[7] is not None else None,
        "max_abs_return_cap": float(max_abs_return),
    }


def main() -> int:
    args = parse_args()
    db_path = Path(args.db_path).expanduser().resolve()

    con = duckdb.connect(str(db_path))
    try:
        stats = build_table(con, max_abs_return=args.max_abs_return)
        print(json.dumps(stats, indent=2), flush=True)
        return 0
    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
