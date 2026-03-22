#!/usr/bin/env python3
from __future__ import annotations

# -----------------------------------------------------------------------------
# Rebuild research_labels from canonical adjusted prices.
#
# Why this rewrite:
# - The previous version built forward returns from research_features_daily.close
#   and partitioned by symbol.
# - That is fragile for dirty historical symbol series, ticker reuse, and
#   corrupted/non-canonical close values.
# - We want forward returns built from price_bars_adjusted.adj_close and grouped
#   by instrument_id whenever possible.
#
# Design goals:
# - SQL-first
# - no leakage
# - deterministic
# - strong data-quality guards
# - lots of comments for future developers
# -----------------------------------------------------------------------------

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
        help=(
            "Hard sanity cap for forward returns. "
            "Rows with abs(return) greater than this are nulled out."
        ),
    )
    return parser.parse_args()


def build_table(con: duckdb.DuckDBPyConnection, max_abs_return: float) -> dict:
    # -------------------------------------------------------------------------
    # Step 1:
    # Build a canonical price base using adjusted prices from price_bars_adjusted.
    #
    # We join through research_features_daily only to preserve the model-facing
    # research universe keys (instrument_id/company_id/symbol/as_of_date).
    #
    # Important:
    # - use adj_close, not the derived close stored in research_features_daily
    # - partition by instrument_id, not symbol
    # - require positive current and future adjusted prices
    # -------------------------------------------------------------------------
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
            WHEN return_fwd_5d_raw IS NULL THEN NULL
            WHEN ABS(return_fwd_5d_raw) > {max_abs_return} THEN NULL
            WHEN return_fwd_5d_raw > 0 THEN 1
            ELSE 0
        END AS target_up_5d,

        CASE
            WHEN return_fwd_10d_raw IS NULL THEN NULL
            WHEN ABS(return_fwd_10d_raw) > {max_abs_return} THEN NULL
            WHEN return_fwd_10d_raw > 0 THEN 1
            ELSE 0
        END AS target_up_10d,

        CASE
            WHEN return_fwd_20d_raw IS NULL THEN NULL
            WHEN ABS(return_fwd_20d_raw) > {max_abs_return} THEN NULL
            WHEN return_fwd_20d_raw > 0 THEN 1
            ELSE 0
        END AS target_up_20d
    FROM raw_returns
    """

    con.execute(sql)

    # -------------------------------------------------------------------------
    # Emit stats that are actually useful for diagnosing label health.
    # -------------------------------------------------------------------------
    row = con.execute(
        """
        SELECT
            COUNT(*) AS rows_total,
            COUNT(DISTINCT instrument_id) AS instruments,
            COUNT(DISTINCT symbol) AS symbols,
            MIN(as_of_date) AS min_date,
            MAX(as_of_date) AS max_date,
            COUNT(return_fwd_5d) AS rows_5d,
            COUNT(return_fwd_10d) AS rows_10d,
            COUNT(return_fwd_20d) AS rows_20d,
            MAX(return_fwd_20d) AS max_return_20d,
            MIN(return_fwd_20d) AS min_return_20d,
            AVG(return_fwd_20d) AS avg_return_20d
        FROM research_labels
        """
    ).fetchone()

    return {
        "table_name": "research_labels",
        "rows_total": int(row[0]),
        "instruments": int(row[1]),
        "symbols": int(row[2]),
        "min_date": str(row[3]) if row[3] is not None else None,
        "max_date": str(row[4]) if row[4] is not None else None,
        "rows_5d": int(row[5]),
        "rows_10d": int(row[6]),
        "rows_20d": int(row[7]),
        "max_return_20d": float(row[8]) if row[8] is not None else None,
        "min_return_20d": float(row[9]) if row[9] is not None else None,
        "avg_return_20d": float(row[10]) if row[10] is not None else None,
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
