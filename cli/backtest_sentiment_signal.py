#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json

import duckdb


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run a simple sentiment backtest on market_features_daily."
    )
    parser.add_argument("--db-path", required=True, help="Path to DuckDB database file.")
    parser.add_argument("--date-from", default=None, help="Inclusive start date YYYY-MM-DD.")
    parser.add_argument("--date-to", default=None, help="Inclusive end date YYYY-MM-DD.")
    parser.add_argument("--symbols", default=None, help="Comma-separated symbols filter.")
    parser.add_argument("--threshold-long", type=float, default=0.5, help="Long threshold on avg_sentiment_score.")
    parser.add_argument("--threshold-short", type=float, default=-0.5, help="Short threshold on avg_sentiment_score.")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose output.")
    return parser.parse_args()


def sql_quote(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def main() -> int:
    args = parse_args()
    con = duckdb.connect(args.db_path)

    where = ["avg_sentiment_score IS NOT NULL"]

    if args.date_from:
        where.append(f"feature_date >= DATE {sql_quote(args.date_from)}")
    if args.date_to:
        where.append(f"feature_date <= DATE {sql_quote(args.date_to)}")
    if args.symbols:
        syms = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
        if syms:
            where.append("symbol IN (" + ", ".join(sql_quote(s) for s in syms) + ")")

    where_sql = " AND ".join(where)

    long_sql = f"""
    SELECT
        COUNT(*) AS n,
        AVG(fwd_return_1d) AS avg_fwd_return_1d,
        AVG(fwd_return_5d) AS avg_fwd_return_5d,
        AVG(CASE WHEN fwd_return_1d > 0 THEN 1.0 ELSE 0.0 END) AS hit_rate_1d,
        AVG(CASE WHEN fwd_return_5d > 0 THEN 1.0 ELSE 0.0 END) AS hit_rate_5d
    FROM market_features_daily
    WHERE {where_sql}
      AND avg_sentiment_score >= {args.threshold_long}
      AND (fwd_return_1d IS NOT NULL OR fwd_return_5d IS NOT NULL)
    """

    short_sql = f"""
    SELECT
        COUNT(*) AS n,
        AVG(-fwd_return_1d) AS avg_fwd_return_1d,
        AVG(-fwd_return_5d) AS avg_fwd_return_5d,
        AVG(CASE WHEN fwd_return_1d < 0 THEN 1.0 ELSE 0.0 END) AS hit_rate_1d,
        AVG(CASE WHEN fwd_return_5d < 0 THEN 1.0 ELSE 0.0 END) AS hit_rate_5d
    FROM market_features_daily
    WHERE {where_sql}
      AND avg_sentiment_score <= {args.threshold_short}
      AND (fwd_return_1d IS NOT NULL OR fwd_return_5d IS NOT NULL)
    """

    total_sql = f"""
    SELECT
        COUNT(*) AS total_rows,
        MIN(feature_date) AS min_date,
        MAX(feature_date) AS max_date
    FROM market_features_daily
    WHERE {where_sql}
    """

    total_rows, min_date, max_date = con.execute(total_sql).fetchone()
    long_row = con.execute(long_sql).fetchone()
    short_row = con.execute(short_sql).fetchone()

    result = {
        "universe": {
            "total_rows": int(total_rows or 0),
            "min_date": str(min_date) if min_date is not None else None,
            "max_date": str(max_date) if max_date is not None else None,
            "symbols": args.symbols,
        },
        "params": {
            "threshold_long": args.threshold_long,
            "threshold_short": args.threshold_short,
            "date_from": args.date_from,
            "date_to": args.date_to,
        },
        "long_book": {
            "n": int(long_row[0] or 0),
            "avg_fwd_return_1d": float(long_row[1]) if long_row[1] is not None else None,
            "avg_fwd_return_5d": float(long_row[2]) if long_row[2] is not None else None,
            "hit_rate_1d": float(long_row[3]) if long_row[3] is not None else None,
            "hit_rate_5d": float(long_row[4]) if long_row[4] is not None else None,
        },
        "short_book": {
            "n": int(short_row[0] or 0),
            "avg_fwd_return_1d": float(short_row[1]) if short_row[1] is not None else None,
            "avg_fwd_return_5d": float(short_row[2]) if short_row[2] is not None else None,
            "hit_rate_1d": float(short_row[3]) if short_row[3] is not None else None,
            "hit_rate_5d": float(short_row[4]) if short_row[4] is not None else None,
        },
    }

    if args.verbose:
        print(f"[backtest_sentiment_signal] db_path={args.db_path}")
        print(f"[backtest_sentiment_signal] thresholds=({args.threshold_long}, {args.threshold_short})")

    print(json.dumps(result, indent=2))
    con.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
