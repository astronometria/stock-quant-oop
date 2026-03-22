#!/usr/bin/env python3
from __future__ import annotations

# =============================================================================
# Build a stricter quality universe for 20D ranking research.
#
# Why:
# The audit showed that the model repeatedly selects noisy / fragile names.
# We therefore build a cleaner research universe with tougher tradability and
# stability filters before retraining.
#
# Design:
# - SQL-first
# - canonical adjusted prices from price_bars_adjusted
# - visible metrics output
# - heavily commented for future developers
# =============================================================================

import argparse
import json
from pathlib import Path

import duckdb


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-path", required=True)
    parser.add_argument("--min-price", type=float, default=10.0)
    parser.add_argument("--min-dollar-volume", type=float, default=5_000_000.0)
    parser.add_argument("--min-volume", type=float, default=300_000.0)
    parser.add_argument("--max-volatility-20", type=float, default=0.12)
    parser.add_argument("--exclude-symbol", action="append", default=["ZVZZT"])
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    db_path = Path(args.db_path).expanduser().resolve()

    excluded_sql = ""
    if args.exclude_symbol:
        quoted = ", ".join("'" + s.replace("'", "''") + "'" for s in sorted(set(args.exclude_symbol)))
        excluded_sql = f"AND f.symbol NOT IN ({quoted})"

    con = duckdb.connect(str(db_path))
    try:
        con.execute(f"""
        CREATE OR REPLACE TABLE research_universe_quality_20d AS
        WITH px AS (
            SELECT
                symbol,
                bar_date,
                adj_close,
                volume,
                adj_close * volume AS dollar_volume
            FROM price_bars_adjusted
            WHERE adj_close IS NOT NULL
              AND adj_close > 0
              AND volume IS NOT NULL
              AND volume > 0
        )
        SELECT
            f.instrument_id,
            f.company_id,
            f.symbol,
            f.as_of_date,
            p.adj_close AS exec_close,
            p.volume AS exec_volume,
            p.dollar_volume AS exec_dollar_volume,
            f.volatility_20,
            f.volatility_60,
            f.atr_pct_14
        FROM research_features_daily f
        INNER JOIN px p
            ON f.symbol = p.symbol
           AND f.as_of_date = p.bar_date
        WHERE p.adj_close >= {float(args.min_price)}
          AND p.dollar_volume >= {float(args.min_dollar_volume)}
          AND p.volume >= {float(args.min_volume)}
          AND COALESCE(f.volatility_20, 0.0) <= {float(args.max_volatility_20)}
          {excluded_sql}
        """)

        summary = con.execute("""
        SELECT
            COUNT(*) AS rows_total,
            COUNT(DISTINCT symbol) AS symbols,
            MIN(as_of_date) AS min_date,
            MAX(as_of_date) AS max_date,
            AVG(exec_close) AS avg_close,
            AVG(exec_dollar_volume) AS avg_dollar_volume,
            AVG(exec_volume) AS avg_volume,
            AVG(volatility_20) AS avg_volatility_20
        FROM research_universe_quality_20d
        """).fetchone()

        print(json.dumps({
            "table_name": "research_universe_quality_20d",
            "rows_total": int(summary[0]),
            "symbols": int(summary[1]),
            "min_date": str(summary[2]) if summary[2] is not None else None,
            "max_date": str(summary[3]) if summary[3] is not None else None,
            "avg_close": float(summary[4]) if summary[4] is not None else None,
            "avg_dollar_volume": float(summary[5]) if summary[5] is not None else None,
            "avg_volume": float(summary[6]) if summary[6] is not None else None,
            "avg_volatility_20": float(summary[7]) if summary[7] is not None else None,
            "min_price": float(args.min_price),
            "min_dollar_volume": float(args.min_dollar_volume),
            "min_volume": float(args.min_volume),
            "max_volatility_20": float(args.max_volatility_20),
            "excluded_symbols": sorted(set(args.exclude_symbol)),
        }, indent=2))
        return 0
    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
