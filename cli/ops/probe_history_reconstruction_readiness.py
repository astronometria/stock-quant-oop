#!/usr/bin/env python3
from __future__ import annotations

"""
Probe d'état pour savoir si la couche history/PIT est scientifiquement exploitable.

Philosophie
-----------
- pas d'hypothèse cachée
- métriques simples et auditables
- verdict lisible: LEVEL_0 / LEVEL_1 / LEVEL_2
"""

import argparse
import json
from pathlib import Path

import duckdb


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Probe history reconstruction readiness for scientific PIT usage."
    )
    parser.add_argument("--db-path", required=True, help="Path to DuckDB database.")
    parser.add_argument("--min-history-dates", type=int, default=30)
    parser.add_argument("--min-overlap-symbols", type=int, default=1000)
    return parser.parse_args()


def safe_scalar(con: duckdb.DuckDBPyConnection, sql: str):
    try:
        row = con.execute(sql).fetchone()
        return None if row is None else row[0]
    except Exception:
        return None


def main() -> int:
    args = parse_args()
    db_path = Path(args.db_path).expanduser().resolve()
    con = duckdb.connect(str(db_path))

    try:
        symbol_reference_dates = safe_scalar(
            con,
            "SELECT COUNT(DISTINCT as_of_date) FROM symbol_reference_source_raw",
        )
        market_universe_dates = safe_scalar(
            con,
            "SELECT COUNT(DISTINCT as_of_date) FROM market_universe",
        )
        listing_rows = safe_scalar(
            con,
            "SELECT COUNT(*) FROM listing_status_history",
        )
        market_hist_rows = safe_scalar(
            con,
            "SELECT COUNT(*) FROM market_universe_history",
        )
        symbol_hist_rows = safe_scalar(
            con,
            "SELECT COUNT(*) FROM symbol_reference_history",
        )
        membership_rows = safe_scalar(
            con,
            "SELECT COUNT(*) FROM universe_membership_history",
        )
        pit_rows = safe_scalar(
            con,
            "SELECT COUNT(*) FROM research_universe_whitelist_20d_pit",
        )
        quality_symbols = safe_scalar(
            con,
            "SELECT COUNT(DISTINCT symbol) FROM research_universe_quality_20d",
        )
        membership_symbols = safe_scalar(
            con,
            "SELECT COUNT(DISTINCT symbol) FROM universe_membership_history",
        )
        overlap_symbols = safe_scalar(
            con,
            """
            SELECT COUNT(DISTINCT q.symbol)
            FROM research_universe_quality_20d q
            INNER JOIN universe_membership_history m
                ON q.symbol = m.symbol
            """
        )

        observed_depth = max(symbol_reference_dates or 0, market_universe_dates or 0)

        if observed_depth >= args.min_history_dates and (overlap_symbols or 0) >= args.min_overlap_symbols:
            level = "PIT_LEVEL_2"
            verdict = "strict_pit_ready"
        elif observed_depth >= 3 and (overlap_symbols or 0) > 0:
            level = "PIT_LEVEL_1"
            verdict = "partial_history_reconstruction_usable_but_not_strict"
        else:
            level = "PIT_LEVEL_0"
            verdict = "insufficient_history_depth"

        print(json.dumps(
            {
                "db_path": str(db_path),
                "thresholds": {
                    "min_history_dates": args.min_history_dates,
                    "min_overlap_symbols": args.min_overlap_symbols,
                },
                "metrics": {
                    "symbol_reference_source_raw_distinct_dates": symbol_reference_dates,
                    "market_universe_distinct_dates": market_universe_dates,
                    "listing_status_history_rows": listing_rows,
                    "market_universe_history_rows": market_hist_rows,
                    "symbol_reference_history_rows": symbol_hist_rows,
                    "universe_membership_history_rows": membership_rows,
                    "research_universe_whitelist_20d_pit_rows": pit_rows,
                    "research_universe_quality_20d_symbols": quality_symbols,
                    "universe_membership_history_symbols": membership_symbols,
                    "overlap_symbols": overlap_symbols,
                },
                "verdict": {
                    "pit_level": level,
                    "status": verdict,
                },
            },
            indent=2,
        ))
        return 0
    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
