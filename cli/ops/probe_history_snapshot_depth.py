#!/usr/bin/env python3
from __future__ import annotations

# =============================================================================
# probe_history_snapshot_depth.py
#
# Objectif:
# - suivre la profondeur réelle de la couche metadata/history
# - voir si la base devient vraiment PIT-ready avec le temps
# =============================================================================

import argparse
import json
from pathlib import Path

import duckdb


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Probe metadata/history snapshot depth.")
    parser.add_argument("--db-path", required=True)
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
        summary = {
            "db_path": str(db_path),
            "snapshot_depth": {
                "symbol_reference_source_raw_distinct_dates": safe_scalar(
                    con, "SELECT COUNT(DISTINCT as_of_date) FROM symbol_reference_source_raw"
                ),
                "symbol_reference_source_raw_min_date": safe_scalar(
                    con, "SELECT MIN(as_of_date) FROM symbol_reference_source_raw"
                ),
                "symbol_reference_source_raw_max_date": safe_scalar(
                    con, "SELECT MAX(as_of_date) FROM symbol_reference_source_raw"
                ),
                "market_universe_distinct_dates": safe_scalar(
                    con, "SELECT COUNT(DISTINCT as_of_date) FROM market_universe"
                ),
                "market_universe_min_date": safe_scalar(
                    con, "SELECT MIN(as_of_date) FROM market_universe"
                ),
                "market_universe_max_date": safe_scalar(
                    con, "SELECT MAX(as_of_date) FROM market_universe"
                ),
            },
            "history_tables": {
                "listing_status_history_rows": safe_scalar(
                    con, "SELECT COUNT(*) FROM listing_status_history"
                ),
                "market_universe_history_rows": safe_scalar(
                    con, "SELECT COUNT(*) FROM market_universe_history"
                ),
                "symbol_reference_history_rows": safe_scalar(
                    con, "SELECT COUNT(*) FROM symbol_reference_history"
                ),
                "universe_membership_history_rows": safe_scalar(
                    con, "SELECT COUNT(*) FROM universe_membership_history"
                ),
                "research_universe_whitelist_20d_pit_rows": safe_scalar(
                    con, "SELECT COUNT(*) FROM research_universe_whitelist_20d_pit"
                ),
            },
        }

        print(json.dumps(summary, indent=2, default=str))
        return 0
    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
