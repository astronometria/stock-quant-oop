#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Show stock-quant-oop core pipeline table status.")
    parser.add_argument("--db-path", default="~/stock-quant-oop/market.duckdb", help="Path to DuckDB database file.")
    return parser.parse_args()


def safe_scalar(con: duckdb.DuckDBPyConnection, sql: str):
    try:
        row = con.execute(sql).fetchone()
        return row[0] if row else None
    except Exception as exc:
        return f"ERROR: {exc}"


def main() -> int:
    args = parse_args()
    db_path = str(Path(args.db_path).expanduser().resolve())
    con = duckdb.connect(db_path)

    status = {
        "db_path": db_path,
        "tables": {
            "symbol_reference_source_raw": safe_scalar(con, "SELECT COUNT(*) FROM symbol_reference_source_raw"),
            "market_universe": safe_scalar(con, "SELECT COUNT(*) FROM market_universe"),
            "market_universe_included": safe_scalar(con, "SELECT COUNT(*) FROM market_universe WHERE include_in_universe = TRUE"),
            "market_universe_conflicts": safe_scalar(con, "SELECT COUNT(*) FROM market_universe_conflicts"),
            "symbol_reference": safe_scalar(con, "SELECT COUNT(*) FROM symbol_reference"),
            "price_source_daily_raw_all": safe_scalar(con, "SELECT COUNT(*) FROM price_source_daily_raw_all"),
            "price_source_daily_raw_yahoo": safe_scalar(con, "SELECT COUNT(*) FROM price_source_daily_raw_yahoo"),
            "price_source_daily_raw": safe_scalar(con, "SELECT COUNT(*) FROM price_source_daily_raw"),
            "price_history": safe_scalar(con, "SELECT COUNT(*) FROM price_history"),
            "price_latest": safe_scalar(con, "SELECT COUNT(*) FROM price_latest"),
            "finra_short_interest_source_raw": safe_scalar(con, "SELECT COUNT(*) FROM finra_short_interest_source_raw"),
            "finra_short_interest_history": safe_scalar(con, "SELECT COUNT(*) FROM finra_short_interest_history"),
            "finra_short_interest_latest": safe_scalar(con, "SELECT COUNT(*) FROM finra_short_interest_latest"),
            "finra_short_interest_sources": safe_scalar(con, "SELECT COUNT(*) FROM finra_short_interest_sources"),
            "news_source_raw": safe_scalar(con, "SELECT COUNT(*) FROM news_source_raw"),
            "news_articles_raw": safe_scalar(con, "SELECT COUNT(*) FROM news_articles_raw"),
            "news_symbol_candidates": safe_scalar(con, "SELECT COUNT(*) FROM news_symbol_candidates"),
        },
        "samples": {
            "market_universe_included_symbols": con.execute(
                """
                SELECT COALESCE(string_agg(symbol, ', ' ORDER BY symbol), '')
                FROM market_universe
                WHERE include_in_universe = TRUE
                """
            ).fetchone()[0],
            "price_latest_symbols": con.execute(
                """
                SELECT COALESCE(string_agg(symbol, ', ' ORDER BY symbol), '')
                FROM price_latest
                """
            ).fetchone()[0],
            "bronze_price_symbols": con.execute(
                """
                SELECT COALESCE(string_agg(symbol, ', ' ORDER BY symbol), '')
                FROM (
                    SELECT DISTINCT symbol
                    FROM price_source_daily_raw_all
                    ORDER BY symbol
                    LIMIT 20
                )
                """
            ).fetchone()[0],
            "yahoo_price_symbols": con.execute(
                """
                SELECT COALESCE(string_agg(symbol, ', ' ORDER BY symbol), '')
                FROM (
                    SELECT DISTINCT symbol
                    FROM price_source_daily_raw_yahoo
                    ORDER BY symbol
                    LIMIT 20
                )
                """
            ).fetchone()[0],
            "finra_latest_symbols": con.execute(
                """
                SELECT COALESCE(string_agg(symbol, ', ' ORDER BY symbol), '')
                FROM finra_short_interest_latest
                """
            ).fetchone()[0],
            "news_candidate_pairs": con.execute(
                """
                SELECT COALESCE(string_agg(CAST(raw_id AS VARCHAR) || ':' || symbol, ', ' ORDER BY raw_id, symbol), '')
                FROM news_symbol_candidates
                """
            ).fetchone()[0],
        },
    }

    con.close()
    print(json.dumps(status, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
