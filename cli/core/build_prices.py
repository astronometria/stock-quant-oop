#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import time

from stock_quant.infrastructure.config.settings_loader import build_app_config
from stock_quant.infrastructure.db.duckdb_session_factory import DuckDbSessionFactory
from stock_quant.infrastructure.db.schema_manager import SchemaManager
from stock_quant.infrastructure.db.table_names import (
    PRICE_HISTORY,
    PRICE_LATEST,
    PRICE_SOURCE_DAILY_RAW,
)
from stock_quant.infrastructure.db.unit_of_work import DuckDbUnitOfWork


def parse_args():
    p = argparse.ArgumentParser(description="Build price_history and price_latest incrementally.")
    p.add_argument("--db-path", default=None, help="Path to DuckDB database file.")
    p.add_argument("--verbose", action="store_true")
    return p.parse_args()


def main():
    args = parse_args()
    config = build_app_config(db_path=args.db_path)
    config.ensure_directories()

    started = time.time()
    session_factory = DuckDbSessionFactory(config.db_path)

    with DuckDbUnitOfWork(session_factory) as uow:
        schema_manager = SchemaManager(uow)
        schema_manager.validate()

        con = uow.connection
        if con is None:
            raise RuntimeError("missing active connection")

        raw_bars = int(con.execute(f"SELECT COUNT(*) FROM {PRICE_SOURCE_DAILY_RAW}").fetchone()[0])

        if raw_bars == 0:
            result = {
                "duration_seconds": round(time.time() - started, 6),
                "error_message": "no raw price bars available in price_source_daily_raw",
                "finished_at": None,
                "metrics": {
                    "accepted_bars": 0,
                    "allowed_symbols": 0,
                    "latest_rows": int(con.execute(f"SELECT COUNT(*) FROM {PRICE_LATEST}").fetchone()[0]),
                    "raw_bars": 0,
                    "skipped_invalid": 0,
                    "skipped_not_in_universe": 0,
                    "delta_rows": 0,
                    "changed_symbols": 0,
                },
                "pipeline_name": "build_prices",
                "rows_read": 0,
                "rows_skipped": 0,
                "rows_written": 0,
                "started_at": None,
                "status": "FAILED",
                "warnings": [],
            }
            print(json.dumps(result, indent=2, sort_keys=True))
            return 1

        con.execute("DROP TABLE IF EXISTS price_delta_stage")
        con.execute(
            f"""
            CREATE TEMP TABLE price_delta_stage AS
            SELECT
                r.symbol,
                r.price_date,
                r.open,
                r.high,
                r.low,
                r.close,
                r.volume,
                r.source_name,
                COALESCE(r.ingested_at, CURRENT_TIMESTAMP) AS ingested_at
            FROM {PRICE_SOURCE_DAILY_RAW} r
            LEFT JOIN {PRICE_HISTORY} h
              ON h.symbol = r.symbol
             AND h.price_date = r.price_date
            WHERE
                h.symbol IS NULL
                OR COALESCE(h.open, -1) <> COALESCE(r.open, -1)
                OR COALESCE(h.high, -1) <> COALESCE(r.high, -1)
                OR COALESCE(h.low, -1) <> COALESCE(r.low, -1)
                OR COALESCE(h.close, -1) <> COALESCE(r.close, -1)
                OR COALESCE(h.volume, -1) <> COALESCE(r.volume, -1)
                OR COALESCE(h.source_name, '') <> COALESCE(r.source_name, '')
            """
        )

        delta_rows = int(con.execute("SELECT COUNT(*) FROM price_delta_stage").fetchone()[0])

        if delta_rows == 0:
            latest_rows = int(con.execute(f"SELECT COUNT(*) FROM {PRICE_LATEST}").fetchone()[0])
            result = {
                "duration_seconds": round(time.time() - started, 6),
                "error_message": None,
                "finished_at": None,
                "metrics": {
                    "accepted_bars": 0,
                    "allowed_symbols": int(con.execute(f"SELECT COUNT(DISTINCT symbol) FROM {PRICE_SOURCE_DAILY_RAW}").fetchone()[0]),
                    "latest_rows": latest_rows,
                    "raw_bars": raw_bars,
                    "skipped_invalid": 0,
                    "skipped_not_in_universe": 0,
                    "delta_rows": 0,
                    "changed_symbols": 0,
                },
                "pipeline_name": "build_prices",
                "rows_read": raw_bars,
                "rows_skipped": 0,
                "rows_written": 0,
                "started_at": None,
                "status": "SUCCESS",
                "warnings": [],
            }
            if args.verbose:
                print("[build_prices] no delta rows")
            print(json.dumps(result, indent=2, sort_keys=True))
            return 0

        changed_symbols = int(con.execute("SELECT COUNT(DISTINCT symbol) FROM price_delta_stage").fetchone()[0])

        con.execute(
            f"""
            DELETE FROM {PRICE_HISTORY}
            WHERE (symbol, price_date) IN (
                SELECT symbol, price_date
                FROM price_delta_stage
            )
            """
        )

        con.execute(
            f"""
            INSERT INTO {PRICE_HISTORY} (
                symbol,
                price_date,
                open,
                high,
                low,
                close,
                volume,
                source_name,
                ingested_at
            )
            SELECT
                symbol,
                price_date,
                open,
                high,
                low,
                close,
                volume,
                source_name,
                ingested_at
            FROM price_delta_stage
            """
        )

        con.execute(
            f"""
            DELETE FROM {PRICE_LATEST}
            WHERE symbol IN (
                SELECT DISTINCT symbol
                FROM price_delta_stage
            )
            """
        )

        con.execute(
            f"""
            INSERT INTO {PRICE_LATEST} (
                symbol,
                latest_price_date,
                close,
                volume,
                source_name,
                updated_at
            )
            SELECT
                ranked.symbol,
                ranked.price_date AS latest_price_date,
                ranked.close,
                ranked.volume,
                ranked.source_name,
                CURRENT_TIMESTAMP
            FROM (
                SELECT
                    h.symbol,
                    h.price_date,
                    h.close,
                    h.volume,
                    h.source_name,
                    ROW_NUMBER() OVER (
                        PARTITION BY h.symbol
                        ORDER BY h.price_date DESC
                    ) AS rn
                FROM {PRICE_HISTORY} h
                WHERE h.symbol IN (
                    SELECT DISTINCT symbol
                    FROM price_delta_stage
                )
            ) ranked
            WHERE ranked.rn = 1
            """
        )

        latest_rows = int(con.execute(f"SELECT COUNT(*) FROM {PRICE_LATEST}").fetchone()[0])

    duration = round(time.time() - started, 6)

    if args.verbose:
        print(f"[build_prices] project_root={config.project_root}")
        print(f"[build_prices] db_path={config.db_path}")
        print(f"[build_prices] delta_rows={delta_rows}")
        print(f"[build_prices] changed_symbols={changed_symbols}")

    result = {
        "duration_seconds": duration,
        "error_message": None,
        "finished_at": None,
        "metrics": {
            "accepted_bars": delta_rows,
            "allowed_symbols": int(changed_symbols),
            "latest_rows": latest_rows,
            "raw_bars": raw_bars,
            "skipped_invalid": 0,
            "skipped_not_in_universe": 0,
            "delta_rows": delta_rows,
            "changed_symbols": changed_symbols,
        },
        "pipeline_name": "build_prices",
        "rows_read": raw_bars,
        "rows_skipped": 0,
        "rows_written": delta_rows,
        "started_at": None,
        "status": "SUCCESS",
        "warnings": [],
    }
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
