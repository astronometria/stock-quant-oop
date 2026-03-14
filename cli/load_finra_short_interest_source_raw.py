#!/usr/bin/env python3
from __future__ import annotations

import argparse
from datetime import datetime

from stock_quant.infrastructure.config.settings_loader import build_app_config
from stock_quant.infrastructure.db.duckdb_session_factory import DuckDbSessionFactory
from stock_quant.infrastructure.db.schema_manager import SchemaManager
from stock_quant.infrastructure.db.table_names import FINRA_SHORT_INTEREST_SOURCE_RAW
from stock_quant.infrastructure.db.unit_of_work import DuckDbUnitOfWork


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load fixture rows into finra_short_interest_source_raw.")
    parser.add_argument("--db-path", default=None, help="Path to DuckDB database file.")
    parser.add_argument("--truncate", action="store_true", help="Delete existing staging rows before insert.")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose output.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    config = build_app_config(db_path=args.db_path)
    config.ensure_directories()

    now = datetime.utcnow()

    rows = [
        ("AAPL", "2026-02-28", 12000000, 11000000, 48000000.0, 15000000000, None, "regular", "reg_20260228.csv", "2026-02-28", now),
        ("MSFT", "2026-02-28", 8000000, 7600000, 25000000.0, 7500000000, None, "regular", "reg_20260228.csv", "2026-02-28", now),
        ("BABA", "2026-02-28", 15000000, 14000000, 18000000.0, 2600000000, "R", "regular", "reg_20260228.csv", "2026-02-28", now),
        ("SPY", "2026-02-28", 30000000, 29500000, 70000000.0, None, None, "regular", "reg_20260228.csv", "2026-02-28", now),
        ("XYZW", "2026-02-28", 50000, 45000, 120000.0, None, None, "regular", "reg_20260228.csv", "2026-02-28", now),
        ("OTCM", "2026-02-28", 10000, 9000, 5000.0, 2000000, None, "otc", "otc_20260228.csv", "2026-02-28", now),
        ("AAPL", "2026-03-15", 12500000, 12000000, 50000000.0, 15000000000, None, "regular", "reg_20260315.csv", "2026-03-15", now),
        ("BABA", "2026-03-15", 14800000, 15000000, 17500000.0, 2600000000, None, "regular", "reg_20260315.csv", "2026-03-15", now),
    ]

    session_factory = DuckDbSessionFactory(config.db_path)

    with DuckDbUnitOfWork(session_factory) as uow:
        schema_manager = SchemaManager(uow)
        schema_manager.validate()

        con = uow.connection
        if con is None:
            raise RuntimeError("missing active connection")

        if args.truncate:
            con.execute(f"DELETE FROM {FINRA_SHORT_INTEREST_SOURCE_RAW}")

        con.executemany(
            f"""
            INSERT INTO {FINRA_SHORT_INTEREST_SOURCE_RAW} (
                symbol,
                settlement_date,
                short_interest,
                previous_short_interest,
                avg_daily_volume,
                shares_float,
                revision_flag,
                source_market,
                source_file,
                source_date,
                ingested_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )

        count = con.execute(f"SELECT COUNT(*) FROM {FINRA_SHORT_INTEREST_SOURCE_RAW}").fetchone()[0]

    if args.verbose:
        print(f"[load_finra_short_interest_source_raw] db_path={config.db_path}")
        print(f"[load_finra_short_interest_source_raw] inserted_rows={len(rows)}")
        print(f"[load_finra_short_interest_source_raw] total_rows={count}")

    print(f"Loaded finra_short_interest_source_raw rows: total_rows={count}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
