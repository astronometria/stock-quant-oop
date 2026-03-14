#!/usr/bin/env python3
from __future__ import annotations

import argparse
from datetime import datetime

from stock_quant.infrastructure.config.settings_loader import build_app_config
from stock_quant.infrastructure.db.duckdb_session_factory import DuckDbSessionFactory
from stock_quant.infrastructure.db.schema_manager import SchemaManager
from stock_quant.infrastructure.db.table_names import PRICE_SOURCE_DAILY_RAW
from stock_quant.infrastructure.db.unit_of_work import DuckDbUnitOfWork


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load fixture rows into price_source_daily_raw.")
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
        ("AAPL", "2026-03-12", 210.0, 213.5, 209.8, 212.4, 51000000, "stooq_fixture", now),
        ("AAPL", "2026-03-13", 212.6, 214.2, 211.9, 213.8, 49800000, "stooq_fixture", now),
        ("MSFT", "2026-03-12", 401.0, 405.2, 399.8, 404.5, 28000000, "stooq_fixture", now),
        ("MSFT", "2026-03-13", 404.7, 407.4, 403.9, 406.8, 26000000, "stooq_fixture", now),
        ("BABA", "2026-03-12", 91.2, 93.0, 90.8, 92.4, 19000000, "stooq_fixture", now),
        ("BABA", "2026-03-13", 92.6, 94.1, 92.1, 93.7, 17500000, "stooq_fixture", now),
        ("SPY", "2026-03-13", 520.0, 522.1, 519.0, 521.4, 70000000, "stooq_fixture", now),
        ("XYZW", "2026-03-13", 2.1, 2.4, 2.0, 2.3, 80000, "stooq_fixture", now),
        ("OTCM", "2026-03-13", 11.0, 11.1, 10.9, 11.0, 5000, "stooq_fixture", now),
    ]

    session_factory = DuckDbSessionFactory(config.db_path)

    with DuckDbUnitOfWork(session_factory) as uow:
        schema_manager = SchemaManager(uow)
        schema_manager.validate()

        con = uow.connection
        if con is None:
            raise RuntimeError("missing active connection")

        if args.truncate:
            con.execute(f"DELETE FROM {PRICE_SOURCE_DAILY_RAW}")

        con.executemany(
            f"""
            INSERT INTO {PRICE_SOURCE_DAILY_RAW} (
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
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )

        count = con.execute(f"SELECT COUNT(*) FROM {PRICE_SOURCE_DAILY_RAW}").fetchone()[0]

    if args.verbose:
        print(f"[load_price_source_daily_raw] db_path={config.db_path}")
        print(f"[load_price_source_daily_raw] inserted_rows={len(rows)}")
        print(f"[load_price_source_daily_raw] total_rows={count}")

    print(f"Loaded price_source_daily_raw rows: total_rows={count}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
