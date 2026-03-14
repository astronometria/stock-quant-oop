#!/usr/bin/env python3
from __future__ import annotations

import argparse
from datetime import datetime

from stock_quant.infrastructure.config.settings_loader import build_app_config
from stock_quant.infrastructure.db.duckdb_session_factory import DuckDbSessionFactory
from stock_quant.infrastructure.db.schema_manager import SchemaManager
from stock_quant.infrastructure.db.table_names import SYMBOL_REFERENCE_SOURCE_RAW
from stock_quant.infrastructure.db.unit_of_work import DuckDbUnitOfWork


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load fixture rows into symbol_reference_source_raw.")
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
        ("AAPL", "Apple Inc.", "0000320193", "NASDAQGS", "Common Stock", "sec_fixture", "2026-03-14", now),
        ("MSFT", "Microsoft Corporation", "0000789019", "NASDAQ", "Common Stock", "sec_fixture", "2026-03-14", now),
        ("AAPL", "Apple Inc. duplicate worse row", None, "OTCQX", "Common Stock", "otc_fixture", "2026-03-14", now),
        ("SPY", "SPDR S&P 500 ETF Trust", None, "NYSEARCA", "ETF", "etf_fixture", "2026-03-14", now),
        ("XYZW", "Example Warrant Corp Warrant", None, "NASDAQ", "Warrant", "warrant_fixture", "2026-03-14", now),
        ("BABA", "Alibaba Group Holding Ltd ADR", None, "NYSE", "ADR", "adr_fixture", "2026-03-14", now),
    ]

    session_factory = DuckDbSessionFactory(config.db_path)

    with DuckDbUnitOfWork(session_factory) as uow:
        schema_manager = SchemaManager(uow)
        schema_manager.validate()

        con = uow.connection
        if con is None:
            raise RuntimeError("missing active connection")

        if args.truncate:
            con.execute(f"DELETE FROM {SYMBOL_REFERENCE_SOURCE_RAW}")

        con.executemany(
            f"""
            INSERT INTO {SYMBOL_REFERENCE_SOURCE_RAW} (
                symbol,
                company_name,
                cik,
                exchange_raw,
                security_type_raw,
                source_name,
                as_of_date,
                ingested_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )

        count = con.execute(f"SELECT COUNT(*) FROM {SYMBOL_REFERENCE_SOURCE_RAW}").fetchone()[0]

    if args.verbose:
        print(f"[load_symbol_reference_source_raw] db_path={config.db_path}")
        print(f"[load_symbol_reference_source_raw] inserted_rows={len(rows)}")
        print(f"[load_symbol_reference_source_raw] total_rows={count}")

    print(f"Loaded symbol_reference_source_raw rows: total_rows={count}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
