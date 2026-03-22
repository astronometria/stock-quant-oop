#!/usr/bin/env python3
from __future__ import annotations

import argparse

from stock_quant.infrastructure.config.settings_loader import build_app_config
from stock_quant.infrastructure.db.duckdb_session_factory import DuckDbSessionFactory
from stock_quant.infrastructure.db.schema_manager import SchemaManager
from stock_quant.infrastructure.db.unit_of_work import DuckDbUnitOfWork


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-path", default=None)
    parser.add_argument("--truncate", action="store_true")
    return parser.parse_args()


def main():
    args = parse_args()

    config = build_app_config(db_path=args.db_path)
    session_factory = DuckDbSessionFactory(config.db_path)

    with DuckDbUnitOfWork(session_factory) as uow:
        con = uow.connection
        SchemaManager(uow).validate()

        if args.truncate:
            con.execute("DELETE FROM price_source_daily_raw")

        # 🔥 FIX: normalisation symbole Stooq
        con.execute("""
        INSERT INTO price_source_daily_raw
        SELECT
            replace(b.symbol, '.US', '') AS symbol,
            b.price_date,
            b.open,
            b.high,
            b.low,
            b.close,
            b.volume,
            b.source_name,
            b.ingested_at
        FROM price_source_daily_raw_all b
        JOIN symbol_reference s
            ON replace(b.symbol, '.US', '') = s.symbol
        QUALIFY
            ROW_NUMBER() OVER (
                PARTITION BY replace(b.symbol, '.US', ''), b.price_date
                ORDER BY b.ingested_at DESC
            ) = 1
        """)

        total = con.execute("SELECT COUNT(*) FROM price_source_daily_raw").fetchone()[0]

    print(f"price_source_daily_raw rebuilt rows={total}")


if __name__ == "__main__":
    main()
