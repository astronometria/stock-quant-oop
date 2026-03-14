#!/usr/bin/env python3
from __future__ import annotations

import argparse

from stock_quant.infrastructure.config.settings_loader import build_app_config
from stock_quant.infrastructure.db.duckdb_session_factory import DuckDbSessionFactory
from stock_quant.infrastructure.db.schema_manager import SchemaManager
from stock_quant.infrastructure.db.table_names import (
    MARKET_UNIVERSE,
    PRICE_SOURCE_DAILY_RAW,
    PRICE_SOURCE_DAILY_RAW_ALL,
)
from stock_quant.infrastructure.db.unit_of_work import DuckDbUnitOfWork


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Filter bronze Stooq price_source_daily_raw_all into price_source_daily_raw using market_universe."
    )
    parser.add_argument("--db-path", default=None, help="Path to DuckDB database file.")
    parser.add_argument(
        "--truncate",
        action="store_true",
        help="Delete existing staging rows before insert.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose output.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    config = build_app_config(db_path=args.db_path)
    config.ensure_directories()

    session_factory = DuckDbSessionFactory(config.db_path)

    with DuckDbUnitOfWork(session_factory) as uow:
        schema_manager = SchemaManager(uow)
        schema_manager.validate()

        con = uow.connection
        if con is None:
            raise RuntimeError("missing active connection")

        bronze_rows = con.execute(
            f"SELECT COUNT(*) FROM {PRICE_SOURCE_DAILY_RAW_ALL}"
        ).fetchone()[0]
        included_symbols = con.execute(
            f"SELECT COUNT(*) FROM {MARKET_UNIVERSE} WHERE include_in_universe = TRUE"
        ).fetchone()[0]

        if args.truncate:
            con.execute(f"DELETE FROM {PRICE_SOURCE_DAILY_RAW}")

        con.execute(
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
            FROM (
                SELECT
                    a.symbol,
                    a.price_date,
                    a.open,
                    a.high,
                    a.low,
                    a.close,
                    a.volume,
                    a.source_name,
                    a.ingested_at,
                    ROW_NUMBER() OVER (
                        PARTITION BY a.symbol, a.price_date
                        ORDER BY a.ingested_at DESC, a.source_path DESC
                    ) AS rn
                FROM {PRICE_SOURCE_DAILY_RAW_ALL} a
                INNER JOIN {MARKET_UNIVERSE} mu
                    ON a.symbol = mu.symbol
                WHERE mu.include_in_universe = TRUE
            ) ranked
            WHERE rn = 1
            """
        )

        staging_rows = con.execute(
            f"SELECT COUNT(*) FROM {PRICE_SOURCE_DAILY_RAW}"
        ).fetchone()[0]

    if args.verbose:
        print(f"[filter_price_source_daily_raw_from_all] db_path={config.db_path}")
        print(f"[filter_price_source_daily_raw_from_all] bronze_rows={bronze_rows}")
        print(f"[filter_price_source_daily_raw_from_all] included_symbols={included_symbols}")
        print(f"[filter_price_source_daily_raw_from_all] staging_rows={staging_rows}")

    print(
        "Filtered price_source_daily_raw_all -> price_source_daily_raw: "
        f"bronze_rows={bronze_rows} included_symbols={included_symbols} staging_rows={staging_rows}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
