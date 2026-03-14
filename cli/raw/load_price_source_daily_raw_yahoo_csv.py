#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

from stock_quant.infrastructure.config.settings_loader import build_app_config
from stock_quant.infrastructure.db.duckdb_session_factory import DuckDbSessionFactory
from stock_quant.infrastructure.db.schema_manager import SchemaManager
from stock_quant.infrastructure.db.table_names import PRICE_SOURCE_DAILY_RAW_YAHOO
from stock_quant.infrastructure.db.unit_of_work import DuckDbUnitOfWork


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Load Yahoo daily CSV into price_source_daily_raw_yahoo."
    )
    parser.add_argument("--db-path", default=None, help="Path to DuckDB database file.")
    parser.add_argument(
        "--csv-path",
        required=True,
        help="Path to Yahoo daily CSV.",
    )
    parser.add_argument(
        "--truncate",
        action="store_true",
        help="Delete existing Yahoo raw rows before load.",
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

    csv_path = Path(args.csv_path).expanduser().resolve()
    if not csv_path.exists():
        raise SystemExit(f"csv file not found: {csv_path}")

    session_factory = DuckDbSessionFactory(config.db_path)

    with DuckDbUnitOfWork(session_factory) as uow:
        schema_manager = SchemaManager(uow)
        schema_manager.validate()

        con = uow.connection
        if con is None:
            raise RuntimeError("missing active connection")

        if args.truncate:
            con.execute(f"DELETE FROM {PRICE_SOURCE_DAILY_RAW_YAHOO}")

        con.execute(
            f"""
            CREATE TEMP TABLE yahoo_csv_stage AS
            SELECT
                UPPER(TRIM(symbol)) AS symbol,
                CAST(price_date AS DATE) AS price_date,
                CAST(open AS DOUBLE) AS open,
                CAST(high AS DOUBLE) AS high,
                CAST(low AS DOUBLE) AS low,
                CAST(close AS DOUBLE) AS close,
                CAST(volume AS BIGINT) AS volume,
                COALESCE(NULLIF(TRIM(source_name), ''), 'yahoo_daily') AS source_name
            FROM read_csv_auto(
                '{csv_path}',
                header = true
            )
            """
        )

        con.execute(
            f"""
            INSERT INTO {PRICE_SOURCE_DAILY_RAW_YAHOO} (
                symbol,
                price_date,
                open,
                high,
                low,
                close,
                volume,
                source_name,
                fetched_at
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
                CURRENT_TIMESTAMP
            FROM yahoo_csv_stage
            """
        )

        total_rows = con.execute(
            f"SELECT COUNT(*) FROM {PRICE_SOURCE_DAILY_RAW_YAHOO}"
        ).fetchone()[0]

    if args.verbose:
        print(f"[load_price_source_daily_raw_yahoo_csv] db_path={config.db_path}")
        print(f"[load_price_source_daily_raw_yahoo_csv] csv_path={csv_path}")
        print(f"[load_price_source_daily_raw_yahoo_csv] total_rows={total_rows}")

    print(
        "Loaded Yahoo daily CSV into price_source_daily_raw_yahoo: "
        f"total_rows={total_rows} csv_path={csv_path}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
