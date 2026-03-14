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
    PRICE_SOURCE_DAILY_RAW_YAHOO,
)
from stock_quant.infrastructure.db.unit_of_work import DuckDbUnitOfWork


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Rebuild effective price_source_daily_raw from Stooq bronze + Yahoo daily raw, with Yahoo priority."
    )
    parser.add_argument("--db-path", default=None, help="Path to DuckDB database file.")
    parser.add_argument(
        "--truncate",
        action="store_true",
        help="Delete existing effective staging rows before rebuild.",
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

        yahoo_rows = con.execute(
            f"SELECT COUNT(*) FROM {PRICE_SOURCE_DAILY_RAW_YAHOO}"
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
            WITH stooq_filtered AS (
                SELECT
                    a.symbol,
                    a.price_date,
                    a.open,
                    a.high,
                    a.low,
                    a.close,
                    a.volume,
                    a.source_name,
                    a.ingested_at AS effective_ts,
                    1 AS source_priority,
                    a.source_path AS source_tiebreak
                FROM {PRICE_SOURCE_DAILY_RAW_ALL} a
                INNER JOIN {MARKET_UNIVERSE} mu
                    ON a.symbol = mu.symbol
                WHERE mu.include_in_universe = TRUE
            ),
            yahoo_filtered AS (
                SELECT
                    y.symbol,
                    y.price_date,
                    y.open,
                    y.high,
                    y.low,
                    y.close,
                    y.volume,
                    COALESCE(y.source_name, 'yahoo_daily') AS source_name,
                    y.fetched_at AS effective_ts,
                    2 AS source_priority,
                    'yahoo' AS source_tiebreak
                FROM {PRICE_SOURCE_DAILY_RAW_YAHOO} y
                INNER JOIN {MARKET_UNIVERSE} mu
                    ON y.symbol = mu.symbol
                WHERE mu.include_in_universe = TRUE
            ),
            unioned AS (
                SELECT * FROM stooq_filtered
                UNION ALL
                SELECT * FROM yahoo_filtered
            ),
            ranked AS (
                SELECT
                    symbol,
                    price_date,
                    open,
                    high,
                    low,
                    close,
                    volume,
                    source_name,
                    effective_ts,
                    ROW_NUMBER() OVER (
                        PARTITION BY symbol, price_date
                        ORDER BY
                            source_priority DESC,
                            effective_ts DESC,
                            source_tiebreak DESC
                    ) AS rn
                FROM unioned
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
                effective_ts AS ingested_at
            FROM ranked
            WHERE rn = 1
            """
        )

        effective_rows = con.execute(
            f"SELECT COUNT(*) FROM {PRICE_SOURCE_DAILY_RAW}"
        ).fetchone()[0]

        yahoo_wins = con.execute(
            f"""
            SELECT COUNT(*)
            FROM {PRICE_SOURCE_DAILY_RAW}
            WHERE source_name LIKE 'yahoo%'
            """
        ).fetchone()[0]

    if args.verbose:
        print(f"[rebuild_price_source_daily_raw_effective] db_path={config.db_path}")
        print(f"[rebuild_price_source_daily_raw_effective] bronze_rows={bronze_rows}")
        print(f"[rebuild_price_source_daily_raw_effective] yahoo_rows={yahoo_rows}")
        print(f"[rebuild_price_source_daily_raw_effective] included_symbols={included_symbols}")
        print(f"[rebuild_price_source_daily_raw_effective] effective_rows={effective_rows}")
        print(f"[rebuild_price_source_daily_raw_effective] yahoo_selected_rows={yahoo_wins}")

    print(
        "Rebuilt effective price_source_daily_raw: "
        f"bronze_rows={bronze_rows} yahoo_rows={yahoo_rows} "
        f"included_symbols={included_symbols} effective_rows={effective_rows} "
        f"yahoo_selected_rows={yahoo_wins}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
