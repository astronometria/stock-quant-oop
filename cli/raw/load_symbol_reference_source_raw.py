#!/usr/bin/env python3
from __future__ import annotations

import argparse
from datetime import date, datetime
from pathlib import Path

import pandas as pd

from stock_quant.infrastructure.config.settings_loader import build_app_config
from stock_quant.infrastructure.db.duckdb_session_factory import DuckDbSessionFactory
from stock_quant.infrastructure.db.schema_manager import SchemaManager
from stock_quant.infrastructure.db.table_names import SYMBOL_REFERENCE_SOURCE_RAW
from stock_quant.infrastructure.db.unit_of_work import DuckDbUnitOfWork
from stock_quant.infrastructure.providers.symbols.symbol_source_loader import SymbolSourceLoader


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Load symbol reference raw rows into DuckDB from fixtures or CSV sources."
    )
    parser.add_argument("--db-path", default=None, help="Path to DuckDB database file.")
    parser.add_argument(
        "--source",
        action="append",
        dest="sources",
        default=[],
        help="Optional CSV source path. Repeat this flag for multiple inputs.",
    )
    parser.add_argument("--truncate", action="store_true", help="Delete existing staging rows before insert.")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose output.")
    return parser.parse_args()


def build_fixture_frame() -> pd.DataFrame:
    now = datetime.utcnow()
    as_of_date = date(2026, 3, 14)

    rows = [
        {
            "symbol": "AAPL",
            "company_name": "Apple Inc.",
            "cik": "0000320193",
            "exchange_raw": "NASDAQGS",
            "security_type_raw": "Common Stock",
            "source_name": "sec_fixture",
            "as_of_date": as_of_date,
            "ingested_at": now,
        },
        {
            "symbol": "AAPL",
            "company_name": "Apple Inc.",
            "cik": "0000320193",
            "exchange_raw": "OTCMKTS",
            "security_type_raw": "Common Stock",
            "source_name": "otc_fixture",
            "as_of_date": as_of_date,
            "ingested_at": now,
        },
        {
            "symbol": "MSFT",
            "company_name": "Microsoft Corporation",
            "cik": "0000789019",
            "exchange_raw": "NASDAQ",
            "security_type_raw": "Common Stock",
            "source_name": "sec_fixture",
            "as_of_date": as_of_date,
            "ingested_at": now,
        },
        {
            "symbol": "BABA",
            "company_name": "Alibaba Group Holding Ltd ADR",
            "cik": "0001577552",
            "exchange_raw": "NYSE",
            "security_type_raw": "ADR",
            "source_name": "sec_fixture",
            "as_of_date": as_of_date,
            "ingested_at": now,
        },
        {
            "symbol": "SPY",
            "company_name": "SPDR S&P 500 ETF Trust",
            "cik": None,
            "exchange_raw": "NYSEARCA",
            "security_type_raw": "ETF",
            "source_name": "sec_fixture",
            "as_of_date": as_of_date,
            "ingested_at": now,
        },
        {
            "symbol": "XYZW",
            "company_name": "XYZW Acquisition Corp Warrant",
            "cik": None,
            "exchange_raw": "NASDAQ",
            "security_type_raw": "Warrant",
            "source_name": "sec_fixture",
            "as_of_date": as_of_date,
            "ingested_at": now,
        },
    ]
    return pd.DataFrame(rows)


def load_source_frame(sources: list[str]) -> pd.DataFrame:
    loader = SymbolSourceLoader(source_paths=sources)
    frame = loader.fetch_symbols_frame()
    if frame.empty:
        return frame

    frame = frame.copy()
    frame.columns = [str(col).strip() for col in frame.columns]

    rename_map = {
        "security_type": "security_type_raw",
        "Security Type": "security_type_raw",
        "source": "source_name",
        "Source": "source_name",
    }
    frame = frame.rename(columns=rename_map)

    if "symbol" in frame.columns:
        frame["symbol"] = frame["symbol"].astype(str).str.strip().str.upper()

    if "source_name" not in frame.columns:
        frame["source_name"] = "csv_source"

    if "as_of_date" not in frame.columns:
        frame["as_of_date"] = date.today()

    if "ingested_at" not in frame.columns:
        frame["ingested_at"] = datetime.utcnow()

    required = [
        "symbol",
        "company_name",
        "cik",
        "exchange_raw",
        "security_type_raw",
        "source_name",
        "as_of_date",
        "ingested_at",
    ]
    for col in required:
        if col not in frame.columns:
            frame[col] = None

    return frame[required].reset_index(drop=True)


def main() -> int:
    args = parse_args()
    config = build_app_config(db_path=args.db_path)
    config.ensure_directories()

    frame = load_source_frame(args.sources) if args.sources else build_fixture_frame()

    session_factory = DuckDbSessionFactory(config.db_path)
    with DuckDbUnitOfWork(session_factory) as uow:
        schema_manager = SchemaManager(uow)
        schema_manager.validate()

        con = uow.connection
        if con is None:
            raise RuntimeError("missing active DB connection")

        if args.truncate:
            con.execute(f"DELETE FROM {SYMBOL_REFERENCE_SOURCE_RAW}")

        rows_before = int(con.execute(f"SELECT COUNT(*) FROM {SYMBOL_REFERENCE_SOURCE_RAW}").fetchone()[0])

        rows_written = 0
        if not frame.empty:
            con.register("tmp_symbol_reference_source_raw_frame", frame)
            try:
                con.execute(
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
                    SELECT
                        UPPER(TRIM(symbol)) AS symbol,
                        CAST(company_name AS VARCHAR) AS company_name,
                        CAST(cik AS VARCHAR) AS cik,
                        CAST(exchange_raw AS VARCHAR) AS exchange_raw,
                        CAST(security_type_raw AS VARCHAR) AS security_type_raw,
                        CAST(source_name AS VARCHAR) AS source_name,
                        CAST(as_of_date AS DATE) AS as_of_date,
                        CAST(ingested_at AS TIMESTAMP) AS ingested_at
                    FROM tmp_symbol_reference_source_raw_frame
                    WHERE symbol IS NOT NULL
                      AND TRIM(symbol) <> ''
                    """
                )
                rows_written = int(len(frame))
            finally:
                try:
                    con.unregister("tmp_symbol_reference_source_raw_frame")
                except Exception:
                    pass

        rows_after = int(con.execute(f"SELECT COUNT(*) FROM {SYMBOL_REFERENCE_SOURCE_RAW}").fetchone()[0])

    if args.verbose:
        print(f"[load_symbol_reference_source_raw] db_path={config.db_path}")
        print(f"[load_symbol_reference_source_raw] source_mode={'csv' if args.sources else 'fixture'}")
        print(f"[load_symbol_reference_source_raw] rows_before={rows_before}")
        print(f"[load_symbol_reference_source_raw] rows_written={rows_written}")
        print(f"[load_symbol_reference_source_raw] rows_after={rows_after}")

    print(f"Loaded symbol_reference_source_raw rows: total_rows={rows_after}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
