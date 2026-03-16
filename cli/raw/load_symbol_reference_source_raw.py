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
        description=(
            "Load real symbol reference raw rows into DuckDB from provided source files. "
            "Prod-only: no implicit fixture fallback."
        )
    )
    parser.add_argument("--db-path", default=None, help="Path to DuckDB database file.")
    parser.add_argument(
        "--source",
        action="append",
        dest="sources",
        default=[],
        help="Source file path. Repeat this flag for multiple inputs.",
    )
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


def _normalize_input_frame(frame: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize provider output into the exact canonical raw schema expected by DuckDB.

    Important:
    - We never synthesize fake rows here.
    - Missing columns are added as NULL to preserve SQL-first loading.
    - Symbol normalization happens here once, before staging into DuckDB.
    """
    frame = frame.copy()
    frame.columns = [str(col).strip() for col in frame.columns]

    rename_map = {
        "security_type": "security_type_raw",
        "Security Type": "security_type_raw",
        "source": "source_name",
        "Source": "source_name",
        "exchange": "exchange_raw",
        "Exchange": "exchange_raw",
    }
    frame = frame.rename(columns=rename_map)

    if "symbol" in frame.columns:
        frame["symbol"] = frame["symbol"].astype(str).str.strip().str.upper()

    if "source_name" not in frame.columns:
        frame["source_name"] = "external_source"

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


def load_source_frame(sources: list[str]) -> pd.DataFrame:
    """
    Load real source data only.

    Prod rule:
    - no source => hard failure
    - empty provider result => hard failure
    """
    if not sources:
        raise SystemExit(
            "load_symbol_reference_source_raw requires at least one --source in prod mode."
        )

    loader = SymbolSourceLoader(source_paths=sources)
    frame = loader.fetch_symbols_frame()

    if frame is None or frame.empty:
        raise SystemExit(
            "load_symbol_reference_source_raw received no rows from the provided source files."
        )

    return _normalize_input_frame(frame)


def main() -> int:
    args = parse_args()
    config = build_app_config(db_path=args.db_path)
    config.ensure_directories()

    frame = load_source_frame(args.sources)

    session_factory = DuckDbSessionFactory(config.db_path)
    with DuckDbUnitOfWork(session_factory) as uow:
        schema_manager = SchemaManager(uow)
        schema_manager.validate()

        con = uow.connection
        if con is None:
            raise RuntimeError("missing active DB connection")

        if args.truncate:
            con.execute(f"DELETE FROM {SYMBOL_REFERENCE_SOURCE_RAW}")

        rows_before = int(
            con.execute(f"SELECT COUNT(*) FROM {SYMBOL_REFERENCE_SOURCE_RAW}").fetchone()[0]
        )

        rows_written = 0
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

        rows_after = int(
            con.execute(f"SELECT COUNT(*) FROM {SYMBOL_REFERENCE_SOURCE_RAW}").fetchone()[0]
        )

    if args.verbose:
        print(f"[load_symbol_reference_source_raw] db_path={config.db_path}")
        print("[load_symbol_reference_source_raw] source_mode=real_sources_only")
        print(f"[load_symbol_reference_source_raw] source_count={len(args.sources)}")
        print(f"[load_symbol_reference_source_raw] rows_before={rows_before}")
        print(f"[load_symbol_reference_source_raw] rows_written={rows_written}")
        print(f"[load_symbol_reference_source_raw] rows_after={rows_after}")

    print(f"Loaded symbol_reference_source_raw rows: total_rows={rows_after}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
