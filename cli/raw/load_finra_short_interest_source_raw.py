#!/usr/bin/env python3
from __future__ import annotations

import argparse
import glob
from datetime import date, datetime
from pathlib import Path

import pandas as pd

from stock_quant.infrastructure.config.settings_loader import build_app_config
from stock_quant.infrastructure.db.duckdb_session_factory import DuckDbSessionFactory
from stock_quant.infrastructure.db.schema_manager import SchemaManager
from stock_quant.infrastructure.db.table_names import FINRA_SHORT_INTEREST_SOURCE_RAW
from stock_quant.infrastructure.db.unit_of_work import DuckDbUnitOfWork
from stock_quant.infrastructure.providers.finra.finra_provider import FinraProvider


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load FINRA raw short interest rows into DuckDB.")
    parser.add_argument("--db-path", default=None, help="Path to DuckDB database file.")
    parser.add_argument(
        "--source",
        action="append",
        dest="sources",
        default=[],
        help="Source path, glob, file, zip or directory. Repeat this flag for multiple inputs.",
    )
    parser.add_argument(
        "--symbol",
        action="append",
        dest="symbols",
        default=[],
        help="Optional symbol filter.",
    )
    parser.add_argument("--start-date", default=None, help="Optional start date YYYY-MM-DD.")
    parser.add_argument("--end-date", default=None, help="Optional end date YYYY-MM-DD.")
    parser.add_argument("--truncate", action="store_true", help="Delete existing staging rows before insert.")
    parser.add_argument("--replace", action="store_true", help="Alias of --truncate for compatibility.")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose output.")
    return parser.parse_args()


def parse_date(value: str | None):
    if value is None:
        return None
    return date.fromisoformat(value)


def resolve_sources(items: list[str]) -> list[Path]:
    resolved: list[Path] = []
    for item in items:
        expanded = sorted(glob.glob(str(Path(item).expanduser())))
        if expanded:
            resolved.extend(Path(path).expanduser().resolve() for path in expanded)
            continue
        path = Path(item).expanduser().resolve()
        if path.exists():
            resolved.append(path)

    unique: list[Path] = []
    seen: set[str] = set()
    for path in resolved:
        key = str(path)
        if key in seen:
            continue
        seen.add(key)
        unique.append(path)
    return unique


def build_fixture_frame() -> pd.DataFrame:
    now = datetime.utcnow()
    rows = [
        ("AAPL", "2026-02-28", 12000000, 11000000, 48000000.0, 15000000000, None, "regular", "reg_20260228.csv", "2026-02-28", now),
        ("AAPL", "2026-03-15", 12500000, 12000000, 50000000.0, 15000000000, None, "regular", "reg_20260315.csv", "2026-03-15", now),
        ("AAPL", "2026-03-15", 12500000, 12000000, 50000000.0, 15000000000, None, "regular", "reg_20260315.csv", "2026-03-15", now),
        ("MSFT", "2026-02-28", 8300000, 8100000, 26000000.0, 7400000000, None, "regular", "reg_20260228.csv", "2026-02-28", now),
        ("MSFT", "2026-03-15", 8700000, 8300000, 25500000.0, 7400000000, None, "regular", "reg_20260315.csv", "2026-03-15", now),
        ("BABA", "2026-03-15", 14800000, 15000000, 17500000.0, 2600000000, None, "regular", "reg_20260315.csv", "2026-03-15", now),
        ("SPY", "2026-03-15", 50000, 45000, 120000.0, None, None, "regular", "reg_20260315.csv", "2026-03-15", now),
        ("OTCM", "2026-03-15", 10000, 9000, 5000.0, 2000000, None, "otc", "otc_20260315.csv", "2026-03-15", now),
    ]
    return pd.DataFrame(
        rows,
        columns=[
            "symbol",
            "settlement_date",
            "short_interest",
            "previous_short_interest",
            "avg_daily_volume",
            "shares_float",
            "revision_flag",
            "source_market",
            "source_file",
            "source_date",
            "ingested_at",
        ],
    )


def build_source_frame(args: argparse.Namespace) -> pd.DataFrame:
    sources = resolve_sources(args.sources)
    if not sources:
        return build_fixture_frame()

    provider = FinraProvider(source_paths=sources)
    frame = provider.fetch_short_interest_frame(
        symbols=args.symbols or None,
        start_date=parse_date(args.start_date),
        end_date=parse_date(args.end_date),
    )
    if frame.empty:
        return frame

    frame = frame.copy()
    frame.columns = [str(col).strip() for col in frame.columns]
    if "ingested_at" not in frame.columns:
        frame["ingested_at"] = datetime.utcnow()

    required = [
        "symbol",
        "settlement_date",
        "short_interest",
        "previous_short_interest",
        "avg_daily_volume",
        "shares_float",
        "revision_flag",
        "source_market",
        "source_file",
        "source_date",
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

    frame = build_source_frame(args)

    session_factory = DuckDbSessionFactory(config.db_path)
    with DuckDbUnitOfWork(session_factory) as uow:
        schema_manager = SchemaManager(uow)
        schema_manager.validate()

        con = uow.connection
        if con is None:
            raise RuntimeError("missing active DB connection")

        if args.truncate or args.replace:
            con.execute(f"DELETE FROM {FINRA_SHORT_INTEREST_SOURCE_RAW}")

        rows_before = int(con.execute(f"SELECT COUNT(*) FROM {FINRA_SHORT_INTEREST_SOURCE_RAW}").fetchone()[0])

        rows_written = 0
        if not frame.empty:
            con.register("tmp_finra_short_interest_frame", frame)
            try:
                con.execute(
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
                    SELECT
                        UPPER(TRIM(symbol)) AS symbol,
                        CAST(settlement_date AS DATE) AS settlement_date,
                        CAST(short_interest AS BIGINT) AS short_interest,
                        CAST(previous_short_interest AS BIGINT) AS previous_short_interest,
                        CAST(avg_daily_volume AS DOUBLE) AS avg_daily_volume,
                        CAST(shares_float AS BIGINT) AS shares_float,
                        CAST(revision_flag AS VARCHAR) AS revision_flag,
                        CAST(source_market AS VARCHAR) AS source_market,
                        CAST(source_file AS VARCHAR) AS source_file,
                        CAST(source_date AS DATE) AS source_date,
                        CAST(ingested_at AS TIMESTAMP) AS ingested_at
                    FROM tmp_finra_short_interest_frame
                    WHERE symbol IS NOT NULL
                      AND TRIM(symbol) <> ''
                      AND settlement_date IS NOT NULL
                    """
                )
                rows_written = int(len(frame))
            finally:
                try:
                    con.unregister("tmp_finra_short_interest_frame")
                except Exception:
                    pass

        rows_after = int(con.execute(f"SELECT COUNT(*) FROM {FINRA_SHORT_INTEREST_SOURCE_RAW}").fetchone()[0])

    if args.verbose:
        print(f"[load_finra_short_interest_source_raw] db_path={config.db_path}")
        print(f"[load_finra_short_interest_source_raw] source_mode={'provider' if args.sources else 'fixture'}")
        print(f"[load_finra_short_interest_source_raw] rows_before={rows_before}")
        print(f"[load_finra_short_interest_source_raw] rows_written={rows_written}")
        print(f"[load_finra_short_interest_source_raw] rows_after={rows_after}")

    print(f"Loaded finra_short_interest_source_raw rows: total_rows={rows_after}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
