#!/usr/bin/env python3
from __future__ import annotations

import argparse
import glob
import json
from datetime import date
from pathlib import Path

from stock_quant.infrastructure.config.settings_loader import build_app_config
from stock_quant.infrastructure.db.duckdb_session_factory import DuckDbSessionFactory
from stock_quant.infrastructure.db.schema_manager import SchemaManager
from stock_quant.infrastructure.db.unit_of_work import DuckDbUnitOfWork
from stock_quant.infrastructure.providers.finra.finra_provider import FinraProvider


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load FINRA raw short interest files into DuckDB.")
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
    parser.add_argument(
        "--replace",
        action="store_true",
        help="Delete existing finra_short_interest_source_raw rows before insert.",
    )
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


def main() -> int:
    args = parse_args()
    config = build_app_config(db_path=args.db_path)
    config.ensure_directories()

    sources = resolve_sources(args.sources)
    if not sources:
        raise SystemExit("no FINRA short interest source paths resolved")

    if args.verbose:
        print(f"[load_finra_short_interest_source_raw] project_root={config.project_root}")
        print(f"[load_finra_short_interest_source_raw] db_path={config.db_path}")
        print(f"[load_finra_short_interest_source_raw] source_count={len(sources)}")
        print(f"[load_finra_short_interest_source_raw] symbols_count={len(args.symbols)}")

    provider = FinraProvider(source_paths=sources)
    frame = provider.fetch_short_interest_frame(
        symbols=args.symbols or None,
        start_date=parse_date(args.start_date),
        end_date=parse_date(args.end_date),
    )

    session_factory = DuckDbSessionFactory(config.db_path)
    with DuckDbUnitOfWork(session_factory) as uow:
        schema_manager = SchemaManager(uow)
        schema_manager.validate()

        con = uow.connection
        if con is None:
            raise RuntimeError("missing active DB connection")

        if args.replace:
            con.execute("DELETE FROM finra_short_interest_source_raw")

        rows_before = int(
            con.execute("SELECT COUNT(*) FROM finra_short_interest_source_raw").fetchone()[0]
        )

        rows_written = 0
        if not frame.empty:
            con.register("tmp_finra_short_interest_frame", frame)
            try:
                con.execute(
                    """
                    INSERT INTO finra_short_interest_source_raw (
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
                    """
                )
            finally:
                con.unregister("tmp_finra_short_interest_frame")

            rows_after = int(
                con.execute("SELECT COUNT(*) FROM finra_short_interest_source_raw").fetchone()[0]
            )
            rows_written = rows_after - rows_before
        else:
            rows_after = rows_before

    print(
        json.dumps(
            {
                "source_count": len(sources),
                "rows_input": int(len(frame)),
                "rows_written": int(rows_written),
                "table_total_rows": int(rows_after),
                "min_settlement_date": (
                    str(frame["settlement_date"].min()) if not frame.empty else None
                ),
                "max_settlement_date": (
                    str(frame["settlement_date"].max()) if not frame.empty else None
                ),
                "source_files": (
                    int(frame["source_file"].nunique()) if not frame.empty else 0
                ),
                "sample_sources": (
                    sorted(frame["source_file"].dropna().astype(str).unique().tolist())[:20]
                    if not frame.empty
                    else []
                ),
            },
            indent=2,
            sort_keys=True,
            default=str,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
