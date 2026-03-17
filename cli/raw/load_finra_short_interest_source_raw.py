#!/usr/bin/env python3
from __future__ import annotations

"""
Load real FINRA short-interest source rows into DuckDB.

Prod-only behavior
------------------
- no fixture fallback
- can load from explicit files, globs, zip files or directories
- writes both:
  1. finra_short_interest_source_raw
  2. finra_short_interest_sources

Why both tables matter
----------------------
The raw staging table stores row-level source data.
The sources table stores file-level metadata used by the canonical builder to
detect newly staged source files. If the sources table is not populated, the
builder can incorrectly noop even though raw rows exist.
"""

import argparse
import glob
from pathlib import Path

import duckdb
import pandas as pd

from stock_quant.infrastructure.providers.finra.finra_provider import FinraProvider


EXPECTED_COLUMNS = [
    "symbol",
    "settlement_date",
    "short_interest",
    "previous_short_interest",
    "avg_daily_volume",
    "days_to_cover",
    "shares_float",
    "revision_flag",
    "source_market",
    "source_file",
    "source_date",
    "ingested_at",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Load real FINRA raw short interest rows into DuckDB. Prod-only: no fixture fallback."
    )
    parser.add_argument(
        "--db-path",
        default="~/stock-quant-oop/market.duckdb",
        help="Path to DuckDB database file.",
    )
    parser.add_argument(
        "--source",
        dest="sources",
        action="append",
        default=[],
        help="Source path, glob, file, zip or directory. Repeat this flag for multiple inputs.",
    )
    parser.add_argument(
        "--symbol",
        dest="symbols",
        action="append",
        default=[],
        help="Optional symbol filter.",
    )
    parser.add_argument(
        "--start-date",
        default=None,
        help="Optional start date YYYY-MM-DD.",
    )
    parser.add_argument(
        "--end-date",
        default=None,
        help="Optional end date YYYY-MM-DD.",
    )
    parser.add_argument(
        "--truncate",
        action="store_true",
        help="Delete existing staging rows before insert.",
    )
    parser.add_argument(
        "--replace",
        action="store_true",
        help="Alias of --truncate for compatibility.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose output.",
    )
    return parser.parse_args()


def _resolve_sources(raw_inputs: list[str]) -> list[Path]:
    """
    Resolve user-provided source inputs into a deterministic list of existing paths.

    Supports:
    - files
    - directories
    - globs
    """
    resolved: list[Path] = []

    for raw_value in raw_inputs:
        expanded = str(Path(raw_value).expanduser())
        matches = sorted(glob.glob(expanded))
        if matches:
            for match in matches:
                path = Path(match).expanduser().resolve()
                if path.exists():
                    resolved.append(path)
            continue

        path = Path(expanded).expanduser().resolve()
        if path.exists():
            resolved.append(path)

    # Deduplicate while preserving deterministic order.
    seen: set[str] = set()
    unique_resolved: list[Path] = []
    for path in sorted(resolved):
        key = str(path)
        if key in seen:
            continue
        seen.add(key)
        unique_resolved.append(path)

    return unique_resolved


def _ensure_expected_columns(frame: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize DataFrame columns and guarantee the schema required by the raw table.
    """
    normalized = frame.copy()

    for column in EXPECTED_COLUMNS:
        if column not in normalized.columns:
            normalized[column] = None

    normalized = normalized[EXPECTED_COLUMNS].copy()

    if "symbol" in normalized.columns:
        normalized["symbol"] = (
            normalized["symbol"]
            .astype("string")
            .str.strip()
            .str.upper()
        )

    return normalized


def build_source_frame(args: argparse.Namespace) -> pd.DataFrame:
    """
    Build the source frame from real FINRA files only.
    """
    if not args.sources:
        raise ValueError(
            "load_finra_short_interest_source_raw requires at least one --source in prod mode."
        )

    resolved_sources = _resolve_sources(args.sources)
    if not resolved_sources:
        raise ValueError(
            "load_finra_short_interest_source_raw could not resolve any existing path from --source."
        )

    provider = FinraProvider(source_paths=[str(path) for path in resolved_sources])
    frame = provider.fetch_short_interest_frame(
        symbols=args.symbols or None,
        start_date=args.start_date,
        end_date=args.end_date,
    )

    if frame.empty:
        raise ValueError(
            "load_finra_short_interest_source_raw received no rows from the provided FINRA source files."
        )

    return _ensure_expected_columns(frame)


def write_source_frame(
    con: duckdb.DuckDBPyConnection,
    frame: pd.DataFrame,
    *,
    truncate: bool,
) -> tuple[int, int]:
    """
    Write the raw source rows and the file-level source metadata.

    Returns
    -------
    (rows_written_raw, rows_written_sources)
    """
    rows_written_raw = 0
    rows_written_sources = 0

    if frame.empty:
        return rows_written_raw, rows_written_sources

    if truncate:
        con.execute("DELETE FROM finra_short_interest_source_raw")
        con.execute("DELETE FROM finra_short_interest_sources")

    # --------------------------------------------------------------
    # 1) Upsert raw row-level staging
    # --------------------------------------------------------------
    con.register("tmp_finra_short_interest_frame", frame)
    try:
        con.execute(
            """
            CREATE OR REPLACE TEMP TABLE tmp_finra_short_interest_frame_clean AS
            SELECT
                UPPER(TRIM(symbol)) AS symbol,
                CAST(settlement_date AS DATE) AS settlement_date,
                CAST(short_interest AS BIGINT) AS short_interest,
                CAST(previous_short_interest AS BIGINT) AS previous_short_interest,
                CAST(avg_daily_volume AS DOUBLE) AS avg_daily_volume,
                CAST(days_to_cover AS DOUBLE) AS days_to_cover,
                CAST(shares_float AS BIGINT) AS shares_float,
                CAST(revision_flag AS VARCHAR) AS revision_flag,
                LOWER(TRIM(CAST(source_market AS VARCHAR))) AS source_market,
                TRIM(CAST(source_file AS VARCHAR)) AS source_file,
                CAST(source_date AS DATE) AS source_date,
                CAST(ingested_at AS TIMESTAMP) AS ingested_at
            FROM tmp_finra_short_interest_frame
            WHERE symbol IS NOT NULL
              AND TRIM(symbol) <> ''
              AND settlement_date IS NOT NULL
              AND source_file IS NOT NULL
              AND TRIM(source_file) <> ''
              AND source_date IS NOT NULL
            """
        )

        con.execute(
            """
            DELETE FROM finra_short_interest_source_raw AS target
            USING tmp_finra_short_interest_frame_clean AS stage
            WHERE UPPER(TRIM(target.symbol)) = stage.symbol
              AND target.settlement_date = stage.settlement_date
              AND COALESCE(target.source_file, '') = COALESCE(stage.source_file, '')
              AND COALESCE(target.source_market, '') = COALESCE(stage.source_market, '')
            """
        )

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
            FROM (
                SELECT
                    *,
                    ROW_NUMBER() OVER (
                        PARTITION BY symbol, settlement_date, source_file, source_market
                        ORDER BY ingested_at DESC NULLS LAST
                    ) AS rn
                FROM tmp_finra_short_interest_frame_clean
            ) x
            WHERE rn = 1
            """
        )

        rows_written_raw = int(
            con.execute("SELECT COUNT(*) FROM tmp_finra_short_interest_frame_clean").fetchone()[0]
        )

        # ----------------------------------------------------------
        # 2) Upsert file-level source metadata used by canonical build
        # ----------------------------------------------------------
        con.execute(
            """
            CREATE OR REPLACE TEMP TABLE tmp_finra_short_interest_sources_stage AS
            SELECT
                source_file,
                source_market,
                source_date,
                COUNT(*) AS row_count,
                MAX(ingested_at) AS loaded_at
            FROM tmp_finra_short_interest_frame_clean
            GROUP BY 1, 2, 3
            """
        )

        con.execute(
            """
            DELETE FROM finra_short_interest_sources AS target
            USING tmp_finra_short_interest_sources_stage AS stage
            WHERE target.source_file = stage.source_file
              AND COALESCE(target.source_market, '') = COALESCE(stage.source_market, '')
              AND target.source_date = stage.source_date
            """
        )

        con.execute(
            """
            INSERT INTO finra_short_interest_sources (
                source_file,
                source_market,
                source_date,
                row_count,
                loaded_at
            )
            SELECT
                source_file,
                source_market,
                source_date,
                row_count,
                loaded_at
            FROM tmp_finra_short_interest_sources_stage
            """
        )

        rows_written_sources = int(
            con.execute("SELECT COUNT(*) FROM tmp_finra_short_interest_sources_stage").fetchone()[0]
        )
    finally:
        try:
            con.unregister("tmp_finra_short_interest_frame")
        except Exception:
            pass

    return rows_written_raw, rows_written_sources


def main() -> int:
    args = parse_args()
    db_path = str(Path(args.db_path).expanduser().resolve())
    truncate = bool(args.truncate or args.replace)

    frame = build_source_frame(args)

    con = duckdb.connect(db_path)
    try:
        rows_before = int(
            con.execute("SELECT COUNT(*) FROM finra_short_interest_source_raw").fetchone()[0]
        )
        sources_before = int(
            con.execute("SELECT COUNT(*) FROM finra_short_interest_sources").fetchone()[0]
        )

        rows_written, source_rows_written = write_source_frame(
            con,
            frame,
            truncate=truncate,
        )

        rows_after = int(
            con.execute("SELECT COUNT(*) FROM finra_short_interest_source_raw").fetchone()[0]
        )
        sources_after = int(
            con.execute("SELECT COUNT(*) FROM finra_short_interest_sources").fetchone()[0]
        )
    finally:
        con.close()

    if args.verbose:
        print(f"[load_finra_short_interest_source_raw] db_path={db_path}")
        print("[load_finra_short_interest_source_raw] source_mode=real_sources_only")
        print(f"[load_finra_short_interest_source_raw] source_count={len(args.sources)}")
        print(f"[load_finra_short_interest_source_raw] rows_before={rows_before}")
        print(f"[load_finra_short_interest_source_raw] rows_written={rows_written}")
        print(f"[load_finra_short_interest_source_raw] rows_after={rows_after}")
        print(f"[load_finra_short_interest_source_raw] source_rows_before={sources_before}")
        print(f"[load_finra_short_interest_source_raw] source_rows_written={source_rows_written}")
        print(f"[load_finra_short_interest_source_raw] source_rows_after={sources_after}")

    print(f"Loaded finra_short_interest_source_raw rows: total_rows={rows_after}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
