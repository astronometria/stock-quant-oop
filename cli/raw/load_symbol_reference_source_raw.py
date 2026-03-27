#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
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
            "Load symbol reference raw rows into DuckDB from provided source files. "
            "Scientific mode: snapshots must carry a real as_of_date."
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
        help="Delete existing staging rows before insert. Use with caution.",
    )
    parser.add_argument(
        "--as-of-date",
        default=None,
        help="Force a single snapshot date YYYY-MM-DD for all loaded rows.",
    )
    parser.add_argument(
        "--infer-as-of-date-from-path",
        action="store_true",
        help="Infer snapshot date from each source path if source data lacks as_of_date.",
    )
    parser.add_argument("--verbose", action="store_true", help="Enable verbose output.")
    return parser.parse_args()


def _extract_date_from_path(path_str: str) -> date | None:
    """
    Extract a snapshot date from a file path.
    Supported examples:
    - 2026-03-22
    - 20260322
    """
    path_str = str(path_str)

    m = re.search(r"(20\d{2})[-_]?(\d{2})[-_]?(\d{2})", path_str)
    if not m:
        return None

    yyyy, mm, dd = m.group(1), m.group(2), m.group(3)
    try:
        return date(int(yyyy), int(mm), int(dd))
    except ValueError:
        return None


def _normalize_single_source_frame(
    frame: pd.DataFrame,
    *,
    source_path: str,
    forced_as_of_date: date | None,
    infer_as_of_date_from_path: bool,
) -> pd.DataFrame:
    """
    Normalize one source frame into the exact canonical raw schema expected by DuckDB.

    Important:
    - no fake rows
    - no silent fallback to date.today()
    - if as_of_date is absent, we require either:
        * --as-of-date
        * or --infer-as-of-date-from-path with a parseable path
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
        frame["source_name"] = Path(source_path).name

    resolved_as_of_date: date | None = None
    if forced_as_of_date is not None:
        resolved_as_of_date = forced_as_of_date
    elif "as_of_date" in frame.columns and frame["as_of_date"].notna().any():
        resolved_as_of_date = None
    elif infer_as_of_date_from_path:
        resolved_as_of_date = _extract_date_from_path(source_path)

    if "as_of_date" not in frame.columns or frame["as_of_date"].isna().all():
        if resolved_as_of_date is None:
            raise SystemExit(
                f"Missing as_of_date for source {source_path}. "
                "Provide --as-of-date or use --infer-as-of-date-from-path with a dated file path."
            )
        frame["as_of_date"] = resolved_as_of_date

    if "ingested_at" not in frame.columns:
        frame["ingested_at"] = datetime.utcnow()

    frame["as_of_date"] = pd.to_datetime(frame["as_of_date"]).dt.date

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


def load_source_frame(
    sources: list[str],
    *,
    forced_as_of_date: date | None,
    infer_as_of_date_from_path: bool,
) -> pd.DataFrame:
    """
    Load real source data only, one file at a time, so snapshot date inference can be accurate.
    """
    if not sources:
        raise SystemExit(
            "load_symbol_reference_source_raw requires at least one --source in prod mode."
        )

    normalized_frames: list[pd.DataFrame] = []

    for src in sources:
        loader = SymbolSourceLoader(source_paths=[src])
        frame = loader.fetch_symbols_frame()
        if frame is None or frame.empty:
            raise SystemExit(
                f"load_symbol_reference_source_raw received no rows from source: {src}"
            )
        normalized = _normalize_single_source_frame(
            frame,
            source_path=src,
            forced_as_of_date=forced_as_of_date,
            infer_as_of_date_from_path=infer_as_of_date_from_path,
        )
        normalized_frames.append(normalized)

    combined = pd.concat(normalized_frames, ignore_index=True)

    # Stable dedupe before staging.
    combined = combined.drop_duplicates(
        subset=[
            "symbol",
            "company_name",
            "cik",
            "exchange_raw",
            "security_type_raw",
            "source_name",
            "as_of_date",
        ],
        keep="last",
    ).reset_index(drop=True)

    return combined


def main() -> int:
    args = parse_args()

    forced_as_of_date = None
    if args.as_of_date:
        forced_as_of_date = datetime.strptime(args.as_of_date, "%Y-%m-%d").date()

    config = build_app_config(db_path=args.db_path)
    config.ensure_directories()

    frame = load_source_frame(
        args.sources,
        forced_as_of_date=forced_as_of_date,
        infer_as_of_date_from_path=args.infer_as_of_date_from_path,
    )

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
                FROM tmp_symbol_reference_source_raw_frame t
                WHERE symbol IS NOT NULL
                  AND TRIM(symbol) <> ''
                  AND as_of_date IS NOT NULL
                  AND NOT EXISTS (
                      SELECT 1
                      FROM {SYMBOL_REFERENCE_SOURCE_RAW} s
                      WHERE UPPER(TRIM(s.symbol)) = UPPER(TRIM(t.symbol))
                        AND COALESCE(s.company_name, '') = COALESCE(t.company_name, '')
                        AND COALESCE(s.cik, '') = COALESCE(t.cik, '')
                        AND COALESCE(s.exchange_raw, '') = COALESCE(t.exchange_raw, '')
                        AND COALESCE(s.security_type_raw, '') = COALESCE(t.security_type_raw, '')
                        AND COALESCE(s.source_name, '') = COALESCE(t.source_name, '')
                        AND s.as_of_date = CAST(t.as_of_date AS DATE)
                  )
                """
            )
        finally:
            try:
                con.unregister("tmp_symbol_reference_source_raw_frame")
            except Exception:
                pass

        rows_after = int(
            con.execute(f"SELECT COUNT(*) FROM {SYMBOL_REFERENCE_SOURCE_RAW}").fetchone()[0]
        )

        distinct_dates = int(
            con.execute(
                f"SELECT COUNT(DISTINCT as_of_date) FROM {SYMBOL_REFERENCE_SOURCE_RAW}"
            ).fetchone()[0]
        )

        summary = {
            "db_path": str(config.db_path),
            "source_mode": "real_sources_only_strict_snapshot_dates",
            "source_count": len(args.sources),
            "rows_before": rows_before,
            "rows_input_deduped": int(len(frame)),
            "rows_after": rows_after,
            "rows_inserted": rows_after - rows_before,
            "distinct_as_of_dates_after": distinct_dates,
            "truncate_used": bool(args.truncate),
            "infer_as_of_date_from_path": bool(args.infer_as_of_date_from_path),
            "forced_as_of_date": str(forced_as_of_date) if forced_as_of_date else None,
        }

        if args.verbose:
            print(json.dumps(summary, indent=2))
        else:
            print(f"Loaded symbol_reference_source_raw rows: total_rows={rows_after}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
