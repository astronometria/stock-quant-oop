#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--db-path", required=True)
    p.add_argument("--memory-limit", default="24GB")
    p.add_argument("--threads", type=int, default=4)
    p.add_argument("--temp-dir", default="/home/marty/stock-quant-oop/tmp")
    p.add_argument("--verbose", action="store_true")
    return p.parse_args()


def table_exists(con: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    row = con.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_name = ?
        """,
        [table_name],
    ).fetchone()
    return bool(row and int(row[0]) > 0)


def get_columns(con: duckdb.DuckDBPyConnection, table_name: str) -> set[str]:
    return {
        str(row[1]).strip()
        for row in con.execute(f"PRAGMA table_info('{table_name}')").fetchall()
    }


def pick_first(existing: set[str], candidates: list[str], label: str) -> str:
    for candidate in candidates:
        if candidate in existing:
            return candidate
    raise RuntimeError(
        f"Could not find a suitable {label} column. "
        f"Available columns: {sorted(existing)}; candidates tried: {candidates}"
    )


def pick_optional(existing: set[str], candidates: list[str]) -> str | None:
    for candidate in candidates:
        if candidate in existing:
            return candidate
    return None


def main() -> int:
    args = parse_args()
    db_path = Path(args.db_path).expanduser().resolve()

    con = duckdb.connect(str(db_path))
    try:
        con.execute(f"PRAGMA memory_limit='{args.memory_limit}'")
        con.execute(f"PRAGMA threads={int(args.threads)}")
        con.execute("PRAGMA preserve_insertion_order=false")

        temp_dir_sql = str(Path(args.temp_dir).expanduser().resolve()).replace("'", "''")
        con.execute(f"PRAGMA temp_directory='{temp_dir_sql}'")

        if not table_exists(con, "short_features_daily"):
            raise RuntimeError("short_features_daily does not exist")

        short_rows = int(con.execute("SELECT COUNT(*) FROM short_features_daily").fetchone()[0])
        if short_rows == 0:
            raise RuntimeError("short_features_daily exists but is empty")

        short_cols = get_columns(con, "short_features_daily")

        symbol_col = pick_first(short_cols, ["symbol"], "symbol")
        date_col = pick_first(short_cols, ["as_of_date", "price_date", "bar_date", "date"], "date")
        short_ratio_col = pick_optional(short_cols, ["short_volume_ratio"])
        short_interest_col = pick_optional(short_cols, ["short_interest"])
        days_to_cover_col = pick_optional(short_cols, ["days_to_cover"])
        short_interest_change_pct_col = pick_optional(short_cols, ["short_interest_change_pct"])
        short_squeeze_score_col = pick_optional(short_cols, ["short_squeeze_score"])
        short_pressure_zscore_col = pick_optional(short_cols, ["short_pressure_zscore"])
        days_to_cover_zscore_col = pick_optional(short_cols, ["days_to_cover_zscore"])
        instrument_id_col = pick_optional(short_cols, ["instrument_id"])
        company_id_col = pick_optional(short_cols, ["company_id"])

        if args.verbose:
            print(f"[short] short_features_daily_rows={short_rows}", flush=True)
            print(f"[short] symbol_col={symbol_col}", flush=True)
            print(f"[short] date_col={date_col}", flush=True)
            print(f"[short] short_ratio_col={short_ratio_col}", flush=True)
            print(f"[short] short_interest_col={short_interest_col}", flush=True)
            print(f"[short] days_to_cover_col={days_to_cover_col}", flush=True)
            print(f"[short] short_interest_change_pct_col={short_interest_change_pct_col}", flush=True)
            print(f"[short] short_squeeze_score_col={short_squeeze_score_col}", flush=True)
            print(f"[short] short_pressure_zscore_col={short_pressure_zscore_col}", flush=True)
            print(f"[short] days_to_cover_zscore_col={days_to_cover_zscore_col}", flush=True)
            print(f"[short] instrument_id_col={instrument_id_col}", flush=True)
            print(f"[short] company_id_col={company_id_col}", flush=True)

        con.execute("DROP TABLE IF EXISTS feature_short_daily")

        instrument_id_sql = (
            f"CAST({instrument_id_col} AS VARCHAR) AS instrument_id"
            if instrument_id_col
            else "NULL::VARCHAR AS instrument_id"
        )
        company_id_sql = (
            f"CAST({company_id_col} AS VARCHAR) AS company_id"
            if company_id_col
            else "NULL::VARCHAR AS company_id"
        )

        short_ratio_sql = f"CAST({short_ratio_col} AS DOUBLE)" if short_ratio_col else "NULL::DOUBLE"
        short_interest_sql = f"CAST({short_interest_col} AS DOUBLE)" if short_interest_col else "NULL::DOUBLE"
        days_to_cover_sql = f"CAST({days_to_cover_col} AS DOUBLE)" if days_to_cover_col else "NULL::DOUBLE"
        short_interest_change_pct_sql = (
            f"CAST({short_interest_change_pct_col} AS DOUBLE)"
            if short_interest_change_pct_col
            else "NULL::DOUBLE"
        )
        short_squeeze_score_sql = (
            f"CAST({short_squeeze_score_col} AS DOUBLE)"
            if short_squeeze_score_col
            else "NULL::DOUBLE"
        )
        short_pressure_zscore_sql = (
            f"CAST({short_pressure_zscore_col} AS DOUBLE)"
            if short_pressure_zscore_col
            else "NULL::DOUBLE"
        )
        days_to_cover_zscore_sql = (
            f"CAST({days_to_cover_zscore_col} AS DOUBLE)"
            if days_to_cover_zscore_col
            else "NULL::DOUBLE"
        )

        con.execute(
            f"""
            CREATE TABLE feature_short_daily AS
            SELECT
                {instrument_id_sql},
                {company_id_sql},
                CAST({symbol_col} AS VARCHAR) AS symbol,
                CAST({date_col} AS DATE) AS as_of_date,
                {short_interest_sql} AS short_interest,
                {days_to_cover_sql} AS days_to_cover,
                {short_ratio_sql} AS short_volume_ratio,
                {short_interest_change_pct_sql} AS short_interest_change_pct,
                {short_squeeze_score_sql} AS short_squeeze_score,
                {short_pressure_zscore_sql} AS short_pressure_zscore,
                {days_to_cover_zscore_sql} AS days_to_cover_zscore,
                'build_feature_short.py' AS source_name,
                CURRENT_TIMESTAMP AS created_at
            FROM short_features_daily
            WHERE {symbol_col} IS NOT NULL
              AND {date_col} IS NOT NULL
            """
        )

        row = con.execute(
            """
            SELECT
                COUNT(*) AS total_rows,
                COUNT(short_interest) AS short_interest_rows,
                COUNT(days_to_cover) AS days_to_cover_rows,
                COUNT(short_volume_ratio) AS short_volume_ratio_rows,
                COUNT(short_interest_change_pct) AS short_interest_change_pct_rows,
                COUNT(short_squeeze_score) AS short_squeeze_score_rows,
                COUNT(short_pressure_zscore) AS short_pressure_zscore_rows,
                COUNT(days_to_cover_zscore) AS days_to_cover_zscore_rows,
                MIN(as_of_date) AS min_date,
                MAX(as_of_date) AS max_date
            FROM feature_short_daily
            """
        ).fetchone()

        print(json.dumps(
            {
                "table_name": "feature_short_daily",
                "rows": int(row[0]),
                "short_interest_rows": int(row[1]),
                "days_to_cover_rows": int(row[2]),
                "short_volume_ratio_rows": int(row[3]),
                "short_interest_change_pct_rows": int(row[4]),
                "short_squeeze_score_rows": int(row[5]),
                "short_pressure_zscore_rows": int(row[6]),
                "days_to_cover_zscore_rows": int(row[7]),
                "min_date": str(row[8]) if row[8] is not None else None,
                "max_date": str(row[9]) if row[9] is not None else None,
            },
            indent=2,
        ), flush=True)

        return 0
    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
