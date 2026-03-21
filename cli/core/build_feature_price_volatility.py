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

        if table_exists(con, "price_bars_adjusted"):
            adjusted_rows = int(con.execute("SELECT COUNT(*) FROM price_bars_adjusted").fetchone()[0])
            if adjusted_rows > 0:
                price_table = "price_bars_adjusted"
            elif table_exists(con, "price_history"):
                price_table = "price_history"
            else:
                raise RuntimeError(
                    "price_bars_adjusted exists but is empty, and price_history does not exist"
                )
        elif table_exists(con, "price_history"):
            price_table = "price_history"
        else:
            raise RuntimeError("Neither price_bars_adjusted nor price_history exists")

        price_cols = get_columns(con, price_table)
        symbol_col = pick_first(price_cols, ["symbol"], "symbol")
        date_col = pick_first(price_cols, ["bar_date", "price_date", "as_of_date", "date"], "date")
        close_col = pick_first(price_cols, ["adj_close", "close", "adjusted_close"], "close")
        high_col = pick_optional(price_cols, ["adj_high", "high"])
        low_col = pick_optional(price_cols, ["adj_low", "low"])
        instrument_id_col = "instrument_id" if "instrument_id" in price_cols else None

        if high_col is None or low_col is None:
            raise RuntimeError(
                f"Need high/low columns for ATR. Available columns in {price_table}: {sorted(price_cols)}"
            )

        if args.verbose:
            print(f"[volatility] price_table={price_table}", flush=True)
            if table_exists(con, "price_bars_adjusted"):
                adjusted_rows = int(con.execute("SELECT COUNT(*) FROM price_bars_adjusted").fetchone()[0])
                print(f"[volatility] price_bars_adjusted_rows={adjusted_rows}", flush=True)
            if table_exists(con, "price_history"):
                history_rows = int(con.execute("SELECT COUNT(*) FROM price_history").fetchone()[0])
                print(f"[volatility] price_history_rows={history_rows}", flush=True)
            print(f"[volatility] symbol_col={symbol_col}", flush=True)
            print(f"[volatility] date_col={date_col}", flush=True)
            print(f"[volatility] close_col={close_col}", flush=True)
            print(f"[volatility] high_col={high_col}", flush=True)
            print(f"[volatility] low_col={low_col}", flush=True)
            print(f"[volatility] instrument_id_col={instrument_id_col}", flush=True)

        con.execute("DROP TABLE IF EXISTS feature_price_volatility_daily")

        instrument_id_sql = (
            f"CAST(p.{instrument_id_col} AS VARCHAR) AS instrument_id"
            if instrument_id_col
            else "NULL::VARCHAR AS instrument_id"
        )

        con.execute(
            f"""
            CREATE TABLE feature_price_volatility_daily AS
            WITH price_base AS (
                SELECT
                    {instrument_id_sql},
                    CAST(p.{symbol_col} AS VARCHAR) AS symbol,
                    CAST(p.{date_col} AS DATE) AS as_of_date,
                    CAST(p.{close_col} AS DOUBLE) AS close,
                    CAST(p.{high_col} AS DOUBLE) AS high_price,
                    CAST(p.{low_col} AS DOUBLE) AS low_price
                FROM {price_table} p
                WHERE p.{symbol_col} IS NOT NULL
                  AND p.{date_col} IS NOT NULL
                  AND p.{close_col} IS NOT NULL
                  AND p.{high_col} IS NOT NULL
                  AND p.{low_col} IS NOT NULL
            ),
            ordered AS (
                SELECT
                    instrument_id,
                    symbol,
                    as_of_date,
                    close,
                    high_price,
                    low_price,
                    LAG(close, 1) OVER (PARTITION BY symbol ORDER BY as_of_date) AS close_lag_1
                FROM price_base
            ),
            enriched AS (
                SELECT
                    instrument_id,
                    symbol,
                    as_of_date,
                    close,
                    CASE
                        WHEN close_lag_1 IS NULL OR close_lag_1 = 0 THEN NULL
                        ELSE (close / close_lag_1) - 1
                    END AS returns_1d,
                    CASE
                        WHEN close_lag_1 IS NULL THEN high_price - low_price
                        ELSE GREATEST(
                            high_price - low_price,
                            ABS(high_price - close_lag_1),
                            ABS(low_price - close_lag_1)
                        )
                    END AS true_range
                FROM ordered
            )
            SELECT
                instrument_id,
                symbol,
                as_of_date,
                close,
                AVG(true_range) OVER (
                    PARTITION BY symbol
                    ORDER BY as_of_date
                    ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
                ) AS atr_14,
                STDDEV_SAMP(returns_1d) OVER (
                    PARTITION BY symbol
                    ORDER BY as_of_date
                    ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                ) AS volatility_20,
                'build_feature_price_volatility.py' AS source_name,
                CURRENT_TIMESTAMP AS created_at
            FROM enriched
            """
        )

        row = con.execute(
            """
            SELECT
                COUNT(*) AS total_rows,
                COUNT(atr_14) AS atr_14_rows,
                COUNT(volatility_20) AS volatility_20_rows,
                MIN(as_of_date) AS min_date,
                MAX(as_of_date) AS max_date
            FROM feature_price_volatility_daily
            """
        ).fetchone()

        print(json.dumps(
            {
                "table_name": "feature_price_volatility_daily",
                "rows": int(row[0]),
                "atr_14_rows": int(row[1]),
                "volatility_20_rows": int(row[2]),
                "min_date": str(row[3]) if row[3] is not None else None,
                "max_date": str(row[4]) if row[4] is not None else None,
            },
            indent=2,
        ), flush=True)

        return 0
    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
