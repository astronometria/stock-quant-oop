#!/usr/bin/env python3
from __future__ import annotations

"""
Build feature_price_trend_daily incrementally.

Objectif
--------
Construire / rafraîchir la famille trend:
- sma_20
- sma_50
- sma_200
- close_to_sma_20

Pourquoi la fenêtre est plus large
----------------------------------
Cette famille dépend du SMA 200.
Donc la fenêtre recalculée doit avoir:
- recouvrement cible: 220 jours
- lookback source: 220 jours

Cela permet d'avoir des moyennes mobiles stables au bord de la fenêtre.
"""

import argparse
import json
from pathlib import Path

import duckdb


TARGET_OVERLAP_DAYS = 220
SOURCE_LOOKBACK_DAYS = 220


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


def table_count(con: duckdb.DuckDBPyConnection, table_name: str) -> int:
    return int(con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0])


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


def ensure_target_table(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS feature_price_trend_daily (
            instrument_id VARCHAR,
            symbol VARCHAR,
            as_of_date DATE,
            close DOUBLE,
            sma_20 DOUBLE,
            sma_50 DOUBLE,
            sma_200 DOUBLE,
            close_to_sma_20 DOUBLE,
            source_name VARCHAR,
            created_at TIMESTAMP WITH TIME ZONE
        )
        """
    )


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

        ensure_target_table(con)

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
        instrument_id_col = "instrument_id" if "instrument_id" in price_cols else None

        target_rows_before = table_count(con, "feature_price_trend_daily")
        max_target_date = con.execute(
            "SELECT MAX(as_of_date) FROM feature_price_trend_daily"
        ).fetchone()[0]

        con.execute("DROP TABLE IF EXISTS tmp_feature_price_trend_window")
        con.execute(
            f"""
            CREATE TEMP TABLE tmp_feature_price_trend_window AS
            SELECT
                CASE
                    WHEN ? IS NULL THEN DATE '1900-01-01'
                    ELSE CAST(? - INTERVAL '{TARGET_OVERLAP_DAYS} days' AS DATE)
                END AS target_window_start,
                CASE
                    WHEN ? IS NULL THEN DATE '1900-01-01'
                    ELSE CAST(? - INTERVAL '{TARGET_OVERLAP_DAYS + SOURCE_LOOKBACK_DAYS} days' AS DATE)
                END AS source_window_start
            """,
            [max_target_date, max_target_date, max_target_date, max_target_date],
        )

        target_window_start, source_window_start = con.execute(
            """
            SELECT target_window_start, source_window_start
            FROM tmp_feature_price_trend_window
            """
        ).fetchone()

        if args.verbose:
            print(f"[trend] price_table={price_table}", flush=True)
            if table_exists(con, "price_bars_adjusted"):
                adjusted_rows = int(con.execute("SELECT COUNT(*) FROM price_bars_adjusted").fetchone()[0])
                print(f"[trend] price_bars_adjusted_rows={adjusted_rows}", flush=True)
            if table_exists(con, "price_history"):
                history_rows = int(con.execute("SELECT COUNT(*) FROM price_history").fetchone()[0])
                print(f"[trend] price_history_rows={history_rows}", flush=True)
            print(f"[trend] symbol_col={symbol_col}", flush=True)
            print(f"[trend] date_col={date_col}", flush=True)
            print(f"[trend] close_col={close_col}", flush=True)
            print(f"[trend] instrument_id_col={instrument_id_col}", flush=True)
            print(f"[trend] target_rows_before={target_rows_before}", flush=True)
            print(f"[trend] max_target_date={max_target_date}", flush=True)
            print(f"[trend] target_window_start={target_window_start}", flush=True)
            print(f"[trend] source_window_start={source_window_start}", flush=True)

        instrument_id_sql = (
            f"CAST(p.{instrument_id_col} AS VARCHAR) AS instrument_id"
            if instrument_id_col
            else "NULL::VARCHAR AS instrument_id"
        )

        con.execute("DROP TABLE IF EXISTS tmp_feature_price_trend_stage")
        con.execute(
            f"""
            CREATE TEMP TABLE tmp_feature_price_trend_stage AS
            WITH price_base AS (
                SELECT
                    {instrument_id_sql},
                    CAST(p.{symbol_col} AS VARCHAR) AS symbol,
                    CAST(p.{date_col} AS DATE) AS as_of_date,
                    CAST(p.{close_col} AS DOUBLE) AS close
                FROM {price_table} p
                WHERE p.{symbol_col} IS NOT NULL
                  AND p.{date_col} IS NOT NULL
                  AND p.{close_col} IS NOT NULL
                  AND CAST(p.{date_col} AS DATE) >= CAST(? AS DATE)
            ),
            calc AS (
                SELECT
                    instrument_id,
                    symbol,
                    as_of_date,
                    close,
                    AVG(close) OVER (
                        PARTITION BY symbol
                        ORDER BY as_of_date
                        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                    ) AS sma_20,
                    AVG(close) OVER (
                        PARTITION BY symbol
                        ORDER BY as_of_date
                        ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
                    ) AS sma_50,
                    AVG(close) OVER (
                        PARTITION BY symbol
                        ORDER BY as_of_date
                        ROWS BETWEEN 199 PRECEDING AND CURRENT ROW
                    ) AS sma_200
                FROM price_base
            )
            SELECT
                instrument_id,
                symbol,
                as_of_date,
                close,
                sma_20,
                sma_50,
                sma_200,
                CASE
                    WHEN sma_20 IS NULL OR sma_20 = 0 THEN NULL
                    ELSE (close / sma_20) - 1
                END AS close_to_sma_20,
                'build_feature_price_trend.py' AS source_name,
                CURRENT_TIMESTAMP AS created_at
            FROM calc
            WHERE as_of_date >= CAST(? AS DATE)
            """,
            [source_window_start, target_window_start],
        )

        stage_rows = table_count(con, "tmp_feature_price_trend_stage")

        deleted_rows = con.execute(
            """
            DELETE FROM feature_price_trend_daily
            WHERE as_of_date >= CAST(? AS DATE)
            """,
            [target_window_start],
        ).fetchone()[0]

        inserted_rows = con.execute(
            """
            INSERT INTO feature_price_trend_daily (
                instrument_id,
                symbol,
                as_of_date,
                close,
                sma_20,
                sma_50,
                sma_200,
                close_to_sma_20,
                source_name,
                created_at
            )
            SELECT
                instrument_id,
                symbol,
                as_of_date,
                close,
                sma_20,
                sma_50,
                sma_200,
                close_to_sma_20,
                source_name,
                created_at
            FROM tmp_feature_price_trend_stage
            """
        ).fetchone()[0]

        row = con.execute(
            """
            SELECT
                COUNT(*) AS total_rows,
                COUNT(sma_20) AS sma_20_rows,
                COUNT(sma_50) AS sma_50_rows,
                COUNT(sma_200) AS sma_200_rows,
                COUNT(close_to_sma_20) AS close_to_sma_20_rows,
                MIN(as_of_date) AS min_date,
                MAX(as_of_date) AS max_date
            FROM feature_price_trend_daily
            """
        ).fetchone()

        print(
            json.dumps(
                {
                    "table_name": "feature_price_trend_daily",
                    "rows": int(row[0]),
                    "sma_20_rows": int(row[1]),
                    "sma_50_rows": int(row[2]),
                    "sma_200_rows": int(row[3]),
                    "close_to_sma_20_rows": int(row[4]),
                    "min_date": str(row[5]) if row[5] is not None else None,
                    "max_date": str(row[6]) if row[6] is not None else None,
                    "target_rows_before": int(target_rows_before),
                    "target_window_start": str(target_window_start) if target_window_start is not None else None,
                    "source_window_start": str(source_window_start) if source_window_start is not None else None,
                    "stage_rows": int(stage_rows),
                    "deleted_rows": int(deleted_rows),
                    "inserted_rows": int(inserted_rows),
                },
                indent=2,
            ),
            flush=True,
        )

        return 0
    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
