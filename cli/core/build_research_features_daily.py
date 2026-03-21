#!/usr/bin/env python3
from __future__ import annotations

"""
Build canonical research_features_daily.

Objectif
--------
Assembler les familles de features canoniques dans une seule table de recherche
point-in-time safe, en joignant directement les tables canoniques:

- feature_price_momentum_daily
- feature_price_trend_daily
- feature_price_volatility_daily
- short_features_daily

Important
---------
- `feature_short_daily` a été retirée: c'était une projection legacy redondante
  de `short_features_daily`.
- Le chemin canonique short est maintenant:
    finra_daily_short_volume_source_raw
    -> daily_short_volume_history
    -> short_features_daily
    -> research_features_daily
"""

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


def table_count(con: duckdb.DuckDBPyConnection, table_name: str) -> int:
    return int(con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0])


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

        # Tables canoniques requises.
        required_tables = [
            "feature_price_momentum_daily",
            "feature_price_trend_daily",
            "feature_price_volatility_daily",
            "short_features_daily",
        ]

        for table_name in required_tables:
            if not table_exists(con, table_name):
                raise RuntimeError(f"required table missing: {table_name}")
            row_count = table_count(con, table_name)
            if args.verbose:
                print(f"[research_features] {table_name}_rows={row_count}", flush=True)

        # Rebuild complet de la table canonique de recherche.
        con.execute("DROP TABLE IF EXISTS research_features_daily")

        con.execute(
            """
            CREATE TABLE research_features_daily AS
            WITH price_base AS (
                SELECT
                    COALESCE(m.instrument_id, t.instrument_id, v.instrument_id) AS instrument_id,
                    COALESCE(m.symbol, t.symbol, v.symbol) AS symbol,
                    COALESCE(m.as_of_date, t.as_of_date, v.as_of_date) AS as_of_date,
                    m.close,
                    m.returns_1d,
                    m.returns_5d,
                    m.returns_20d,
                    m.rsi_14,
                    t.sma_20,
                    t.sma_50,
                    t.sma_200,
                    t.close_to_sma_20,
                    v.atr_14,
                    v.volatility_20
                FROM feature_price_momentum_daily m
                FULL OUTER JOIN feature_price_trend_daily t
                  ON UPPER(TRIM(m.symbol)) = UPPER(TRIM(t.symbol))
                 AND m.as_of_date = t.as_of_date
                FULL OUTER JOIN feature_price_volatility_daily v
                  ON UPPER(TRIM(COALESCE(m.symbol, t.symbol))) = UPPER(TRIM(v.symbol))
                 AND COALESCE(m.as_of_date, t.as_of_date) = v.as_of_date
            )
            SELECT
                p.instrument_id,
                s.company_id,
                p.symbol,
                p.as_of_date,
                p.close,
                p.returns_1d,
                p.returns_5d,
                p.returns_20d,
                p.sma_20,
                p.sma_50,
                p.sma_200,
                p.close_to_sma_20,
                p.rsi_14,
                p.atr_14,
                p.volatility_20,
                s.short_interest,
                s.days_to_cover,
                s.short_volume_ratio,
                s.short_interest_change_pct,
                s.short_squeeze_score,
                s.short_pressure_zscore,
                s.days_to_cover_zscore,
                'build_research_features_daily.py' AS source_name,
                CURRENT_TIMESTAMP AS created_at
            FROM price_base p
            LEFT JOIN short_features_daily s
              ON UPPER(TRIM(s.symbol)) = UPPER(TRIM(p.symbol))
             AND s.as_of_date = p.as_of_date
            WHERE p.symbol IS NOT NULL
              AND p.as_of_date IS NOT NULL
            """
        )

        row = con.execute(
            """
            SELECT
                COUNT(*) AS total_rows,
                COUNT(close) AS close_rows,
                COUNT(returns_1d) AS returns_1d_rows,
                COUNT(rsi_14) AS rsi_14_rows,
                COUNT(sma_20) AS sma_20_rows,
                COUNT(sma_50) AS sma_50_rows,
                COUNT(sma_200) AS sma_200_rows,
                COUNT(close_to_sma_20) AS close_to_sma_20_rows,
                COUNT(atr_14) AS atr_14_rows,
                COUNT(volatility_20) AS volatility_20_rows,
                COUNT(short_volume_ratio) AS short_volume_ratio_rows,
                MIN(as_of_date) AS min_date,
                MAX(as_of_date) AS max_date
            FROM research_features_daily
            """
        ).fetchone()

        print(
            json.dumps(
                {
                    "table_name": "research_features_daily",
                    "rows": int(row[0]),
                    "close_rows": int(row[1]),
                    "returns_1d_rows": int(row[2]),
                    "rsi_14_rows": int(row[3]),
                    "sma_20_rows": int(row[4]),
                    "sma_50_rows": int(row[5]),
                    "sma_200_rows": int(row[6]),
                    "close_to_sma_20_rows": int(row[7]),
                    "atr_14_rows": int(row[8]),
                    "volatility_20_rows": int(row[9]),
                    "short_volume_ratio_rows": int(row[10]),
                    "min_date": str(row[11]) if row[11] is not None else None,
                    "max_date": str(row[12]) if row[12] is not None else None,
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
