#!/usr/bin/env python3

from __future__ import annotations

# =============================================================================
# Build research_features_daily
#
# Réparation ciblée:
# - supporte feature_short_daily OU short_features_daily
# - n'exige PAS instrument_id/company_id côté short
# - jointure short uniquement sur symbol + as_of_date
# - reste SQL-first
# =============================================================================

import argparse
import json
from pathlib import Path

import duckdb


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--db-path", required=True)
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
    rows = con.execute(f"DESCRIBE {table_name}").fetchall()
    return {str(r[0]) for r in rows}


def choose_short_table(con: duckdb.DuckDBPyConnection) -> str:
    if table_exists(con, "feature_short_daily"):
        return "feature_short_daily"
    if table_exists(con, "short_features_daily"):
        return "short_features_daily"
    raise RuntimeError("Neither feature_short_daily nor short_features_daily exists")


def choose_date_expr(short_cols: set[str]) -> str:
    for candidate in ["as_of_date", "price_date", "trade_date", "report_date", "settlement_date"]:
        if candidate in short_cols:
            return f"CAST(s.{candidate} AS DATE)"
    raise RuntimeError(
        "Short table must contain one of: as_of_date, price_date, trade_date, report_date, settlement_date"
    )


def short_numeric_expr(short_cols: set[str], col_name: str) -> str:
    if col_name in short_cols:
        return f"CAST(s.{col_name} AS DOUBLE) AS {col_name}"
    return f"CAST(NULL AS DOUBLE) AS {col_name}"


def main() -> int:
    args = parse_args()
    db_path = Path(args.db_path).expanduser().resolve()
    con = duckdb.connect(str(db_path))

    try:
        required = [
            "feature_price_momentum_daily",
            "feature_price_trend_daily",
            "feature_price_volatility_daily",
        ]
        missing = [t for t in required if not table_exists(con, t)]
        if missing:
            raise RuntimeError(f"Missing required upstream tables: {missing}")

        short_table = choose_short_table(con)
        short_cols = get_columns(con, short_table)
        short_date_expr = choose_date_expr(short_cols)

        short_cte = f"""
        s AS (
            SELECT
                UPPER(TRIM(s.symbol)) AS symbol,
                {short_date_expr} AS as_of_date,
                {short_numeric_expr(short_cols, "short_interest")},
                {short_numeric_expr(short_cols, "days_to_cover")},
                {short_numeric_expr(short_cols, "short_volume_ratio")},
                {short_numeric_expr(short_cols, "short_interest_change_pct")},
                {short_numeric_expr(short_cols, "short_squeeze_score")},
                {short_numeric_expr(short_cols, "short_pressure_zscore")},
                {short_numeric_expr(short_cols, "days_to_cover_zscore")}
            FROM {short_table} s
            WHERE s.symbol IS NOT NULL
        )
        """

        sql = f"""
        DROP TABLE IF EXISTS research_features_daily;

        CREATE TABLE research_features_daily AS
        WITH
        m AS (
            SELECT
                instrument_id,
                company_id,
                symbol,
                as_of_date,
                close,
                returns_1d,
                returns_5d,
                returns_10d,
                returns_20d,
                returns_60d,
                returns_log_1d,
                momentum_20d,
                momentum_acceleration_20d,
                rsi_14,
                williams_r_14,
                stoch_k_14,
                distance_from_252d_high
            FROM feature_price_momentum_daily
        ),
        t AS (
            SELECT
                instrument_id,
                company_id,
                symbol,
                as_of_date,
                sma_20,
                sma_50,
                sma_200,
                close_to_sma_20,
                close_to_sma_50,
                close_to_sma_200,
                ema_12,
                ema_26,
                macd_line,
                macd_signal,
                macd_hist,
                slope_20d,
                slope_50d,
                trend_strength
            FROM feature_price_trend_daily
        ),
        v AS (
            SELECT
                instrument_id,
                company_id,
                symbol,
                as_of_date,
                atr_14,
                atr_pct_14,
                volatility_5,
                volatility_10,
                volatility_20,
                volatility_60,
                realized_vol_20d,
                parkinson_vol_20d,
                garman_klass_vol_20d,
                bollinger_zscore_20,
                bollinger_bandwidth_20
            FROM feature_price_volatility_daily
        ),
        {short_cte}
        SELECT
            m.instrument_id,
            m.company_id,
            m.symbol,
            m.as_of_date,
            m.close,
            m.returns_1d,
            m.returns_5d,
            m.returns_10d,
            m.returns_20d,
            m.returns_60d,
            m.returns_log_1d,
            m.momentum_20d,
            m.momentum_acceleration_20d,
            m.rsi_14,
            m.williams_r_14,
            m.stoch_k_14,
            m.distance_from_252d_high,
            t.sma_20,
            t.sma_50,
            t.sma_200,
            t.close_to_sma_20,
            t.close_to_sma_50,
            t.close_to_sma_200,
            t.ema_12,
            t.ema_26,
            t.macd_line,
            t.macd_signal,
            t.macd_hist,
            t.slope_20d,
            t.slope_50d,
            t.trend_strength,
            v.atr_14,
            v.atr_pct_14,
            v.volatility_5,
            v.volatility_10,
            v.volatility_20,
            v.volatility_60,
            v.realized_vol_20d,
            v.parkinson_vol_20d,
            v.garman_klass_vol_20d,
            v.bollinger_zscore_20,
            v.bollinger_bandwidth_20,
            s.short_interest,
            s.days_to_cover,
            s.short_volume_ratio,
            s.short_interest_change_pct,
            s.short_squeeze_score,
            s.short_pressure_zscore,
            s.days_to_cover_zscore
        FROM m
        LEFT JOIN t
            ON m.symbol = t.symbol
           AND m.as_of_date = t.as_of_date
        LEFT JOIN v
            ON m.symbol = v.symbol
           AND m.as_of_date = v.as_of_date
        LEFT JOIN s
            ON m.symbol = s.symbol
           AND m.as_of_date = s.as_of_date
        """

        con.execute(sql)

        row = con.execute("""
            SELECT
                COUNT(*) AS rows,
                COUNT(DISTINCT symbol) AS symbols,
                COUNT(close) AS close_rows,
                COUNT(returns_1d) AS returns_1d_rows,
                COUNT(rsi_14) AS rsi_14_rows,
                COUNT(sma_20) AS sma_20_rows,
                COUNT(atr_14) AS atr_14_rows,
                COUNT(short_volume_ratio) AS short_volume_ratio_rows,
                MIN(as_of_date) AS min_date,
                MAX(as_of_date) AS max_date
            FROM research_features_daily
        """).fetchone()

        print(json.dumps({
            "table_name": "research_features_daily",
            "rows": int(row[0]),
            "symbols": int(row[1]),
            "close_rows": int(row[2]),
            "returns_1d_rows": int(row[3]),
            "rsi_14_rows": int(row[4]),
            "sma_20_rows": int(row[5]),
            "atr_14_rows": int(row[6]),
            "short_volume_ratio_rows": int(row[7]),
            "min_date": str(row[8]) if row[8] is not None else None,
            "max_date": str(row[9]) if row[9] is not None else None,
            "short_source_table": short_table,
            "short_source_columns": sorted(short_cols),
            "mode": "schema_adaptive_v4_join_on_symbol_date",
        }, indent=2))

        return 0
    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
