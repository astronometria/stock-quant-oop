#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--db-path", required=True)
    return p.parse_args()


def main() -> int:
    args = parse_args()
    db_path = Path(args.db_path).expanduser().resolve()

    con = duckdb.connect(str(db_path))
    try:
        con.execute("DROP TABLE IF EXISTS research_features_daily")

        con.execute(
            """
            CREATE TABLE research_features_daily AS
            WITH m AS (
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
                    rsi_14,
                    williams_r_14,
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
                    macd_hist
                FROM feature_price_trend_daily
            ),
            v AS (
                SELECT
                    instrument_id,
                    symbol,
                    as_of_date,
                    atr_14,
                    volatility_20
                FROM feature_price_volatility_daily
            ),
            s AS (
                SELECT
                    instrument_id,
                    company_id,
                    symbol,
                    as_of_date,
                    short_interest,
                    days_to_cover,
                    short_volume_ratio,
                    short_interest_change_pct,
                    short_squeeze_score,
                    short_pressure_zscore,
                    days_to_cover_zscore
                FROM feature_short_daily
            )
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
                m.rsi_14,
                m.williams_r_14,
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

                v.atr_14,
                v.volatility_20,

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
        )

        row = con.execute(
            """
            SELECT
                COUNT(*) AS rows,
                COUNT(DISTINCT symbol) AS symbols,
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
                    "symbols": int(row[1]),
                    "close_rows": int(row[2]),
                    "returns_1d_rows": int(row[3]),
                    "rsi_14_rows": int(row[4]),
                    "sma_20_rows": int(row[5]),
                    "sma_50_rows": int(row[6]),
                    "sma_200_rows": int(row[7]),
                    "close_to_sma_20_rows": int(row[8]),
                    "atr_14_rows": int(row[9]),
                    "volatility_20_rows": int(row[10]),
                    "short_volume_ratio_rows": int(row[11]),
                    "min_date": str(row[12]) if row[12] is not None else None,
                    "max_date": str(row[13]) if row[13] is not None else None,
                    "mode": "price_master_left_join",
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
