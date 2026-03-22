#!/usr/bin/env python3
from __future__ import annotations

"""
Build research_training_dataset from:
- research_features_daily  (X)
- research_labels          (y)

Design:
- SQL-first
- no leakage
- configurable horizon
- strict target filtering
- keeps one row per (symbol, as_of_date)
"""

import argparse
import json
from pathlib import Path

import duckdb


ALLOWED_HORIZONS = {
    "5d": ("return_fwd_5d", "target_up_5d"),
    "10d": ("return_fwd_10d", "target_up_10d"),
    "20d": ("return_fwd_20d", "target_up_20d"),
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-path", required=True)
    parser.add_argument(
        "--horizon",
        choices=sorted(ALLOWED_HORIZONS.keys()),
        default="20d",
        help="Forward label horizon to use.",
    )
    parser.add_argument(
        "--min-date",
        default=None,
        help="Optional lower bound inclusive for as_of_date (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--max-date",
        default=None,
        help="Optional upper bound inclusive for as_of_date (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--drop-null-short",
        action="store_true",
        help="If set, require short features to be present.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    db_path = Path(args.db_path).expanduser().resolve()

    return_col, class_col = ALLOWED_HORIZONS[args.horizon]

    date_filters = []
    params: list[object] = []

    if args.min_date:
        date_filters.append("f.as_of_date >= CAST(? AS DATE)")
        params.append(args.min_date)

    if args.max_date:
        date_filters.append("f.as_of_date <= CAST(? AS DATE)")
        params.append(args.max_date)

    if args.drop_null_short:
        date_filters.append("f.short_volume_ratio IS NOT NULL")

    where_sql = ""
    if date_filters:
        where_sql = "AND " + " AND ".join(date_filters)

    con = duckdb.connect(str(db_path))
    try:
        con.execute("DROP TABLE IF EXISTS research_training_dataset")

        sql = f"""
        CREATE TABLE research_training_dataset AS
        WITH joined AS (
            SELECT
                f.instrument_id,
                f.company_id,
                f.symbol,
                f.as_of_date,

                -- =========================
                -- MASTER PRICE FEATURE SET
                -- =========================
                f.close,

                -- Momentum
                f.returns_1d,
                f.returns_5d,
                f.returns_10d,
                f.returns_20d,
                f.returns_60d,
                f.returns_log_1d,
                f.momentum_20d,
                f.momentum_acceleration_20d,
                f.rsi_14,
                f.williams_r_14,
                f.stoch_k_14,
                f.distance_from_252d_high,

                -- Trend
                f.sma_20,
                f.sma_50,
                f.sma_200,
                f.close_to_sma_20,
                f.close_to_sma_50,
                f.close_to_sma_200,
                f.ema_12,
                f.ema_26,
                f.macd_line,
                f.macd_signal,
                f.macd_hist,
                f.slope_20d,
                f.slope_50d,
                f.trend_strength,

                -- Volatility
                f.atr_14,
                f.atr_pct_14,
                f.volatility_5,
                f.volatility_10,
                f.volatility_20,
                f.volatility_60,
                f.realized_vol_20d,
                f.parkinson_vol_20d,
                f.garman_klass_vol_20d,
                f.bollinger_zscore_20,
                f.bollinger_bandwidth_20,

                -- Short
                f.short_interest,
                f.days_to_cover,
                f.short_volume_ratio,
                f.short_interest_change_pct,
                f.short_squeeze_score,
                f.short_pressure_zscore,
                f.days_to_cover_zscore,

                -- Labels
                l.{return_col} AS target_return,
                l.{class_col} AS target_class

            FROM research_features_daily f
            INNER JOIN research_labels l
                ON f.symbol = l.symbol
               AND f.as_of_date = l.as_of_date

            WHERE l.{return_col} IS NOT NULL
              AND l.{class_col} IS NOT NULL
              {where_sql}
        )
        SELECT *
        FROM joined
        """

        con.execute(sql, params)

        row = con.execute(
            """
            SELECT
                COUNT(*) AS rows,
                COUNT(DISTINCT symbol) AS symbols,
                MIN(as_of_date) AS min_date,
                MAX(as_of_date) AS max_date,
                COUNT(target_return) AS target_return_rows,
                COUNT(target_class) AS target_class_rows,
                AVG(target_return) AS avg_target_return,
                AVG(CAST(target_class AS DOUBLE)) AS positive_class_rate
            FROM research_training_dataset
            """
        ).fetchone()

        print(
            json.dumps(
                {
                    "table_name": "research_training_dataset",
                    "horizon": args.horizon,
                    "rows": int(row[0]),
                    "symbols": int(row[1]),
                    "min_date": str(row[2]) if row[2] is not None else None,
                    "max_date": str(row[3]) if row[3] is not None else None,
                    "target_return_rows": int(row[4]),
                    "target_class_rows": int(row[5]),
                    "avg_target_return": float(row[6]) if row[6] is not None else None,
                    "positive_class_rate": float(row[7]) if row[7] is not None else None,
                    "drop_null_short": bool(args.drop_null_short),
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
