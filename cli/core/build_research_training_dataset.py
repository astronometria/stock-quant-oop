#!/usr/bin/env python3
from __future__ import annotations

# =============================================================================
# Build research_training_dataset
#
# This version adds support for horizon=1d so training can be aligned with a
# next-day realized-return backtest.
# =============================================================================

import argparse
import json
from pathlib import Path

import duckdb

ALLOWED_HORIZONS = {
    "1d": ("return_fwd_1d", "target_up_1d"),
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
        default="1d",
        help="Forward label horizon to use.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    db_path = Path(args.db_path).expanduser().resolve()
    return_col, class_col = ALLOWED_HORIZONS[args.horizon]

    con = duckdb.connect(str(db_path))
    try:
        con.execute("DROP TABLE IF EXISTS research_training_dataset")
        sql = f"""
        CREATE TABLE research_training_dataset AS
        SELECT
            f.*,
            l.{return_col} AS target_return,
            l.{class_col} AS target_class
        FROM research_features_daily f
        INNER JOIN research_labels l
            ON f.instrument_id = l.instrument_id
           AND f.as_of_date = l.as_of_date
        WHERE l.{return_col} IS NOT NULL
          AND l.{class_col} IS NOT NULL
        """
        con.execute(sql)

        row = con.execute(
            """
            SELECT
                COUNT(*) AS rows,
                COUNT(DISTINCT symbol) AS symbols,
                MIN(as_of_date) AS min_date,
                MAX(as_of_date) AS max_date,
                AVG(target_return) AS avg_target_return,
                AVG(CAST(target_class AS DOUBLE)) AS positive_class_rate
            FROM research_training_dataset
            """
        ).fetchone()

        print(json.dumps({
            "table_name": "research_training_dataset",
            "horizon": args.horizon,
            "rows": int(row[0]),
            "symbols": int(row[1]),
            "min_date": str(row[2]) if row[2] is not None else None,
            "max_date": str(row[3]) if row[3] is not None else None,
            "avg_target_return": float(row[4]) if row[4] is not None else None,
            "positive_class_rate": float(row[5]) if row[5] is not None else None,
        }, indent=2), flush=True)
        return 0
    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
