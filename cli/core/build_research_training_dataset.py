#!/usr/bin/env python3
from __future__ import annotations

"""
Research training dataset builder (PIT-safe)

IMPORTANT:
- as-of join backward
- no look-ahead bias
- SQL-first
"""

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from stock_quant.infrastructure.db.research_training_dataset_schema import (
    ResearchTrainingDatasetSchemaManager,
)


def _now():
    return datetime.now(timezone.utc)


def _dataset_id(snapshot_id: str) -> str:
    return f"{snapshot_id}_dataset_{_now().strftime('%Y%m%dT%H%M%SZ')}"


def _choose_price_date_column(con: duckdb.DuckDBPyConnection) -> str:
    rows = con.execute("PRAGMA table_info('price_history')").fetchall()
    cols = {r[1].lower() for r in rows}

    for c in ["price_date", "date", "trade_date", "as_of_date"]:
        if c in cols:
            return c

    raise RuntimeError(f"no date column found in price_history: {cols}")


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--db-path", required=True)
    p.add_argument("--snapshot-id", required=True)
    return p.parse_args()


def main():
    args = parse_args()
    db = Path(args.db_path).expanduser().resolve()

    print(f"[dataset_builder_v3] db={db}", flush=True)

    con = duckdb.connect(str(db))
    try:
        ResearchTrainingDatasetSchemaManager(con).ensure_tables()

        price_date_col = _choose_price_date_column(con)

        dataset_id = _dataset_id(args.snapshot_id)

        con.execute("DELETE FROM research_training_dataset WHERE dataset_id = ?", [dataset_id])

        con.execute(f"""
        INSERT INTO research_training_dataset
        SELECT
            '{dataset_id}' AS dataset_id,
            '{args.snapshot_id}' AS snapshot_id,
            p.symbol,
            p.{price_date_col} AS as_of_date,
            p.close,
            sf.short_volume_ratio
        FROM price_history p

        LEFT JOIN (
            SELECT
                p.symbol,
                p.{price_date_col} AS price_date,
                sf.short_volume_ratio,
                ROW_NUMBER() OVER (
                    PARTITION BY p.symbol, p.{price_date_col}
                    ORDER BY sf.as_of_date DESC
                ) AS rn
            FROM price_history p
            LEFT JOIN short_features_daily sf
              ON sf.symbol = p.symbol
             AND sf.as_of_date <= p.{price_date_col}
        ) sf
          ON sf.symbol = p.symbol
         AND sf.price_date = p.{price_date_col}
         AND sf.rn = 1

        WHERE p.symbol IS NOT NULL
          AND p.{price_date_col} IS NOT NULL
        """)

        count = con.execute("""
            SELECT COUNT(*) FROM research_training_dataset WHERE dataset_id = ?
        """, [dataset_id]).fetchone()[0]

        print(json.dumps({
            "dataset_id": dataset_id,
            "rows": count,
            "mode": "asof_join"
        }, indent=2), flush=True)

        return 0

    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
