#!/usr/bin/env python3
from __future__ import annotations

"""
Build research training dataset from snapshot_id.

Philosophie
-----------
- SQL-first
- point-in-time safe
- aucune fuite future
"""

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from stock_quant.infrastructure.db.research_training_dataset_schema import (
    ResearchTrainingDatasetSchemaManager,
)


def _now_utc():
    return datetime.now(timezone.utc)


def _dataset_id(snapshot_id: str) -> str:
    stamp = _now_utc().strftime("%Y%m%dT%H%M%SZ")
    return f"{snapshot_id}_dataset_{stamp}"


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--db-path", required=True)
    p.add_argument("--snapshot-id", required=True)
    return p.parse_args()


def main():
    args = parse_args()
    db_path = Path(args.db_path).expanduser().resolve()

    con = duckdb.connect(str(db_path))
    try:
        ResearchTrainingDatasetSchemaManager(con).ensure_tables()

        # --- vérifier snapshot ---
        snapshot = con.execute("""
            SELECT snapshot_id, status
            FROM research_dataset_manifest
            WHERE snapshot_id = ?
        """, [args.snapshot_id]).fetchone()

        if snapshot is None:
            raise RuntimeError("snapshot_id not found")

        if snapshot[1] != "completed":
            raise RuntimeError("snapshot not completed")

        dataset_id = _dataset_id(args.snapshot_id)

        # --- SQL principal (PIT safe) ---
        con.execute(f"""
            INSERT INTO research_training_dataset
            SELECT
                '{dataset_id}' AS dataset_id,
                '{args.snapshot_id}' AS snapshot_id,
                p.symbol,
                p.date AS as_of_date,

                p.close,

                sf.short_volume_ratio

            FROM price_history p

            LEFT JOIN short_features_daily sf
              ON sf.symbol = p.symbol
             AND sf.as_of_date = p.date

            WHERE p.date IS NOT NULL
        """)

        row_count = con.execute("""
            SELECT COUNT(*) FROM research_training_dataset
            WHERE dataset_id = ?
        """, [dataset_id]).fetchone()[0]

        output = {
            "dataset_id": dataset_id,
            "snapshot_id": args.snapshot_id,
            "row_count": int(row_count),
        }

        print(json.dumps(output, indent=2))

    finally:
        con.close()


if __name__ == "__main__":
    main()
