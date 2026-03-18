#!/usr/bin/env python3
from __future__ import annotations

"""
Build research labels (forward returns).

IMPORTANT:
- no look-ahead bias
- labels séparés des features
"""

import argparse
import json
from pathlib import Path
from datetime import datetime, timezone

import duckdb


def _now():
    return datetime.now(timezone.utc)


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--db-path", required=True)
    p.add_argument("--snapshot-id", required=True)
    p.add_argument("--dataset-id", required=True)
    return p.parse_args()


def main():
    args = parse_args()
    db_path = Path(args.db_path).expanduser().resolve()

    con = duckdb.connect(str(db_path))
    try:
        # validation snapshot
        snap = con.execute("""
            SELECT status
            FROM research_dataset_manifest
            WHERE snapshot_id = ?
        """, [args.snapshot_id]).fetchone()

        if snap is None:
            raise RuntimeError("snapshot not found")

        if snap[0] != "completed":
            raise RuntimeError("snapshot not completed")

        # SQL labels
        con.execute(f"""
            INSERT INTO research_labels (
                dataset_id,
                snapshot_id,
                symbol,
                as_of_date,
                fwd_return_1d,
                fwd_return_5d,
                fwd_return_20d
            )
            SELECT
                '{args.dataset_id}',
                '{args.snapshot_id}',
                p.symbol,
                p.date,

                -- 1d return
                LEAD(p.close, 1) OVER w / p.close - 1,

                -- 5d return
                LEAD(p.close, 5) OVER w / p.close - 1,

                -- 20d return
                LEAD(p.close, 20) OVER w / p.close - 1

            FROM price_history p

            WINDOW w AS (
                PARTITION BY p.symbol
                ORDER BY p.date
            )
        """)

        count = con.execute("""
            SELECT COUNT(*) FROM research_labels
            WHERE dataset_id = ?
        """, [args.dataset_id]).fetchone()[0]

        print(json.dumps({
            "dataset_id": args.dataset_id,
            "rows": int(count)
        }, indent=2))

    finally:
        con.close()


if __name__ == "__main__":
    main()
