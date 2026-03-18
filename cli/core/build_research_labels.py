#!/usr/bin/env python3
from __future__ import annotations

"""
Build research labels (forward returns).

IMPORTANT
---------
- no look-ahead bias
- labels séparés des features
- schema assuré avant écriture
"""

import argparse
import json
from pathlib import Path

import duckdb

from stock_quant.infrastructure.db.research_labels_schema import (
    ResearchLabelsSchemaManager,
)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--db-path", required=True)
    p.add_argument("--snapshot-id", required=True)
    p.add_argument("--dataset-id", required=True)
    return p.parse_args()


def main() -> int:
    args = parse_args()
    db_path = Path(args.db_path).expanduser().resolve()

    print(f"[build_research_labels] db_path={db_path}", flush=True)

    con = duckdb.connect(str(db_path))
    try:
        # Assure le schéma avant toute lecture/écriture.
        ResearchLabelsSchemaManager(con).ensure_tables()

        # Validation snapshot
        snap = con.execute(
            """
            SELECT status
            FROM research_dataset_manifest
            WHERE snapshot_id = ?
            """,
            [args.snapshot_id],
        ).fetchone()

        if snap is None:
            raise RuntimeError("snapshot not found")

        if snap[0] != "completed":
            raise RuntimeError("snapshot not completed")

        # Idempotence simple pour ce dataset_id.
        con.execute(
            """
            DELETE FROM research_labels
            WHERE dataset_id = ?
            """,
            [args.dataset_id],
        )

        # SQL labels PIT-safe:
        # les labels utilisent le futur seulement ici, jamais dans les features.
        con.execute(
            f"""
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
                '{args.dataset_id}' AS dataset_id,
                '{args.snapshot_id}' AS snapshot_id,
                p.symbol,
                p.date AS as_of_date,

                CASE
                    WHEN p.close IS NOT NULL
                     AND p.close <> 0
                     AND LEAD(p.close, 1) OVER w IS NOT NULL
                    THEN LEAD(p.close, 1) OVER w / p.close - 1
                    ELSE NULL
                END AS fwd_return_1d,

                CASE
                    WHEN p.close IS NOT NULL
                     AND p.close <> 0
                     AND LEAD(p.close, 5) OVER w IS NOT NULL
                    THEN LEAD(p.close, 5) OVER w / p.close - 1
                    ELSE NULL
                END AS fwd_return_5d,

                CASE
                    WHEN p.close IS NOT NULL
                     AND p.close <> 0
                     AND LEAD(p.close, 20) OVER w IS NOT NULL
                    THEN LEAD(p.close, 20) OVER w / p.close - 1
                    ELSE NULL
                END AS fwd_return_20d

            FROM price_history p
            WHERE p.date IS NOT NULL
              AND p.symbol IS NOT NULL
              AND TRIM(p.symbol) <> ''

            WINDOW w AS (
                PARTITION BY p.symbol
                ORDER BY p.date
            )
            """
        )

        count = con.execute(
            """
            SELECT COUNT(*)
            FROM research_labels
            WHERE dataset_id = ?
            """,
            [args.dataset_id],
        ).fetchone()[0]

        print(
            json.dumps(
                {
                    "dataset_id": args.dataset_id,
                    "snapshot_id": args.snapshot_id,
                    "rows": int(count),
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
