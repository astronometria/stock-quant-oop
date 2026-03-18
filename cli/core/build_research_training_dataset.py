#!/usr/bin/env python3
from __future__ import annotations

"""
Build research training dataset from snapshot_id.

Principes
---------
- SQL-first
- point-in-time safe
- aucune fuite future
- as-of join backward pour short_features_daily
- robuste aux variantes de nom de colonne date dans price_history
"""

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from stock_quant.infrastructure.db.research_training_dataset_schema import (
    ResearchTrainingDatasetSchemaManager,
)


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _dataset_id(snapshot_id: str) -> str:
    stamp = _now_utc().strftime("%Y%m%dT%H%M%SZ")
    return f"{snapshot_id}_dataset_{stamp}"


def _table_exists(con: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    row = con.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE lower(table_name) = lower(?)
        """,
        [table_name],
    ).fetchone()
    return bool(row and int(row[0]) > 0)


def _choose_price_date_column(
    con: duckdb.DuckDBPyConnection,
    table_name: str = "price_history",
) -> str:
    if not _table_exists(con, table_name):
        raise RuntimeError(f"{table_name} does not exist")

    rows = con.execute(f"PRAGMA table_info('{table_name}')").fetchall()
    cols = {str(row[1]).strip().lower() for row in rows}

    preferred_order = [
        "price_date",
        "date",
        "trade_date",
        "as_of_date",
        "business_date",
    ]
    for col in preferred_order:
        if col in cols:
            return col

    for col in cols:
        if "date" in col or "time" in col:
            return col

    raise RuntimeError(
        f"unable to detect price date column in {table_name}; available columns={sorted(cols)}"
    )


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--db-path", required=True)
    p.add_argument("--snapshot-id", required=True)
    return p.parse_args()


def main() -> int:
    args = parse_args()
    db_path = Path(args.db_path).expanduser().resolve()

    print(f"[build_research_training_dataset] db_path={db_path}", flush=True)

    con = duckdb.connect(str(db_path))
    try:
        ResearchTrainingDatasetSchemaManager(con).ensure_tables()

        # 1) Échouer d'abord proprement sur le snapshot, avant tout autre probe.
        snapshot = con.execute(
            """
            SELECT snapshot_id, status
            FROM research_dataset_manifest
            WHERE snapshot_id = ?
            """,
            [args.snapshot_id],
        ).fetchone()

        if snapshot is None:
            raise RuntimeError("snapshot_id not found")

        if snapshot[1] != "completed":
            raise RuntimeError("snapshot not completed")

        # 2) Seulement ensuite, on inspecte les tables source.
        price_date_col = _choose_price_date_column(con, "price_history")
        has_short_features = _table_exists(con, "short_features_daily")

        dataset_id = _dataset_id(args.snapshot_id)

        # Idempotence simple
        con.execute(
            """
            DELETE FROM research_training_dataset
            WHERE dataset_id = ?
            """,
            [dataset_id],
        )

        if has_short_features:
            # As-of join backward PIT-safe:
            # pour chaque ligne prix, on prend la dernière feature short disponible
            # avec sf.as_of_date <= p.price_date.
            con.execute(
                f"""
                INSERT INTO research_training_dataset (
                    dataset_id,
                    snapshot_id,
                    symbol,
                    as_of_date,
                    close,
                    short_volume_ratio
                )
                WITH joined AS (
                    SELECT
                        ? AS dataset_id,
                        ? AS snapshot_id,
                        p.symbol,
                        p.{price_date_col} AS as_of_date,
                        p.close,
                        sf.short_volume_ratio,
                        ROW_NUMBER() OVER (
                            PARTITION BY p.symbol, p.{price_date_col}
                            ORDER BY sf.as_of_date DESC NULLS LAST
                        ) AS rn
                    FROM price_history p
                    LEFT JOIN short_features_daily sf
                      ON sf.symbol = p.symbol
                     AND sf.as_of_date <= p.{price_date_col}
                    WHERE p.{price_date_col} IS NOT NULL
                      AND p.symbol IS NOT NULL
                      AND TRIM(p.symbol) <> ''
                )
                SELECT
                    dataset_id,
                    snapshot_id,
                    symbol,
                    as_of_date,
                    close,
                    short_volume_ratio
                FROM joined
                WHERE rn = 1
                """,
                [dataset_id, args.snapshot_id],
            )
        else:
            con.execute(
                f"""
                INSERT INTO research_training_dataset (
                    dataset_id,
                    snapshot_id,
                    symbol,
                    as_of_date,
                    close,
                    short_volume_ratio
                )
                SELECT
                    ? AS dataset_id,
                    ? AS snapshot_id,
                    p.symbol,
                    p.{price_date_col} AS as_of_date,
                    p.close,
                    NULL AS short_volume_ratio
                FROM price_history p
                WHERE p.{price_date_col} IS NOT NULL
                  AND p.symbol IS NOT NULL
                  AND TRIM(p.symbol) <> ''
                """,
                [dataset_id, args.snapshot_id],
            )

        row_count = con.execute(
            """
            SELECT COUNT(*)
            FROM research_training_dataset
            WHERE dataset_id = ?
            """,
            [dataset_id],
        ).fetchone()[0]

        output = {
            "dataset_id": dataset_id,
            "snapshot_id": args.snapshot_id,
            "row_count": int(row_count),
            "used_short_features_daily": has_short_features,
            "price_date_column": price_date_col,
            "join_mode": "asof_backward" if has_short_features else "prices_only",
        }

        print(json.dumps(output, indent=2), flush=True)
        return 0

    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
