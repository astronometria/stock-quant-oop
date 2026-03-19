#!/usr/bin/env python3
from __future__ import annotations

"""
Chunked, SQL-first, memory-safe builder for research_training_dataset.

Objectif
--------
Construire research_training_dataset sans OOM, à partir de:
- price_history
- short_features_daily

Principes
---------
- chunking par mois
- SQL-first
- PIT-safe / asof backward
- logs visibles
- insert avec colonnes explicites, aligné au schéma réel

Schéma cible attendu
--------------------
Le schéma de recherche actuel attend au minimum:
- dataset_id
- snapshot_id
- symbol
- as_of_date
- close
- short_volume_ratio
- created_at (DEFAULT CURRENT_TIMESTAMP)

On n'insère donc volontairement que les 6 premières colonnes métiers.
"""

import argparse
import json
from datetime import datetime, timezone, date, timedelta
from pathlib import Path

import duckdb
from dateutil.relativedelta import relativedelta
from tqdm import tqdm

from stock_quant.infrastructure.db.research_training_dataset_schema import (
    ResearchTrainingDatasetSchemaManager,
)


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _dataset_id(snapshot_id: str, split_id: str) -> str:
    stamp = _now_utc().strftime("%Y%m%dT%H%M%SZ")
    return f"{snapshot_id}_{split_id}_dataset_{stamp}"


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


def _month_range(start: date, end: date):
    """
    Retourne des fenêtres mensuelles [month_start, month_end].

    Pourquoi:
    - borne la taille des joins
    - réduit drastiquement le temp spill
    - rend la progression visible
    """
    cur = start.replace(day=1)
    while cur <= end:
        nxt = (cur + relativedelta(months=1)) - timedelta(days=1)
        yield cur, min(nxt, end)
        cur = cur + relativedelta(months=1)


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--db-path", required=True)
    p.add_argument("--snapshot-id", required=True)
    p.add_argument("--split-id", required=True)
    p.add_argument("--dataset-id", default=None)
    p.add_argument("--memory-limit", default="24GB")
    p.add_argument("--threads", type=int, default=6)
    p.add_argument("--temp-dir", default="/home/marty/stock-quant-oop/tmp")
    return p.parse_args()


def _table_stats_for_dataset(
    con: duckdb.DuckDBPyConnection,
    dataset_id: str,
) -> tuple[int, int | None, int | None]:
    row = con.execute(
        """
        SELECT
            COUNT(*) AS rows,
            COUNT(short_volume_ratio) AS signal_rows,
            COUNT(DISTINCT symbol) AS symbols
        FROM research_training_dataset
        WHERE dataset_id = ?
        """,
        [dataset_id],
    ).fetchone()
    return int(row[0] or 0), int(row[1] or 0), int(row[2] or 0)


def main() -> int:
    args = parse_args()
    db_path = Path(args.db_path).expanduser().resolve()

    print(f"[build_research_training_dataset] db_path={db_path}", flush=True)

    con = duckdb.connect(str(db_path))
    try:
        # ------------------------------------------------------------------
        # Réglages DuckDB pour limiter la pression mémoire et permettre
        # le spilling disque.
        # ------------------------------------------------------------------
        con.execute(f"PRAGMA memory_limit='{args.memory_limit}'")
        con.execute(f"PRAGMA threads={args.threads}")
        con.execute("PRAGMA preserve_insertion_order=false")
        temp_dir_sql = str(Path(args.temp_dir).expanduser().resolve()).replace("'", "''")
        con.execute(f"PRAGMA temp_directory='{temp_dir_sql}'")

        ResearchTrainingDatasetSchemaManager(con).ensure_tables()

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

        split = con.execute(
            """
            SELECT
                split_id,
                train_start, train_end,
                valid_start, valid_end,
                test_start, test_end,
                embargo_days
            FROM research_split_manifest
            WHERE split_id = ?
            """,
            [args.split_id],
        ).fetchone()
        if split is None:
            raise RuntimeError("split_id not found")

        (
            split_id,
            train_start, train_end,
            valid_start, valid_end,
            test_start, test_end,
            embargo_days,
        ) = split

        price_date_col = _choose_price_date_column(con, "price_history")
        has_short_features = _table_exists(con, "short_features_daily")

        dataset_id = args.dataset_id or _dataset_id(args.snapshot_id, args.split_id)

        print(f"[builder] dataset_id={dataset_id}", flush=True)
        print(f"[builder] snapshot_id={args.snapshot_id}", flush=True)
        print(f"[builder] split_id={args.split_id}", flush=True)
        print(f"[builder] price_date_column={price_date_col}", flush=True)
        print(f"[builder] has_short_features={has_short_features}", flush=True)
        print(f"[builder] memory_limit={args.memory_limit}", flush=True)
        print(f"[builder] threads={args.threads}", flush=True)
        print(f"[builder] temp_dir={temp_dir_sql}", flush=True)
        print(f"[builder] embargo_days={embargo_days}", flush=True)

        con.execute(
            """
            DELETE FROM research_training_dataset
            WHERE dataset_id = ?
            """,
            [dataset_id],
        )

        partitions = [
            ("train", train_start, train_end),
            ("valid", valid_start, valid_end),
            ("test", test_start, test_end),
        ]

        total_inserted = 0

        for partition_name, partition_start, partition_end in partitions:
            print(
                f"\n===== PARTITION {partition_name} {partition_start} -> {partition_end} =====",
                flush=True,
            )

            months = list(_month_range(partition_start, partition_end))

            for month_start, month_end in tqdm(
                months,
                desc=f"{partition_name} months",
                dynamic_ncols=True,
                mininterval=0.5,
            ):
                print(
                    f"[chunk] partition={partition_name} start={month_start} end={month_end}",
                    flush=True,
                )

                # ----------------------------------------------------------
                # Prix bornés à la fenêtre du chunk.
                # ----------------------------------------------------------
                con.execute("DROP TABLE IF EXISTS tmp_price_chunk")
                con.execute(
                    f"""
                    CREATE TEMP TABLE tmp_price_chunk AS
                    SELECT
                        symbol,
                        {price_date_col} AS as_of_date,
                        close
                    FROM price_history
                    WHERE {price_date_col} BETWEEN ? AND ?
                      AND {price_date_col} IS NOT NULL
                      AND symbol IS NOT NULL
                      AND TRIM(symbol) <> ''
                    """,
                    [month_start, month_end],
                )

                price_rows = con.execute(
                    "SELECT COUNT(*) FROM tmp_price_chunk"
                ).fetchone()[0]

                if price_rows == 0:
                    print(
                        f"[chunk_done] partition={partition_name} start={month_start} end={month_end} inserted_rows=0 reason=no_price_rows",
                        flush=True,
                    )
                    continue

                if has_short_features:
                    # ------------------------------------------------------
                    # Features bornées à ce qui est nécessaire pour ce chunk:
                    # on garde seulement les lignes <= month_end.
                    #
                    # Oui, ce n'est pas le filtrage le plus fin possible,
                    # mais c'est déjà beaucoup plus memory-safe que le join
                    # global sur tout l'historique.
                    # ------------------------------------------------------
                    con.execute("DROP TABLE IF EXISTS tmp_short_chunk")
                    con.execute(
                        """
                        CREATE TEMP TABLE tmp_short_chunk AS
                        SELECT
                            symbol,
                            as_of_date,
                            short_volume_ratio
                        FROM short_features_daily
                        WHERE as_of_date <= ?
                          AND symbol IS NOT NULL
                          AND TRIM(symbol) <> ''
                        """,
                        [month_end],
                    )

                    con.execute(
                        """
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
                            p.as_of_date,
                            p.close,
                            f.short_volume_ratio
                        FROM tmp_price_chunk p
                        LEFT JOIN LATERAL (
                            SELECT
                                short_volume_ratio
                            FROM tmp_short_chunk f
                            WHERE f.symbol = p.symbol
                              AND f.as_of_date <= p.as_of_date
                            ORDER BY f.as_of_date DESC
                            LIMIT 1
                        ) f ON TRUE
                        """,
                        [dataset_id, args.snapshot_id],
                    )
                else:
                    con.execute(
                        """
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
                            p.as_of_date,
                            p.close,
                            NULL AS short_volume_ratio
                        FROM tmp_price_chunk p
                        """,
                        [dataset_id, args.snapshot_id],
                    )

                current_rows, current_signal_rows, current_symbols = _table_stats_for_dataset(
                    con, dataset_id
                )

                print(
                    f"[chunk_done] partition={partition_name} start={month_start} end={month_end} "
                    f"chunk_price_rows={price_rows} dataset_rows={current_rows} "
                    f"signal_rows={current_signal_rows} symbols={current_symbols}",
                    flush=True,
                )

            partition_probe = con.execute(
                """
                SELECT
                    COUNT(*) AS rows,
                    COUNT(short_volume_ratio) AS signal_rows,
                    MIN(as_of_date) AS min_date,
                    MAX(as_of_date) AS max_date
                FROM research_training_dataset
                WHERE dataset_id = ?
                  AND as_of_date BETWEEN ? AND ?
                """,
                [dataset_id, partition_start, partition_end],
            ).fetchone()

            print(
                f"[partition_done] {partition_name} rows={partition_probe[0]} "
                f"signal_rows={partition_probe[1]} min_date={partition_probe[2]} max_date={partition_probe[3]}",
                flush=True,
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
            "split_id": args.split_id,
            "row_count": int(row_count),
            "used_short_features_daily": has_short_features,
            "price_date_column": price_date_col,
            "join_mode": "asof_backward_chunked" if has_short_features else "prices_only_chunked",
            "date_window": {
                "start": str(train_start),
                "end": str(test_end),
            },
        }

        print("\n===== BUILD COMPLETE =====", flush=True)
        print(json.dumps(output, indent=2), flush=True)
        return 0

    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
