#!/usr/bin/env python3
from __future__ import annotations

"""
Incremental, SQL-first, memory-safe builder for research_training_dataset.

Objectif
--------
Construire research_training_dataset à partir de research_features_daily
en mode:
- chunké par mois
- résumable / incrémental par dataset_id
- PIT-safe
- memory-safe

Pourquoi cette version
----------------------
L'ancienne logique faisait un as-of backward join sur un gros historique
de features à chaque chunk mensuel, ce qui faisait exploser la mémoire.

La bonne logique ici est:
- research_features_daily est déjà le feature store quotidien canonique
- le dataset d'entraînement n'est qu'une projection de cette table
- on lit seulement le mois demandé
- on saute les mois déjà chargés pour un dataset_id donné

Notes importantes
-----------------
- pour reprendre un dataset interrompu, il faut réutiliser le même dataset_id
- si un mois est partiellement chargé, on supprime ce mois puis on le reconstruit
"""

import argparse
import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import duckdb
from dateutil.relativedelta import relativedelta
from tqdm import tqdm

from stock_quant.infrastructure.db.research_training_dataset_schema import (
    ResearchTrainingDatasetSchemaManager,
)


TARGET_FEATURE_COLUMNS: list[tuple[str, str]] = [
    ("close", "DOUBLE"),
    ("returns_1d", "DOUBLE"),
    ("returns_5d", "DOUBLE"),
    ("returns_20d", "DOUBLE"),
    ("sma_20", "DOUBLE"),
    ("sma_50", "DOUBLE"),
    ("sma_200", "DOUBLE"),
    ("close_to_sma_20", "DOUBLE"),
    ("rsi_14", "DOUBLE"),
    ("atr_14", "DOUBLE"),
    ("volatility_20", "DOUBLE"),
    ("short_volume_ratio", "DOUBLE"),
]


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


def _month_range(start: date, end: date):
    cur = start.replace(day=1)
    while cur <= end:
        nxt = (cur + relativedelta(months=1)) - timedelta(days=1)
        yield cur, min(nxt, end)
        cur = cur + relativedelta(months=1)


def _sql_symbol_type_case(symbol_expr: str) -> str:
    return f"""
        CASE
            WHEN UPPER(TRIM({symbol_expr})) IN ('ZVZZT')
                THEN 'TEST_OR_SPECIAL'
            WHEN regexp_matches(UPPER(TRIM({symbol_expr})), '.*W$')
                THEN 'LIKELY_WARRANT'
            WHEN regexp_matches(UPPER(TRIM({symbol_expr})), '.*R$')
                THEN 'LIKELY_RIGHT'
            WHEN regexp_matches(UPPER(TRIM({symbol_expr})), '.*U$')
                THEN 'LIKELY_UNIT'
            ELSE 'COMMON_OR_OTHER'
        END
    """


def _sql_allowed_symbol_predicate(symbol_expr: str, symbol_filter_mode: str) -> str:
    if symbol_filter_mode == "all":
        return "TRUE"

    if symbol_filter_mode == "common_only":
        return f"({_sql_symbol_type_case(symbol_expr)}) = 'COMMON_OR_OTHER'"

    raise ValueError(f"unsupported symbol_filter_mode={symbol_filter_mode}")


def _ensure_dataset_schema_has_feature_columns(con: duckdb.DuckDBPyConnection) -> None:
    """
    Garde la compatibilité avec le schema manager existant, puis ajoute les
    colonnes de features nécessaires si elles manquent.
    """
    ResearchTrainingDatasetSchemaManager(con).ensure_tables()

    existing_columns = {
        str(row[1]).strip()
        for row in con.execute("PRAGMA table_info('research_training_dataset')").fetchall()
    }

    for column_name, column_type in TARGET_FEATURE_COLUMNS:
        if column_name not in existing_columns:
            con.execute(
                f"ALTER TABLE research_training_dataset ADD COLUMN {column_name} {column_type}"
            )


def _target_feature_columns_from_source(
    con: duckdb.DuckDBPyConnection,
) -> list[str]:
    """
    Retourne uniquement les colonnes features présentes à la fois dans la source
    research_features_daily et dans la cible research_training_dataset.
    """
    source_columns = {
        str(row[1]).strip()
        for row in con.execute("PRAGMA table_info('research_features_daily')").fetchall()
    }
    target_columns = {
        str(row[1]).strip()
        for row in con.execute("PRAGMA table_info('research_training_dataset')").fetchall()
    }

    selected = [
        col_name
        for col_name, _col_type in TARGET_FEATURE_COLUMNS
        if col_name in source_columns and col_name in target_columns
    ]
    return selected


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--db-path", required=True)
    p.add_argument("--snapshot-id", required=True)
    p.add_argument("--split-id", required=True)
    p.add_argument(
        "--dataset-id",
        default=None,
        help="Reuse an existing dataset_id to resume an interrupted build incrementally.",
    )
    p.add_argument("--memory-limit", default="24GB")
    p.add_argument("--threads", type=int, default=6)
    p.add_argument("--temp-dir", default="/home/marty/stock-quant-oop/tmp")
    p.add_argument(
        "--symbol-filter-mode",
        default="common_only",
        choices=["all", "common_only"],
    )
    p.add_argument("--verbose", action="store_true")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    db_path = Path(args.db_path).expanduser().resolve()

    print(f"[build_research_training_dataset] db_path={db_path}", flush=True)

    con = duckdb.connect(str(db_path))
    try:
        # --------------------------------------------------------------
        # DuckDB runtime aligné avec le reste du codebase research.
        # --------------------------------------------------------------
        con.execute(f"PRAGMA memory_limit='{args.memory_limit}'")
        con.execute(f"PRAGMA threads={args.threads}")
        con.execute("PRAGMA preserve_insertion_order=false")

        temp_dir_path = Path(args.temp_dir).expanduser().resolve()
        temp_dir_path.mkdir(parents=True, exist_ok=True)
        temp_dir_sql = str(temp_dir_path).replace("'", "''")
        con.execute(f"PRAGMA temp_directory='{temp_dir_sql}'")

        # --------------------------------------------------------------
        # Validation source.
        # --------------------------------------------------------------
        if not _table_exists(con, "research_features_daily"):
            raise RuntimeError("research_features_daily does not exist")

        source_feature_rows = int(
            con.execute("SELECT COUNT(*) FROM research_features_daily").fetchone()[0]
        )
        if source_feature_rows == 0:
            raise RuntimeError(
                "research_features_daily is empty; build/populate features before building the training dataset"
            )

        # --------------------------------------------------------------
        # Préparation schéma cible.
        # --------------------------------------------------------------
        _ensure_dataset_schema_has_feature_columns(con)

        # --------------------------------------------------------------
        # Snapshot manifest.
        # --------------------------------------------------------------
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

        # --------------------------------------------------------------
        # Split manifest.
        # --------------------------------------------------------------
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
            train_start,
            train_end,
            valid_start,
            valid_end,
            test_start,
            test_end,
            embargo_days,
        ) = split

        dataset_id = args.dataset_id or _dataset_id(args.snapshot_id, args.split_id)
        target_feature_columns = _target_feature_columns_from_source(con)

        if not target_feature_columns:
            raise RuntimeError(
                "No overlapping feature columns found between research_features_daily and research_training_dataset"
            )

        print(f"[builder] dataset_id={dataset_id}", flush=True)
        print(f"[builder] snapshot_id={args.snapshot_id}", flush=True)
        print(f"[builder] split_id={args.split_id}", flush=True)
        print(f"[builder] source_feature_rows={source_feature_rows}", flush=True)
        print(f"[builder] target_feature_columns={target_feature_columns}", flush=True)
        print(f"[builder] memory_limit={args.memory_limit}", flush=True)
        print(f"[builder] threads={args.threads}", flush=True)
        print(f"[builder] temp_dir={temp_dir_sql}", flush=True)
        print(f"[builder] embargo_days={embargo_days}", flush=True)
        print(f"[builder] symbol_filter_mode={args.symbol_filter_mode}", flush=True)
        print(f"[builder] verbose={args.verbose}", flush=True)

        partitions = [
            ("train", train_start, train_end),
            ("valid", valid_start, valid_end),
            ("test", test_start, test_end),
        ]

        allowed_symbol_predicate = _sql_allowed_symbol_predicate(
            "symbol",
            args.symbol_filter_mode,
        )

        # Colonnes dynamiques pour projection source -> cible.
        feature_select_sql = ",\n                            ".join(
            [f"f.{col}" for col in target_feature_columns]
        )
        feature_insert_sql = ",\n                            ".join(target_feature_columns)

        inserted_rows_total = 0
        skipped_rows_total = 0

        for partition_name, partition_start, partition_end in partitions:
            if partition_start is None or partition_end is None:
                print(f"[partition_skip] {partition_name} reason=missing_dates", flush=True)
                continue

            month_windows = list(_month_range(partition_start, partition_end))

            print(
                f"\n===== PARTITION {partition_name} {partition_start} -> {partition_end} =====",
                flush=True,
            )

            for month_start, month_end in tqdm(
                month_windows,
                desc=f"{partition_name} months",
                dynamic_ncols=True,
            ):
                print(
                    f"[chunk] partition={partition_name} start={month_start} end={month_end}",
                    flush=True,
                )

                # ------------------------------------------------------
                # Source chunk = seulement les lignes du mois.
                # C'est le point clé anti-OOM.
                # ------------------------------------------------------
                con.execute("DROP TABLE IF EXISTS tmp_feature_chunk")
                con.execute(
                    f"""
                    CREATE TEMP TABLE tmp_feature_chunk AS
                    SELECT
                        UPPER(TRIM(symbol)) AS symbol,
                        as_of_date,
                        {", ".join(target_feature_columns)},
                        {_sql_symbol_type_case("symbol")} AS symbol_type
                    FROM research_features_daily
                    WHERE as_of_date BETWEEN ? AND ?
                      AND as_of_date IS NOT NULL
                      AND symbol IS NOT NULL
                      AND TRIM(symbol) <> ''
                      AND {allowed_symbol_predicate}
                    """,
                    [month_start, month_end],
                )

                source_month_rows = int(
                    con.execute("SELECT COUNT(*) FROM tmp_feature_chunk").fetchone()[0]
                )

                if args.verbose:
                    mix_rows = con.execute(
                        """
                        SELECT
                            symbol_type,
                            COUNT(DISTINCT symbol) AS distinct_symbols,
                            COUNT(*) AS row_count
                        FROM tmp_feature_chunk
                        GROUP BY symbol_type
                        ORDER BY row_count DESC, symbol_type
                        """
                    ).fetchall()
                    print(
                        f"[feature_chunk_mix] partition={partition_name} start={month_start} end={month_end} mix={mix_rows}",
                        flush=True,
                    )

                if source_month_rows == 0:
                    print(
                        f"[chunk_done] partition={partition_name} start={month_start} end={month_end} inserted_rows=0 reason=no_feature_rows",
                        flush=True,
                    )
                    continue

                # ------------------------------------------------------
                # Mode incremental / resume:
                # - si le mois est déjà complet pour ce dataset_id, on skip
                # - s'il est partiel, on le delete et on le reconstruit
                # ------------------------------------------------------
                existing_month_rows = int(
                    con.execute(
                        """
                        SELECT COUNT(*)
                        FROM research_training_dataset
                        WHERE dataset_id = ?
                          AND as_of_date BETWEEN ? AND ?
                        """,
                        [dataset_id, month_start, month_end],
                    ).fetchone()[0]
                )

                if existing_month_rows == source_month_rows:
                    skipped_rows_total += existing_month_rows
                    print(
                        f"[chunk_skip] partition={partition_name} start={month_start} end={month_end} "
                        f"existing_rows={existing_month_rows} reason=already_complete",
                        flush=True,
                    )
                    continue

                if existing_month_rows > 0:
                    con.execute(
                        """
                        DELETE FROM research_training_dataset
                        WHERE dataset_id = ?
                          AND as_of_date BETWEEN ? AND ?
                        """,
                        [dataset_id, month_start, month_end],
                    )
                    print(
                        f"[chunk_reset] partition={partition_name} start={month_start} end={month_end} "
                        f"deleted_partial_rows={existing_month_rows}",
                        flush=True,
                    )

                con.execute(
                    f"""
                    INSERT INTO research_training_dataset (
                        dataset_id,
                        snapshot_id,
                        symbol,
                        as_of_date,
                        {feature_insert_sql}
                    )
                    SELECT
                        ? AS dataset_id,
                        ? AS snapshot_id,
                        f.symbol,
                        f.as_of_date,
                        {feature_select_sql}
                    FROM tmp_feature_chunk f
                    """,
                    [dataset_id, args.snapshot_id],
                )

                inserted_rows_total += source_month_rows

                current_rows = int(
                    con.execute(
                        """
                        SELECT COUNT(*)
                        FROM research_training_dataset
                        WHERE dataset_id = ?
                        """,
                        [dataset_id],
                    ).fetchone()[0]
                )

                print(
                    f"[chunk_done] partition={partition_name} start={month_start} end={month_end} "
                    f"source_rows={source_month_rows} dataset_rows={current_rows}",
                    flush=True,
                )

            partition_probe = con.execute(
                """
                SELECT
                    COUNT(*) AS rows,
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
                f"min_date={partition_probe[1]} max_date={partition_probe[2]}",
                flush=True,
            )

        row_count = int(
            con.execute(
                """
                SELECT COUNT(*)
                FROM research_training_dataset
                WHERE dataset_id = ?
                """,
                [dataset_id],
            ).fetchone()[0]
        )

        symbol_mix_final = con.execute(
            f"""
            WITH typed AS (
                SELECT
                    {_sql_symbol_type_case("symbol")} AS symbol_type,
                    symbol
                FROM research_training_dataset
                WHERE dataset_id = ?
            )
            SELECT
                symbol_type,
                COUNT(DISTINCT symbol) AS distinct_symbols,
                COUNT(*) AS row_count
            FROM typed
            GROUP BY symbol_type
            ORDER BY row_count DESC, symbol_type
            """,
            [dataset_id],
        ).fetchall()

        output = {
            "dataset_id": dataset_id,
            "snapshot_id": args.snapshot_id,
            "split_id": args.split_id,
            "row_count": row_count,
            "used_research_features_daily": True,
            "source_feature_rows": source_feature_rows,
            "inserted_rows_total": inserted_rows_total,
            "skipped_rows_total": skipped_rows_total,
            "join_mode": "direct_monthly_projection_incremental",
            "date_window": {
                "start": str(train_start),
                "end": str(test_end),
            },
            "symbol_filter_mode": args.symbol_filter_mode,
            "target_feature_columns": target_feature_columns,
            "symbol_mix_final": [
                {
                    "symbol_type": row[0],
                    "distinct_symbols": int(row[1]),
                    "row_count": int(row[2]),
                }
                for row in symbol_mix_final
            ],
        }

        print("\n===== BUILD COMPLETE =====", flush=True)
        print(json.dumps(output, indent=2), flush=True)
        return 0
    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
