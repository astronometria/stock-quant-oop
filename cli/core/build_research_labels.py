#!/usr/bin/env python3
from __future__ import annotations

"""
Scientific-grade, SQL-first label builder for research_labels.

Objectif
--------
Construire des labels forward returns robustes à partir de price_history, avec:
- garde-fous contre les prix nuls / négatifs
- filtre anti-outliers extrêmes
- paramètres runtime alignés avec les autres builders
- logs visibles en mode verbose
- sortie JSON finale compatible avec run_research_experiment.py

Principes
---------
- SQL-first: DuckDB fait le gros du travail
- PIT-safe: uniquement des LEAD() sur la série de prix
- Python mince: orchestration + logs + configuration
- Robustesse: on neutralise les labels absurdes plutôt que de laisser le backtest exploser

Important
---------
On garde volontairement les lignes du dataset, mais les labels extrêmes deviennent NULL.
Cela permet:
- de conserver la reproductibilité du dataset
- d'éviter que quelques séries corrompues dominent les résultats
"""

import argparse
import json
from pathlib import Path

import duckdb


def _table_exists(con: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    """
    Vérifie l'existence d'une table dans la base courante.

    Pourquoi:
    - certains environnements de test peuvent avoir un schéma partiel
    - on veut une erreur explicite si price_history ou le schéma research manque
    """
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
    """
    Détecte la colonne date effective de la table de prix.

    Pourquoi:
    - le projet a déjà vécu plusieurs itérations de schéma
    - on veut rester compatible si le nom exact a changé
    """
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
    """
    Parse les arguments CLI.

    On aligne volontairement cette interface sur les autres builders research pour
    simplifier l'orchestration depuis run_research_experiment.py.
    """
    p = argparse.ArgumentParser()
    p.add_argument("--db-path", required=True)
    p.add_argument("--snapshot-id", required=True)
    p.add_argument("--dataset-id", required=True)
    p.add_argument("--split-id", required=True)

    # Réglages DuckDB / runtime alignés avec les autres scripts.
    p.add_argument("--memory-limit", default="24GB")
    p.add_argument("--threads", type=int, default=6)
    p.add_argument("--temp-dir", default="/home/marty/stock-quant-oop/tmp")
    p.add_argument("--verbose", action="store_true")

    # Garde-fou sanitaire configurable.
    p.add_argument(
        "--max-abs-return",
        type=float,
        default=1.0,
        help=(
            "Maximum absolute forward return allowed before the label is nulled. "
            "Default=1.0 means any move beyond +/-100%% is discarded."
        ),
    )
    return p.parse_args()


def main() -> int:
    args = parse_args()
    db_path = Path(args.db_path).expanduser().resolve()

    print(f"[build_research_labels] db_path={db_path}", flush=True)

    con = duckdb.connect(str(db_path))
    try:
        # ------------------------------------------------------------------
        # Réglages DuckDB.
        # ------------------------------------------------------------------
        con.execute(f"PRAGMA memory_limit='{args.memory_limit}'")
        con.execute(f"PRAGMA threads={args.threads}")
        con.execute("PRAGMA preserve_insertion_order=false")

        temp_dir_sql = str(Path(args.temp_dir).expanduser().resolve()).replace("'", "''")
        con.execute(f"PRAGMA temp_directory='{temp_dir_sql}'")

        # ------------------------------------------------------------------
        # Validation schéma minimal requis.
        # ------------------------------------------------------------------
        if not _table_exists(con, "research_labels"):
            raise RuntimeError("research_labels table not found; initialize research foundation first")

        if not _table_exists(con, "research_dataset_manifest"):
            raise RuntimeError("research_dataset_manifest table not found")

        if not _table_exists(con, "research_split_manifest"):
            raise RuntimeError("research_split_manifest table not found")

        if not _table_exists(con, "price_history"):
            raise RuntimeError("price_history table not found")

        price_date_col = _choose_price_date_column(con, "price_history")

        # ------------------------------------------------------------------
        # Validation snapshot + split.
        # ------------------------------------------------------------------
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

        split = con.execute(
            """
            SELECT
                split_id,
                train_start,
                train_end,
                valid_start,
                valid_end,
                test_start,
                test_end,
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

        if args.max_abs_return <= 0:
            raise RuntimeError("--max-abs-return must be > 0")

        if args.verbose:
            print(f"[labels] memory_limit={args.memory_limit}", flush=True)
            print(f"[labels] threads={args.threads}", flush=True)
            print(f"[labels] temp_dir={temp_dir_sql}", flush=True)
            print(f"[labels] price_date_column={price_date_col}", flush=True)
            print(f"[labels] date_range={train_start} -> {test_end}", flush=True)
            print(f"[labels] embargo_days={embargo_days}", flush=True)
            print(f"[labels] max_abs_return={args.max_abs_return}", flush=True)

        # ------------------------------------------------------------------
        # Suppression de l'ancien dataset labels.
        # ------------------------------------------------------------------
        con.execute(
            """
            DELETE FROM research_labels
            WHERE dataset_id = ?
            """,
            [args.dataset_id],
        )

        if args.verbose:
            print("[labels] previous dataset cleared", flush=True)

        # ------------------------------------------------------------------
        # Construction des labels.
        #
        # Stratégie:
        # 1) on calcule les close forward avec LEAD()
        # 2) on calcule les rendements bruts
        # 3) on NULL les labels invalides / extrêmes
        #
        # Pourquoi ne pas supprimer la ligne ?
        # - on préfère garder le dataset aligné par (symbol, as_of_date)
        # - le backtest ne prendra que les labels non nuls
        # ------------------------------------------------------------------
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
            WITH base AS (
                SELECT
                    p.symbol,
                    p.{price_date_col} AS as_of_date,
                    p.close AS close_t,

                    LEAD(p.close, 1) OVER w AS close_t1,
                    LEAD(p.close, 5) OVER w AS close_t5,
                    LEAD(p.close, 20) OVER w AS close_t20

                FROM price_history p
                WHERE p.{price_date_col} IS NOT NULL
                  AND p.symbol IS NOT NULL
                  AND TRIM(p.symbol) <> ''
                  AND p.{price_date_col} BETWEEN ? AND ?
                WINDOW w AS (
                    PARTITION BY p.symbol
                    ORDER BY p.{price_date_col}
                )
            ),
            raw_returns AS (
                SELECT
                    symbol,
                    as_of_date,
                    close_t,
                    close_t1,
                    close_t5,
                    close_t20,

                    CASE
                        WHEN close_t IS NULL OR close_t <= 0 THEN NULL
                        WHEN close_t1 IS NULL OR close_t1 <= 0 THEN NULL
                        ELSE (close_t1 / close_t) - 1
                    END AS raw_fwd_return_1d,

                    CASE
                        WHEN close_t IS NULL OR close_t <= 0 THEN NULL
                        WHEN close_t5 IS NULL OR close_t5 <= 0 THEN NULL
                        ELSE (close_t5 / close_t) - 1
                    END AS raw_fwd_return_5d,

                    CASE
                        WHEN close_t IS NULL OR close_t <= 0 THEN NULL
                        WHEN close_t20 IS NULL OR close_t20 <= 0 THEN NULL
                        ELSE (close_t20 / close_t) - 1
                    END AS raw_fwd_return_20d

                FROM base
            )
            SELECT
                ? AS dataset_id,
                ? AS snapshot_id,
                symbol,
                as_of_date,

                CASE
                    WHEN raw_fwd_return_1d IS NULL THEN NULL
                    WHEN ABS(raw_fwd_return_1d) > ? THEN NULL
                    ELSE raw_fwd_return_1d
                END AS fwd_return_1d,

                CASE
                    WHEN raw_fwd_return_5d IS NULL THEN NULL
                    WHEN ABS(raw_fwd_return_5d) > ? THEN NULL
                    ELSE raw_fwd_return_5d
                END AS fwd_return_5d,

                CASE
                    WHEN raw_fwd_return_20d IS NULL THEN NULL
                    WHEN ABS(raw_fwd_return_20d) > ? THEN NULL
                    ELSE raw_fwd_return_20d
                END AS fwd_return_20d

            FROM raw_returns
            """,
            [
                train_start,
                test_end,
                args.dataset_id,
                args.snapshot_id,
                args.max_abs_return,
                args.max_abs_return,
                args.max_abs_return,
            ],
        )

        # ------------------------------------------------------------------
        # Statistiques de qualité.
        #
        # On recalcule les stats depuis la table cible pour:
        # - exposer des métriques utiles au log final
        # - faciliter les probes en prod
        # ------------------------------------------------------------------
        stats = con.execute(
            """
            SELECT
                COUNT(*) AS total_rows,

                COUNT(fwd_return_1d) AS labeled_rows_1d,
                COUNT(fwd_return_5d) AS labeled_rows_5d,
                COUNT(fwd_return_20d) AS labeled_rows_20d,

                MIN(as_of_date) AS min_as_of_date,
                MAX(as_of_date) AS max_as_of_date,

                MIN(fwd_return_1d) AS min_ret_1d,
                MAX(fwd_return_1d) AS max_ret_1d,
                AVG(fwd_return_1d) AS avg_ret_1d,
                STDDEV_SAMP(fwd_return_1d) AS std_ret_1d
            FROM research_labels
            WHERE dataset_id = ?
            """,
            [args.dataset_id],
        ).fetchone()

        # Comptes d'anomalies calculés directement sur price_history sur la même fenêtre.
        anomaly_stats = con.execute(
            f"""
            WITH base AS (
                SELECT
                    p.symbol,
                    p.{price_date_col} AS as_of_date,
                    p.close AS close_t,
                    LEAD(p.close, 1) OVER (
                        PARTITION BY p.symbol
                        ORDER BY p.{price_date_col}
                    ) AS close_t1,
                    LEAD(p.close, 5) OVER (
                        PARTITION BY p.symbol
                        ORDER BY p.{price_date_col}
                    ) AS close_t5,
                    LEAD(p.close, 20) OVER (
                        PARTITION BY p.symbol
                        ORDER BY p.{price_date_col}
                    ) AS close_t20
                FROM price_history p
                WHERE p.{price_date_col} IS NOT NULL
                  AND p.symbol IS NOT NULL
                  AND TRIM(p.symbol) <> ''
                  AND p.{price_date_col} BETWEEN ? AND ?
            ),
            raw_returns AS (
                SELECT
                    *,
                    CASE
                        WHEN close_t IS NULL OR close_t <= 0 THEN NULL
                        WHEN close_t1 IS NULL OR close_t1 <= 0 THEN NULL
                        ELSE (close_t1 / close_t) - 1
                    END AS raw_fwd_return_1d,
                    CASE
                        WHEN close_t IS NULL OR close_t <= 0 THEN NULL
                        WHEN close_t5 IS NULL OR close_t5 <= 0 THEN NULL
                        ELSE (close_t5 / close_t) - 1
                    END AS raw_fwd_return_5d,
                    CASE
                        WHEN close_t IS NULL OR close_t <= 0 THEN NULL
                        WHEN close_t20 IS NULL OR close_t20 <= 0 THEN NULL
                        ELSE (close_t20 / close_t) - 1
                    END AS raw_fwd_return_20d
                FROM base
            )
            SELECT
                SUM(CASE WHEN close_t IS NULL OR close_t <= 0 THEN 1 ELSE 0 END) AS bad_close_t_rows,
                SUM(CASE WHEN close_t1 IS NOT NULL AND close_t1 <= 0 THEN 1 ELSE 0 END) AS bad_close_t1_rows,
                SUM(CASE WHEN close_t5 IS NOT NULL AND close_t5 <= 0 THEN 1 ELSE 0 END) AS bad_close_t5_rows,
                SUM(CASE WHEN close_t20 IS NOT NULL AND close_t20 <= 0 THEN 1 ELSE 0 END) AS bad_close_t20_rows,

                SUM(CASE WHEN raw_fwd_return_1d IS NOT NULL AND ABS(raw_fwd_return_1d) > ? THEN 1 ELSE 0 END) AS extreme_1d_rows,
                SUM(CASE WHEN raw_fwd_return_5d IS NOT NULL AND ABS(raw_fwd_return_5d) > ? THEN 1 ELSE 0 END) AS extreme_5d_rows,
                SUM(CASE WHEN raw_fwd_return_20d IS NOT NULL AND ABS(raw_fwd_return_20d) > ? THEN 1 ELSE 0 END) AS extreme_20d_rows
            FROM raw_returns
            """,
            [
                train_start,
                test_end,
                args.max_abs_return,
                args.max_abs_return,
                args.max_abs_return,
            ],
        ).fetchone()

        result = {
            "dataset_id": args.dataset_id,
            "snapshot_id": args.snapshot_id,
            "split_id": split_id,
            "rows": int(stats[0] or 0),
            "price_date_column": price_date_col,
            "date_window": {
                "start": str(train_start),
                "end": str(test_end),
            },
            "quality": {
                "max_abs_return": float(args.max_abs_return),
                "labeled_rows_1d": int(stats[1] or 0),
                "labeled_rows_5d": int(stats[2] or 0),
                "labeled_rows_20d": int(stats[3] or 0),
                "bad_close_t_rows": int(anomaly_stats[0] or 0),
                "bad_close_t1_rows": int(anomaly_stats[1] or 0),
                "bad_close_t5_rows": int(anomaly_stats[2] or 0),
                "bad_close_t20_rows": int(anomaly_stats[3] or 0),
                "extreme_1d_rows_filtered": int(anomaly_stats[4] or 0),
                "extreme_5d_rows_filtered": int(anomaly_stats[5] or 0),
                "extreme_20d_rows_filtered": int(anomaly_stats[6] or 0),
                "retained_1d_min": float(stats[6]) if stats[6] is not None else None,
                "retained_1d_max": float(stats[7]) if stats[7] is not None else None,
                "retained_1d_avg": float(stats[8]) if stats[8] is not None else None,
                "retained_1d_std": float(stats[9]) if stats[9] is not None else None,
                "min_as_of_date": str(stats[4]) if stats[4] is not None else None,
                "max_as_of_date": str(stats[5]) if stats[5] is not None else None,
            },
        }

        if args.verbose:
            print(
                "[labels] quality_summary="
                + json.dumps(result["quality"], sort_keys=True),
                flush=True,
            )

        print(json.dumps(result, indent=2), flush=True)
        return 0

    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
