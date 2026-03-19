#!/usr/bin/env python3
from __future__ import annotations

"""
Dataset-scoped, SQL-first, memory-safe builder for research_labels.

Objectif
--------
Construire research_labels de façon reproductible et propre à partir
du dataset de recherche déjà figé dans research_training_dataset.

Pourquoi cette version
----------------------
L'ancienne version calculait les labels directement depuis tout price_history
dans une grande fenêtre de dates. Cela avait plusieurs défauts:
- les métriques "rows" pouvaient être trompeuses par rapport au dataset ciblé
- le travail était plus large que nécessaire
- les résultats étaient moins propres quand on compare plusieurs variantes
  de dataset (all symbols, common_only, etc.)
- les outliers extrêmes de prix pouvaient contaminer la distribution des labels

Cette version:
- est strictement dataset-scoped
- calcule les labels uniquement pour les lignes du dataset demandé
- garde une logique SQL-first
- ajoute des réglages runtime cohérents avec les autres scripts:
  --verbose
  --memory-limit
  --threads
  --temp-dir
- permet de filtrer les rendements extrêmes avec --max-abs-return
- produit un résumé qualité utile pour le diagnostic

Principes
---------
- point-in-time safe côté features: les labels restent forward-looking ici,
  mais ne sont jamais utilisés pour construire les features
- SQL-first pour les transformations
- Python mince pour l'orchestration, les validations et le reporting
- beaucoup de commentaires pour aider les autres développeurs
"""

import argparse
import json
from pathlib import Path

import duckdb

from stock_quant.infrastructure.db.research_labels_schema import (
    ResearchLabelsSchemaManager,
)


# ---------------------------------------------------------------------------
# Helpers généraux
# ---------------------------------------------------------------------------

def _table_exists(con: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    """
    Vérifie l'existence d'une table.

    Pourquoi:
    - éviter des erreurs moins lisibles plus loin
    - garder un message clair si un prérequis manque
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
    Détecte dynamiquement la colonne de date du prix.

    Pourquoi:
    - certaines bases existantes peuvent avoir "date"
    - d'autres peuvent avoir "price_date", "trade_date", etc.
    - on veut rester compatible avec la DB actuelle sans casser
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


def _normalize_temp_dir(path_str: str) -> tuple[Path, str]:
    """
    Normalise le chemin temp_dir pour l'affichage et le PRAGMA DuckDB.
    """
    path = Path(path_str).expanduser().resolve()
    sql_value = str(path).replace("'", "''")
    return path, sql_value


def parse_args() -> argparse.Namespace:
    """
    Parse les arguments CLI.

    Important:
    - on aligne l'interface avec les autres scripts lourds du pipeline
    - cela permet au runner parent de propager la config runtime sans surprise
    """
    p = argparse.ArgumentParser()
    p.add_argument("--db-path", required=True)
    p.add_argument("--snapshot-id", required=True)
    p.add_argument("--dataset-id", required=True)
    p.add_argument("--split-id", required=True)
    p.add_argument("--memory-limit", default="24GB")
    p.add_argument("--threads", type=int, default=6)
    p.add_argument("--temp-dir", default="/home/marty/stock-quant-oop/tmp")
    p.add_argument(
        "--max-abs-return",
        type=float,
        default=1.0,
        help=(
            "Set returns with absolute value greater than this threshold to NULL. "
            "Example: 1.0 means filter anything beyond +/-100%%."
        ),
    )
    p.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose diagnostic output.",
    )
    return p.parse_args()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    args = parse_args()
    db_path = Path(args.db_path).expanduser().resolve()
    temp_dir_path, temp_dir_sql = _normalize_temp_dir(args.temp_dir)

    print(f"[build_research_labels] db_path={db_path}", flush=True)

    con = duckdb.connect(str(db_path))
    try:
        # -------------------------------------------------------------------
        # Réglages DuckDB pour gros workloads.
        # -------------------------------------------------------------------
        con.execute(f"PRAGMA memory_limit='{args.memory_limit}'")
        con.execute(f"PRAGMA threads={args.threads}")
        con.execute("PRAGMA preserve_insertion_order=false")
        con.execute(f"PRAGMA temp_directory='{temp_dir_sql}'")

        ResearchLabelsSchemaManager(con).ensure_tables()

        # -------------------------------------------------------------------
        # Vérification des prérequis.
        # -------------------------------------------------------------------
        if not _table_exists(con, "research_training_dataset"):
            raise RuntimeError("research_training_dataset does not exist")

        if not _table_exists(con, "research_dataset_manifest"):
            raise RuntimeError("research_dataset_manifest does not exist")

        if not _table_exists(con, "research_split_manifest"):
            raise RuntimeError("research_split_manifest does not exist")

        if not _table_exists(con, "price_history"):
            raise RuntimeError("price_history does not exist")

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

        if str(snap[0]).strip().lower() != "completed":
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

        price_date_col = _choose_price_date_column(con, "price_history")

        # -------------------------------------------------------------------
        # Vérifie que le dataset ciblé existe réellement dans
        # research_training_dataset.
        # -------------------------------------------------------------------
        dataset_probe = con.execute(
            """
            SELECT
                COUNT(*) AS rows,
                COUNT(DISTINCT symbol) AS symbols,
                MIN(as_of_date) AS min_as_of_date,
                MAX(as_of_date) AS max_as_of_date
            FROM research_training_dataset
            WHERE dataset_id = ?
            """,
            [args.dataset_id],
        ).fetchone()

        dataset_rows = int(dataset_probe[0] or 0)
        dataset_symbols = int(dataset_probe[1] or 0)
        dataset_min_date = dataset_probe[2]
        dataset_max_date = dataset_probe[3]

        if dataset_rows == 0:
            raise RuntimeError(
                f"dataset_id not found or empty in research_training_dataset: {args.dataset_id}"
            )

        print(f"[labels] memory_limit={args.memory_limit}", flush=True)
        print(f"[labels] threads={args.threads}", flush=True)
        print(f"[labels] temp_dir={temp_dir_path}", flush=True)
        print(f"[labels] price_date_column={price_date_col}", flush=True)
        print(f"[labels] date_range={train_start} -> {test_end}", flush=True)
        print(f"[labels] embargo_days={embargo_days}", flush=True)
        print(f"[labels] max_abs_return={args.max_abs_return}", flush=True)

        if args.verbose:
            print(
                f"[labels] dataset_probe="
                f'{{"rows": {dataset_rows}, "symbols": {dataset_symbols}, '
                f'"min_as_of_date": "{dataset_min_date}", "max_as_of_date": "{dataset_max_date}"}}',
                flush=True,
            )

        # -------------------------------------------------------------------
        # Nettoyage du dataset cible dans research_labels.
        # -------------------------------------------------------------------
        con.execute(
            """
            DELETE FROM research_labels
            WHERE dataset_id = ?
            """,
            [args.dataset_id],
        )
        print("[labels] previous dataset cleared", flush=True)

        # -------------------------------------------------------------------
        # Étape 1: scope minimal du dataset.
        #
        # On ne garde que les colonnes nécessaires pour le calcul du label.
        # Le DISTINCT protège contre d'éventuels doublons accidentels.
        # -------------------------------------------------------------------
        con.execute("DROP TABLE IF EXISTS tmp_dataset_scope")
        con.execute(
            """
            CREATE TEMP TABLE tmp_dataset_scope AS
            SELECT DISTINCT
                dataset_id,
                snapshot_id,
                symbol,
                as_of_date,
                close AS dataset_close
            FROM research_training_dataset
            WHERE dataset_id = ?
              AND symbol IS NOT NULL
              AND TRIM(symbol) <> ''
              AND as_of_date IS NOT NULL
            """,
            [args.dataset_id],
        )

        # -------------------------------------------------------------------
        # Étape 2: on limite price_history aux seuls symbols du dataset.
        #
        # Pourquoi:
        # - évite de calculer LEAD() pour des milliers de symbols inutiles
        # - réduit fortement la pression mémoire
        # - garde une exécution plus déterministe
        #
        # Important:
        # - on prend tout l'historique disponible de ces symbols jusqu'à la
        #   date max du dataset + prix futurs présents dans la table
        # - on ne coupe PAS au split_end avant les LEAD(), sinon on perdrait
        #   artificiellement certaines forward returns de fin de période
        # -------------------------------------------------------------------
        con.execute("DROP TABLE IF EXISTS tmp_price_symbol_scope")
        con.execute(
            f"""
            CREATE TEMP TABLE tmp_price_symbol_scope AS
            SELECT
                p.symbol,
                p.{price_date_col} AS as_of_date,
                p.close
            FROM price_history p
            INNER JOIN (
                SELECT DISTINCT symbol
                FROM tmp_dataset_scope
            ) ds
                ON ds.symbol = p.symbol
            WHERE p.{price_date_col} IS NOT NULL
              AND p.symbol IS NOT NULL
              AND TRIM(p.symbol) <> ''
            """,
        )

        if args.verbose:
            price_scope_probe = con.execute(
                """
                SELECT
                    COUNT(*) AS rows,
                    COUNT(DISTINCT symbol) AS symbols,
                    MIN(as_of_date) AS min_as_of_date,
                    MAX(as_of_date) AS max_as_of_date
                FROM tmp_price_symbol_scope
                """
            ).fetchone()
            print(
                f"[labels] price_scope="
                f'{{"rows": {int(price_scope_probe[0] or 0)}, '
                f'"symbols": {int(price_scope_probe[1] or 0)}, '
                f'"min_as_of_date": "{price_scope_probe[2]}", '
                f'"max_as_of_date": "{price_scope_probe[3]}"}}',
                flush=True,
            )

        # -------------------------------------------------------------------
        # Étape 3: enrichissement des prix avec les closes futurs.
        #
        # LEAD() calcule les prix futurs sur la série réelle par symbole.
        # Ensuite, on fera un LEFT JOIN sur les lignes exactes du dataset.
        # -------------------------------------------------------------------
        con.execute("DROP TABLE IF EXISTS tmp_price_enriched")
        con.execute(
            """
            CREATE TEMP TABLE tmp_price_enriched AS
            SELECT
                symbol,
                as_of_date,
                close AS close_t,
                LEAD(close, 1) OVER (
                    PARTITION BY symbol
                    ORDER BY as_of_date
                ) AS close_t1,
                LEAD(close, 5) OVER (
                    PARTITION BY symbol
                    ORDER BY as_of_date
                ) AS close_t5,
                LEAD(close, 20) OVER (
                    PARTITION BY symbol
                    ORDER BY as_of_date
                ) AS close_t20
            FROM tmp_price_symbol_scope
            """
        )

        # -------------------------------------------------------------------
        # Étape 4: staging complet avec diagnostics qualité.
        #
        # On calcule d'abord les rendements bruts.
        # Puis on applique le filtre max_abs_return.
        # On garde des flags pour compter précisément ce qui a été filtré.
        # -------------------------------------------------------------------
        con.execute("DROP TABLE IF EXISTS tmp_label_stage")
        con.execute(
            """
            CREATE TEMP TABLE tmp_label_stage AS
            WITH base AS (
                SELECT
                    ds.dataset_id,
                    ds.snapshot_id,
                    ds.symbol,
                    ds.as_of_date,
                    ds.dataset_close,
                    pe.close_t,
                    pe.close_t1,
                    pe.close_t5,
                    pe.close_t20
                FROM tmp_dataset_scope ds
                LEFT JOIN tmp_price_enriched pe
                    ON pe.symbol = ds.symbol
                   AND pe.as_of_date = ds.as_of_date
            ),
            raw_returns AS (
                SELECT
                    dataset_id,
                    snapshot_id,
                    symbol,
                    as_of_date,

                    dataset_close,
                    close_t,
                    close_t1,
                    close_t5,
                    close_t20,

                    CASE
                        WHEN COALESCE(close_t, dataset_close) IS NOT NULL
                         AND COALESCE(close_t, dataset_close) > 0
                         AND close_t1 IS NOT NULL
                         AND close_t1 > 0
                        THEN close_t1 / COALESCE(close_t, dataset_close) - 1
                        ELSE NULL
                    END AS raw_fwd_return_1d,

                    CASE
                        WHEN COALESCE(close_t, dataset_close) IS NOT NULL
                         AND COALESCE(close_t, dataset_close) > 0
                         AND close_t5 IS NOT NULL
                         AND close_t5 > 0
                        THEN close_t5 / COALESCE(close_t, dataset_close) - 1
                        ELSE NULL
                    END AS raw_fwd_return_5d,

                    CASE
                        WHEN COALESCE(close_t, dataset_close) IS NOT NULL
                         AND COALESCE(close_t, dataset_close) > 0
                         AND close_t20 IS NOT NULL
                         AND close_t20 > 0
                        THEN close_t20 / COALESCE(close_t, dataset_close) - 1
                        ELSE NULL
                    END AS raw_fwd_return_20d
                FROM base
            )
            SELECT
                dataset_id,
                snapshot_id,
                symbol,
                as_of_date,

                CASE
                    WHEN raw_fwd_return_1d IS NOT NULL
                     AND ABS(raw_fwd_return_1d) <= ?
                    THEN raw_fwd_return_1d
                    ELSE NULL
                END AS fwd_return_1d,

                CASE
                    WHEN raw_fwd_return_5d IS NOT NULL
                     AND ABS(raw_fwd_return_5d) <= ?
                    THEN raw_fwd_return_5d
                    ELSE NULL
                END AS fwd_return_5d,

                CASE
                    WHEN raw_fwd_return_20d IS NOT NULL
                     AND ABS(raw_fwd_return_20d) <= ?
                    THEN raw_fwd_return_20d
                    ELSE NULL
                END AS fwd_return_20d,

                CASE
                    WHEN COALESCE(close_t, dataset_close) IS NULL
                      OR COALESCE(close_t, dataset_close) <= 0
                    THEN 1 ELSE 0
                END AS bad_close_t,

                CASE
                    WHEN close_t1 IS NULL OR close_t1 <= 0
                    THEN 1 ELSE 0
                END AS bad_close_t1,

                CASE
                    WHEN close_t5 IS NULL OR close_t5 <= 0
                    THEN 1 ELSE 0
                END AS bad_close_t5,

                CASE
                    WHEN close_t20 IS NULL OR close_t20 <= 0
                    THEN 1 ELSE 0
                END AS bad_close_t20,

                CASE
                    WHEN raw_fwd_return_1d IS NOT NULL
                     AND ABS(raw_fwd_return_1d) > ?
                    THEN 1 ELSE 0
                END AS extreme_1d_filtered,

                CASE
                    WHEN raw_fwd_return_5d IS NOT NULL
                     AND ABS(raw_fwd_return_5d) > ?
                    THEN 1 ELSE 0
                END AS extreme_5d_filtered,

                CASE
                    WHEN raw_fwd_return_20d IS NOT NULL
                     AND ABS(raw_fwd_return_20d) > ?
                    THEN 1 ELSE 0
                END AS extreme_20d_filtered
            FROM raw_returns
            """,
            [
                args.max_abs_return,
                args.max_abs_return,
                args.max_abs_return,
                args.max_abs_return,
                args.max_abs_return,
                args.max_abs_return,
            ],
        )

        # -------------------------------------------------------------------
        # Étape 5: insertion finale dans research_labels.
        #
        # On n'insère que les colonnes métiers du schéma cible.
        # -------------------------------------------------------------------
        con.execute(
            """
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
                dataset_id,
                snapshot_id,
                symbol,
                as_of_date,
                fwd_return_1d,
                fwd_return_5d,
                fwd_return_20d
            FROM tmp_label_stage
            """
        )

        # -------------------------------------------------------------------
        # Étape 6: résumé qualité.
        # -------------------------------------------------------------------
        quality_row = con.execute(
            """
            SELECT
                COUNT(*) AS rows,
                COUNT(fwd_return_1d) AS labeled_rows_1d,
                COUNT(fwd_return_5d) AS labeled_rows_5d,
                COUNT(fwd_return_20d) AS labeled_rows_20d,

                SUM(bad_close_t) AS bad_close_t_rows,
                SUM(bad_close_t1) AS bad_close_t1_rows,
                SUM(bad_close_t5) AS bad_close_t5_rows,
                SUM(bad_close_t20) AS bad_close_t20_rows,

                SUM(extreme_1d_filtered) AS extreme_1d_rows_filtered,
                SUM(extreme_5d_filtered) AS extreme_5d_rows_filtered,
                SUM(extreme_20d_filtered) AS extreme_20d_rows_filtered,

                MIN(fwd_return_1d) AS retained_1d_min,
                MAX(fwd_return_1d) AS retained_1d_max,
                AVG(fwd_return_1d) AS retained_1d_avg,
                STDDEV_SAMP(fwd_return_1d) AS retained_1d_std,

                MIN(as_of_date) AS min_as_of_date,
                MAX(as_of_date) AS max_as_of_date
            FROM tmp_label_stage
            """
        ).fetchone()

        quality = {
            "max_abs_return": float(args.max_abs_return),
            "labeled_rows_1d": int(quality_row[1] or 0),
            "labeled_rows_5d": int(quality_row[2] or 0),
            "labeled_rows_20d": int(quality_row[3] or 0),
            "bad_close_t_rows": int(quality_row[4] or 0),
            "bad_close_t1_rows": int(quality_row[5] or 0),
            "bad_close_t5_rows": int(quality_row[6] or 0),
            "bad_close_t20_rows": int(quality_row[7] or 0),
            "extreme_1d_rows_filtered": int(quality_row[8] or 0),
            "extreme_5d_rows_filtered": int(quality_row[9] or 0),
            "extreme_20d_rows_filtered": int(quality_row[10] or 0),
            "retained_1d_min": float(quality_row[11]) if quality_row[11] is not None else None,
            "retained_1d_max": float(quality_row[12]) if quality_row[12] is not None else None,
            "retained_1d_avg": float(quality_row[13]) if quality_row[13] is not None else None,
            "retained_1d_std": float(quality_row[14]) if quality_row[14] is not None else None,
            "min_as_of_date": str(quality_row[15]) if quality_row[15] is not None else None,
            "max_as_of_date": str(quality_row[16]) if quality_row[16] is not None else None,
        }

        print(
            f"[labels] quality_summary={json.dumps(quality, sort_keys=True)}",
            flush=True,
        )

        # -------------------------------------------------------------------
        # Étape 7: payload final.
        # Important:
        # - "rows" doit refléter exactement le dataset_id courant
        # - plus de confusion avec un calcul hors scope
        # -------------------------------------------------------------------
        final_count = con.execute(
            """
            SELECT COUNT(*)
            FROM research_labels
            WHERE dataset_id = ?
            """,
            [args.dataset_id],
        ).fetchone()[0]

        output = {
            "dataset_id": args.dataset_id,
            "snapshot_id": args.snapshot_id,
            "split_id": args.split_id,
            "rows": int(final_count),
            "price_date_column": price_date_col,
            "date_window": {
                "start": str(train_start),
                "end": str(test_end),
            },
            "quality": quality,
        }

        print(json.dumps(output, indent=2), flush=True)
        return 0

    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
