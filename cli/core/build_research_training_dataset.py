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
- Option A appliquée à la source:
  on exclut les symboles manifestement spéciaux / non-common
  (warrants, rights, units, test symbols)

Pourquoi filtrer ici
--------------------
Le probe a montré que:
- le dataset est fabriqué depuis tmp_price_chunk
- le join short est fait via tmp_short_chunk
- aucun filtre W/R/U n'était appliqué à la source

Donc le meilleur endroit pour corriger est:
1) au chargement du chunk prix
2) au chargement du chunk short
et non pas seulement plus tard dans les labels ou le backtest.

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
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import duckdb
from dateutil.relativedelta import relativedelta
from tqdm import tqdm

from stock_quant.infrastructure.db.research_training_dataset_schema import (
    ResearchTrainingDatasetSchemaManager,
)


def _now_utc() -> datetime:
    """Retourne un timestamp UTC timezone-aware."""
    return datetime.now(timezone.utc)


def _dataset_id(snapshot_id: str, split_id: str) -> str:
    """Construit un dataset_id reproductible et horodaté."""
    stamp = _now_utc().strftime("%Y%m%dT%H%M%SZ")
    return f"{snapshot_id}_{split_id}_dataset_{stamp}"


def _table_exists(con: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    """Sonde l'existence d'une table sans rien supposer."""
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
    Détecte la colonne date d'une table de prix.

    Pourquoi:
    - éviter d'assumer un schéma rigide
    - rester compatible avec l'existant
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


def _sql_symbol_type_case(symbol_expr: str) -> str:
    """
    Heuristique SQL de typage de symbole.

    Catégories:
    - TEST_OR_SPECIAL  : symboles de test / placeholder connus
    - LIKELY_WARRANT   : suffixe W
    - LIKELY_RIGHT     : suffixe R
    - LIKELY_UNIT      : suffixe U
    - COMMON_OR_OTHER  : tout le reste

    Important:
    - heuristique volontairement simple, stable et explicite
    - on ne fait pas de magie opaque
    - SQL-first: la logique vit dans le SQL, pas dans des boucles Python
    """
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
    """
    Construit le prédicat SQL de filtre symbole.

    Modes:
    - all: aucun filtre
    - common_only: ne garde que COMMON_OR_OTHER
    """
    if symbol_filter_mode == "all":
        return "TRUE"

    if symbol_filter_mode == "common_only":
        return f"({_sql_symbol_type_case(symbol_expr)}) = 'COMMON_OR_OTHER'"

    raise ValueError(f"unsupported symbol_filter_mode={symbol_filter_mode}")


def parse_args() -> argparse.Namespace:
    """
    Parse les arguments CLI.

    Notes:
    - --symbol-filter-mode=common_only = Option A activée par défaut
    - on garde un opt-out explicite avec --symbol-filter-mode all
    """
    p = argparse.ArgumentParser()
    p.add_argument("--db-path", required=True)
    p.add_argument("--snapshot-id", required=True)
    p.add_argument("--split-id", required=True)
    p.add_argument("--dataset-id", default=None)
    p.add_argument("--memory-limit", default="24GB")
    p.add_argument("--threads", type=int, default=6)
    p.add_argument("--temp-dir", default="/home/marty/stock-quant-oop/tmp")
    p.add_argument(
        "--symbol-filter-mode",
        default="common_only",
        choices=["all", "common_only"],
        help="Filter symbol universe at dataset-build time.",
    )
    p.add_argument("--verbose", action="store_true")
    return p.parse_args()


def _table_stats_for_dataset(
    con: duckdb.DuckDBPyConnection,
    dataset_id: str,
) -> tuple[int, int, int]:
    """
    Retourne:
    - rows
    - signal_rows
    - symbols
    """
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


def _tmp_chunk_symbol_mix(
    con: duckdb.DuckDBPyConnection,
    temp_table_name: str,
) -> list[tuple[str, int, int]]:
    """
    Probe de composition des symboles dans une table temporaire.

    Retour:
    [(symbol_type, distinct_symbols, row_count), ...]

    Pourquoi:
    - observer le filtre appliqué
    - garder une visibilité scientifique / auditabilité
    """
    return con.execute(
        f"""
        SELECT
            symbol_type,
            COUNT(DISTINCT symbol) AS distinct_symbols,
            COUNT(*) AS row_count
        FROM {temp_table_name}
        GROUP BY symbol_type
        ORDER BY row_count DESC, symbol_type
        """
    ).fetchall()


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

        temp_dir_path = Path(args.temp_dir).expanduser().resolve()
        temp_dir_path.mkdir(parents=True, exist_ok=True)
        temp_dir_sql = str(temp_dir_path).replace("'", "''")
        con.execute(f"PRAGMA temp_directory='{temp_dir_sql}'")

        # ------------------------------------------------------------------
        # Validation / préparation du schéma cible.
        # ------------------------------------------------------------------
        ResearchTrainingDatasetSchemaManager(con).ensure_tables()

        # ------------------------------------------------------------------
        # Snapshot manifest.
        # ------------------------------------------------------------------
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

        # ------------------------------------------------------------------
        # Split manifest.
        # ------------------------------------------------------------------
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

        # ------------------------------------------------------------------
        # Détection schéma source.
        # ------------------------------------------------------------------
        price_date_col = _choose_price_date_column(con, "price_history")
        has_short_features = _table_exists(con, "short_features_daily")

        # ------------------------------------------------------------------
        # Identifiant dataset.
        # ------------------------------------------------------------------
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
        print(f"[builder] symbol_filter_mode={args.symbol_filter_mode}", flush=True)
        print(f"[builder] verbose={args.verbose}", flush=True)

        # ------------------------------------------------------------------
        # Nettoyage du dataset cible.
        # ------------------------------------------------------------------
        con.execute(
            """
            DELETE FROM research_training_dataset
            WHERE dataset_id = ?
            """,
            [dataset_id],
        )

        # ------------------------------------------------------------------
        # Fenêtres train / valid / test.
        # ------------------------------------------------------------------
        partitions = [
            ("train", train_start, train_end),
            ("valid", valid_start, valid_end),
            ("test", test_start, test_end),
        ]

        allowed_symbol_predicate_price = _sql_allowed_symbol_predicate(
            "symbol",
            args.symbol_filter_mode,
        )
        allowed_symbol_predicate_short = _sql_allowed_symbol_predicate(
            "symbol",
            args.symbol_filter_mode,
        )

        # ------------------------------------------------------------------
        # Construction chunkée par mois.
        # ------------------------------------------------------------------
        for partition_name, partition_start, partition_end in partitions:
            if partition_start is None or partition_end is None:
                print(
                    f"[partition_skip] {partition_name} reason=missing_dates",
                    flush=True,
                )
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

                # ----------------------------------------------------------
                # Prix bornés à la fenêtre du chunk + filtre symboles.
                #
                # On applique Option A ici:
                # - exclusion des warrants / rights / units / test symbols
                # - cela réduit le dataset source avant tout join aval
                # ----------------------------------------------------------
                con.execute("DROP TABLE IF EXISTS tmp_price_chunk")
                con.execute(
                    f"""
                    CREATE TEMP TABLE tmp_price_chunk AS
                    SELECT
                        UPPER(TRIM(symbol)) AS symbol,
                        {price_date_col} AS as_of_date,
                        close,
                        {_sql_symbol_type_case("symbol")} AS symbol_type
                    FROM price_history
                    WHERE {price_date_col} BETWEEN ? AND ?
                      AND {price_date_col} IS NOT NULL
                      AND symbol IS NOT NULL
                      AND TRIM(symbol) <> ''
                      AND {allowed_symbol_predicate_price}
                    """,
                    [month_start, month_end],
                )

                price_rows = con.execute(
                    "SELECT COUNT(*) FROM tmp_price_chunk"
                ).fetchone()[0]

                if args.verbose:
                    mix_rows = _tmp_chunk_symbol_mix(con, "tmp_price_chunk")
                    print(
                        f"[price_chunk_mix] partition={partition_name} start={month_start} end={month_end} mix={mix_rows}",
                        flush=True,
                    )

                if price_rows == 0:
                    print(
                        f"[chunk_done] partition={partition_name} start={month_start} end={month_end} inserted_rows=0 reason=no_price_rows",
                        flush=True,
                    )
                    continue

                if has_short_features:
                    # ------------------------------------------------------
                    # Features bornées à ce qui est nécessaire pour ce chunk.
                    #
                    # Important:
                    # - on ne garde que les lignes <= month_end (PIT-safe)
                    # - on applique le même filtre symbole que côté prix
                    # - SQL-first, pas de boucles Python sur les lignes
                    # ------------------------------------------------------
                    con.execute("DROP TABLE IF EXISTS tmp_short_chunk")
                    con.execute(
                        f"""
                        CREATE TEMP TABLE tmp_short_chunk AS
                        SELECT
                            UPPER(TRIM(symbol)) AS symbol,
                            as_of_date,
                            short_volume_ratio,
                            {_sql_symbol_type_case("symbol")} AS symbol_type
                        FROM short_features_daily
                        WHERE as_of_date <= ?
                          AND as_of_date IS NOT NULL
                          AND symbol IS NOT NULL
                          AND TRIM(symbol) <> ''
                          AND {allowed_symbol_predicate_short}
                        """,
                        [month_end],
                    )

                    if args.verbose:
                        mix_rows = _tmp_chunk_symbol_mix(con, "tmp_short_chunk")
                        print(
                            f"[short_chunk_mix] partition={partition_name} start={month_start} end={month_end} mix={mix_rows}",
                            flush=True,
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
                    con,
                    dataset_id,
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

        # ------------------------------------------------------------------
        # Résumé final.
        # ------------------------------------------------------------------
        row_count = con.execute(
            """
            SELECT COUNT(*)
            FROM research_training_dataset
            WHERE dataset_id = ?
            """,
            [dataset_id],
        ).fetchone()[0]

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
            "row_count": int(row_count),
            "used_short_features_daily": has_short_features,
            "price_date_column": price_date_col,
            "join_mode": "asof_backward_chunked" if has_short_features else "prices_only_chunked",
            "date_window": {
                "start": str(train_start),
                "end": str(test_end),
            },
            "symbol_filter_mode": args.symbol_filter_mode,
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
