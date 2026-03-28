#!/usr/bin/env python3
from __future__ import annotations

"""
rebuild_price_source_daily_raw_effective.py

Objectif
--------
Reconstruire `price_source_daily_raw` à partir des couches raw disponibles :

- `price_source_daily_raw_all`   (historique Stooq bulk)
- `price_source_daily_raw_yahoo` (raw Yahoo/yfinance chargé depuis disque)

Philosophie
-----------
Ce script applique une logique raw-first stricte :

1. Stooq et Yahoo existent comme couches raw distinctes.
2. `price_source_daily_raw` est la couche raw effective fusionnée.
3. `price_history` doit ensuite être reconstruit depuis `price_source_daily_raw`.
4. Ce script ne doit pas écrire dans `price_history` ni `price_latest`.

Décision de design
------------------
Par défaut, cette reconstruction se fait SANS filtrage univers.

Pourquoi ?
----------
Le filtrage par `market_universe` est une logique métier/research.
La couche raw effective doit rester la plus générale possible.

Le filtrage univers reste possible via un flag optionnel :
    --filter-include-in-universe

Priorité des sources
--------------------
Pour une même clé logique `(symbol, price_date)` :

- Yahoo/yfinance raw a priorité sur Stooq
- si plusieurs lignes existent dans une même source, la plus récente en timestamp gagne

Sortie
------
La table `price_source_daily_raw` est reconstruite entièrement à partir des raw.

Notes
-----
- On garde des commentaires abondants pour aider les autres développeurs.
- On corrige aussi la métrique Yahoo : la source s'appelle désormais
  `yfinance_raw_disk`, donc il ne faut plus compter seulement `LIKE 'yahoo%'`.
"""

import argparse
import json

from stock_quant.infrastructure.config.settings_loader import build_app_config
from stock_quant.infrastructure.db.duckdb_session_factory import DuckDbSessionFactory
from stock_quant.infrastructure.db.schema_manager import SchemaManager
from stock_quant.infrastructure.db.table_names import (
    MARKET_UNIVERSE,
    PRICE_SOURCE_DAILY_RAW,
    PRICE_SOURCE_DAILY_RAW_ALL,
    PRICE_SOURCE_DAILY_RAW_YAHOO,
)
from stock_quant.infrastructure.db.unit_of_work import DuckDbUnitOfWork


def parse_args() -> argparse.Namespace:
    """
    Parse les arguments CLI.

    Flags importants
    ----------------
    --truncate
        Vide explicitement la table cible avant le rebuild.

    --filter-include-in-universe
        Applique un INNER JOIN sur market_universe avec include_in_universe = TRUE.
        Ce filtre est volontairement OPTIONNEL.

    --verbose
        Affiche des métriques détaillées.
    """
    parser = argparse.ArgumentParser(
        description=(
            "Rebuild effective price_source_daily_raw from Stooq raw + Yahoo raw, "
            "with Yahoo priority and optional universe filtering."
        )
    )
    parser.add_argument(
        "--db-path",
        default=None,
        help="Path to DuckDB database file.",
    )
    parser.add_argument(
        "--truncate",
        action="store_true",
        help="Delete existing effective rows before rebuild.",
    )
    parser.add_argument(
        "--filter-include-in-universe",
        action="store_true",
        help="If enabled, restrict rebuild to market_universe.include_in_universe = TRUE.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose output.",
    )
    return parser.parse_args()


def main() -> int:
    """
    Pipeline principal.

    Étapes :
    1. validation schéma
    2. probes de comptage
    3. rebuild effectif fusionné
    4. métriques finales
    """
    args = parse_args()
    config = build_app_config(db_path=args.db_path)
    config.ensure_directories()

    session_factory = DuckDbSessionFactory(config.db_path)

    with DuckDbUnitOfWork(session_factory) as uow:
        schema_manager = SchemaManager(uow)
        schema_manager.validate()

        con = uow.connection
        if con is None:
            raise RuntimeError("missing active connection")

        # --------------------------------------------------------------------
        # Probes amont
        # --------------------------------------------------------------------
        bronze_rows = int(
            con.execute(
                f"SELECT COUNT(*) FROM {PRICE_SOURCE_DAILY_RAW_ALL}"
            ).fetchone()[0]
        )

        yahoo_rows = int(
            con.execute(
                f"SELECT COUNT(*) FROM {PRICE_SOURCE_DAILY_RAW_YAHOO}"
            ).fetchone()[0]
        )

        # Le probe univers reste utile, même si le filtrage est optionnel.
        included_symbols = None
        if args.filter_include_in_universe:
            included_symbols = int(
                con.execute(
                    f"SELECT COUNT(*) FROM {MARKET_UNIVERSE} WHERE include_in_universe = TRUE"
                ).fetchone()[0]
            )

        # --------------------------------------------------------------------
        # Gestion de la table cible
        # --------------------------------------------------------------------
        if args.truncate:
            con.execute(f"DELETE FROM {PRICE_SOURCE_DAILY_RAW}")

        # --------------------------------------------------------------------
        # Construction SQL-first du rebuild effectif
        # --------------------------------------------------------------------
        #
        # Idée :
        # - on normalise Stooq
        # - on normalise Yahoo
        # - on unionne
        # - on ranke par (symbol, price_date)
        # - Yahoo > Stooq
        # - timestamp le plus récent gagne
        #
        # Important :
        # - Yahoo raw utilise fetched_at
        # - Stooq raw utilise ingested_at
        #
        # On expose ensuite un timestamp unifié `effective_ts`.
        # --------------------------------------------------------------------
        if args.filter_include_in_universe:
            stooq_join_sql = f"""
                INNER JOIN {MARKET_UNIVERSE} mu
                    ON a.symbol = mu.symbol
                WHERE mu.include_in_universe = TRUE
            """
            yahoo_join_sql = f"""
                INNER JOIN {MARKET_UNIVERSE} mu
                    ON y.symbol = mu.symbol
                WHERE mu.include_in_universe = TRUE
            """
            filter_mode = "include_in_universe_only"
        else:
            stooq_join_sql = ""
            yahoo_join_sql = ""
            filter_mode = "all_symbols"

        # On reconstruit proprement la table cible.
        con.execute(
            f"""
            CREATE OR REPLACE TEMP TABLE tmp_price_source_daily_raw_effective AS
            WITH stooq_filtered AS (
                SELECT
                    UPPER(TRIM(CAST(a.symbol AS VARCHAR))) AS symbol,
                    CAST(a.price_date AS DATE) AS price_date,
                    CAST(a.open AS DOUBLE) AS open,
                    CAST(a.high AS DOUBLE) AS high,
                    CAST(a.low AS DOUBLE) AS low,
                    CAST(a.close AS DOUBLE) AS close,
                    CAST(a.volume AS BIGINT) AS volume,
                    CAST(a.source_name AS VARCHAR) AS source_name,
                    CAST(a.ingested_at AS TIMESTAMP) AS effective_ts,
                    1 AS source_priority,
                    COALESCE(CAST(a.source_path AS VARCHAR), 'stooq') AS source_tiebreak
                FROM {PRICE_SOURCE_DAILY_RAW_ALL} a
                {stooq_join_sql}
            ),
            yahoo_filtered AS (
                SELECT
                    UPPER(TRIM(CAST(y.symbol AS VARCHAR))) AS symbol,
                    CAST(y.price_date AS DATE) AS price_date,
                    CAST(y.open AS DOUBLE) AS open,
                    CAST(y.high AS DOUBLE) AS high,
                    CAST(y.low AS DOUBLE) AS low,
                    CAST(y.close AS DOUBLE) AS close,
                    CAST(y.volume AS BIGINT) AS volume,
                    COALESCE(CAST(y.source_name AS VARCHAR), 'yfinance_raw_disk') AS source_name,
                    CAST(y.fetched_at AS TIMESTAMP) AS effective_ts,
                    2 AS source_priority,
                    'yfinance_raw_disk' AS source_tiebreak
                FROM {PRICE_SOURCE_DAILY_RAW_YAHOO} y
                {yahoo_join_sql}
            ),
            unioned AS (
                SELECT * FROM stooq_filtered
                UNION ALL
                SELECT * FROM yahoo_filtered
            ),
            ranked AS (
                SELECT
                    symbol,
                    price_date,
                    open,
                    high,
                    low,
                    close,
                    volume,
                    source_name,
                    effective_ts,
                    ROW_NUMBER() OVER (
                        PARTITION BY symbol, price_date
                        ORDER BY
                            source_priority DESC,
                            effective_ts DESC NULLS LAST,
                            source_tiebreak DESC
                    ) AS rn
                FROM unioned
                WHERE symbol IS NOT NULL
                  AND price_date IS NOT NULL
                  AND close IS NOT NULL
            )
            SELECT
                symbol,
                price_date,
                open,
                high,
                low,
                close,
                close AS adj_close,
                volume,
                source_name,
                effective_ts AS ingested_at
            FROM ranked
            WHERE rn = 1
            """
        )

        # --------------------------------------------------------------------
        # Recharge complète de la table cible.
        # --------------------------------------------------------------------
        con.execute(f"DELETE FROM {PRICE_SOURCE_DAILY_RAW}")

        con.execute(
            f"""
            INSERT INTO {PRICE_SOURCE_DAILY_RAW} (
                symbol,
                price_date,
                open,
                high,
                low,
                close,
                adj_close,
                volume,
                source_name,
                ingested_at
            )
            SELECT
                symbol,
                price_date,
                open,
                high,
                low,
                close,
                adj_close,
                volume,
                source_name,
                ingested_at
            FROM tmp_price_source_daily_raw_effective
            """
        )

        # --------------------------------------------------------------------
        # Métriques aval
        # --------------------------------------------------------------------
        effective_rows = int(
            con.execute(
                f"SELECT COUNT(*) FROM {PRICE_SOURCE_DAILY_RAW}"
            ).fetchone()[0]
        )

        effective_symbols = int(
            con.execute(
                f"SELECT COUNT(DISTINCT symbol) FROM {PRICE_SOURCE_DAILY_RAW}"
            ).fetchone()[0]
        )

        min_date, max_date = con.execute(
            f"""
            SELECT MIN(price_date), MAX(price_date)
            FROM {PRICE_SOURCE_DAILY_RAW}
            """
        ).fetchone()

        # Compteur Yahoo corrigé :
        # on ne dépend plus d'un LIKE 'yahoo%'.
        yahoo_selected_rows = int(
            con.execute(
                f"""
                SELECT COUNT(*)
                FROM {PRICE_SOURCE_DAILY_RAW}
                WHERE LOWER(COALESCE(source_name, '')) LIKE '%yahoo%'
                   OR LOWER(COALESCE(source_name, '')) LIKE '%yfinance%'
                """
            ).fetchone()[0]
        )

        stooq_selected_rows = int(
            con.execute(
                f"""
                SELECT COUNT(*)
                FROM {PRICE_SOURCE_DAILY_RAW}
                WHERE LOWER(COALESCE(source_name, '')) LIKE '%stooq%'
                """
            ).fetchone()[0]
        )

        source_breakdown = con.execute(
            f"""
            SELECT source_name, COUNT(*) AS rows_count
            FROM {PRICE_SOURCE_DAILY_RAW}
            GROUP BY 1
            ORDER BY rows_count DESC, source_name
            """
        ).fetchall()

    # ------------------------------------------------------------------------
    # Logs / sortie terminal
    # ------------------------------------------------------------------------
    if args.verbose:
        print(f"[rebuild_price_source_daily_raw_effective] db_path={config.db_path}")
        print(f"[rebuild_price_source_daily_raw_effective] bronze_rows={bronze_rows}")
        print(f"[rebuild_price_source_daily_raw_effective] yahoo_rows={yahoo_rows}")
        print(f"[rebuild_price_source_daily_raw_effective] filter_mode={filter_mode}")
        print(f"[rebuild_price_source_daily_raw_effective] included_symbols={included_symbols}")
        print(f"[rebuild_price_source_daily_raw_effective] effective_rows={effective_rows}")
        print(f"[rebuild_price_source_daily_raw_effective] effective_symbols={effective_symbols}")
        print(f"[rebuild_price_source_daily_raw_effective] yahoo_selected_rows={yahoo_selected_rows}")
        print(f"[rebuild_price_source_daily_raw_effective] stooq_selected_rows={stooq_selected_rows}")
        print(
            "[rebuild_price_source_daily_raw_effective] source_breakdown="
            + json.dumps(source_breakdown, ensure_ascii=False)
        )

    print(
        json.dumps(
            {
                "db_path": str(config.db_path),
                "bronze_rows": bronze_rows,
                "yahoo_rows": yahoo_rows,
                "filter_mode": filter_mode,
                "included_symbols": included_symbols,
                "effective_rows": effective_rows,
                "effective_symbols": effective_symbols,
                "min_price_date": str(min_date) if min_date is not None else None,
                "max_price_date": str(max_date) if max_date is not None else None,
                "yahoo_selected_rows": yahoo_selected_rows,
                "stooq_selected_rows": stooq_selected_rows,
                "source_breakdown": source_breakdown,
                "mode": "raw_first_effective_merge",
            },
            indent=2,
        )
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
