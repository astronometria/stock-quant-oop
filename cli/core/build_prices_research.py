#!/usr/bin/env python3

from __future__ import annotations

# =============================================================================
# Build research-grade price tables
#
# Réparation ciblée du repo:
# - le CLI original importait BuildPricesResearchPipeline, qui n'existe plus
# - plusieurs scripts downstream attendent pourtant price_bars_unadjusted,
#   price_bars_adjusted et price_quality_flags
#
# Cette version répare le chemin "foundation -> adjusted bars" de façon SQL-first.
#
# Principes:
# - source canonique préférée: price_history
# - fallback acceptable: price_source_daily_raw
# - phase 1 d'ajustement: adj_* = raw_* ; adjustment_factor = 1.0
# - génération de flags qualité simples pour aider l'audit downstream
#
# On évite volontairement de charger 27M+ lignes en pandas.
# Toute la construction reste dans DuckDB pour rester mince côté Python.
# =============================================================================

import argparse
import json
from pathlib import Path

from tqdm import tqdm

from stock_quant.infrastructure.config.settings_loader import build_app_config
from stock_quant.infrastructure.db.duckdb_session_factory import DuckDbSessionFactory
from stock_quant.infrastructure.db.master_data_schema import MasterDataSchemaManager
from stock_quant.infrastructure.db.prices_research_schema import PricesResearchSchemaManager
from stock_quant.infrastructure.db.schema_manager import SchemaManager
from stock_quant.infrastructure.db.unit_of_work import DuckDbUnitOfWork


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build research-grade price tables from canonical market data.")
    parser.add_argument("--db-path", default=None, help="Path to DuckDB database file.")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose output.")
    parser.add_argument(
        "--prefer-source-table",
        default="price_history",
        choices=["price_history", "price_source_daily_raw"],
        help="Preferred source table when multiple valid sources exist.",
    )
    return parser.parse_args()


def table_exists(con, table_name: str) -> bool:
    row = con.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_name = ?
        """
        ,
        [table_name],
    ).fetchone()
    return bool(row and row[0] > 0)


def table_row_count(con, table_name: str) -> int:
    return int(con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0])


def choose_source_table(con, preferred: str) -> str:
    """
    Choisit la meilleure table source disponible.

    Politique:
    - si la table préférée existe et contient des lignes, on la prend
    - sinon on prend l'autre source si elle existe et contient des lignes
    - sinon on échoue clairement
    """
    candidates = [preferred] + [t for t in ["price_history", "price_source_daily_raw"] if t != preferred]

    for table_name in candidates:
        if table_exists(con, table_name):
            count = table_row_count(con, table_name)
            if count > 0:
                return table_name

    raise RuntimeError(
        "No usable source price table found. Expected one of: price_history, price_source_daily_raw"
    )


def source_select_sql(source_table: str) -> str:
    """
    Retourne le SELECT normalisé vers le contrat interne attendu.

    On unifie les noms de colonnes entre:
    - price_history: price_date
    - price_source_daily_raw: price_date également dans ce repo
    """
    return f"""
    SELECT
        UPPER(TRIM(symbol)) AS symbol,
        CAST(price_date AS DATE) AS bar_date,
        CAST(open AS DOUBLE) AS open,
        CAST(high AS DOUBLE) AS high,
        CAST(low AS DOUBLE) AS low,
        CAST(close AS DOUBLE) AS close,
        CAST(volume AS BIGINT) AS volume,
        CAST(source_name AS VARCHAR) AS source_name
    FROM {source_table}
    WHERE
        symbol IS NOT NULL
        AND TRIM(symbol) <> ''
        AND price_date IS NOT NULL
        AND open IS NOT NULL
        AND high IS NOT NULL
        AND low IS NOT NULL
        AND close IS NOT NULL
        AND volume IS NOT NULL
    """


def build_price_tables(con, source_table: str) -> dict:
    """
    Construit:
    - price_bars_unadjusted
    - price_bars_adjusted
    - price_quality_flags

    Le tout en SQL-first, directement dans DuckDB.
    """

    # -------------------------------------------------------------------------
    # Étape A: base normalisée temporaire
    # -------------------------------------------------------------------------
    con.execute("DROP TABLE IF EXISTS tmp_prices_research_base")
    con.execute(
        f"""
        CREATE TEMP TABLE tmp_prices_research_base AS
        WITH source_rows AS (
            {source_select_sql(source_table)}
        )
        SELECT
            COALESCE(im.instrument_id, s.symbol) AS instrument_id,
            s.symbol,
            s.bar_date,
            s.open,
            s.high,
            s.low,
            s.close,
            s.volume,
            s.source_name,
            CURRENT_TIMESTAMP AS created_at
        FROM source_rows s
        LEFT JOIN instrument_master im
            ON UPPER(TRIM(im.symbol)) = s.symbol
        """
    )

    # -------------------------------------------------------------------------
    # Étape B: barres non ajustées
    # -------------------------------------------------------------------------
    con.execute("DELETE FROM price_bars_unadjusted")
    con.execute(
        """
        INSERT INTO price_bars_unadjusted (
            instrument_id,
            symbol,
            bar_date,
            open,
            high,
            low,
            close,
            volume,
            source_name,
            created_at
        )
        SELECT
            instrument_id,
            symbol,
            bar_date,
            open,
            high,
            low,
            close,
            volume,
            source_name,
            created_at
        FROM tmp_prices_research_base
        ORDER BY symbol, bar_date
        """
    )

    # -------------------------------------------------------------------------
    # Étape C: barres ajustées
    #
    # Phase 1 volontairement simple et alignée avec le comportement historique
    # repéré dans le repo:
    # - adj_open  = open
    # - adj_high  = high
    # - adj_low   = low
    # - adj_close = close
    # - adjustment_factor = 1.0
    #
    # Cela remet en route le pipeline sans prétendre résoudre encore la
    # totalité des corporate actions.
    # -------------------------------------------------------------------------
    con.execute("DELETE FROM price_bars_adjusted")
    con.execute(
        """
        INSERT INTO price_bars_adjusted (
            instrument_id,
            symbol,
            bar_date,
            adj_open,
            adj_high,
            adj_low,
            adj_close,
            volume,
            adjustment_factor,
            source_name,
            created_at
        )
        SELECT
            instrument_id,
            symbol,
            bar_date,
            open AS adj_open,
            high AS adj_high,
            low  AS adj_low,
            close AS adj_close,
            volume,
            1.0 AS adjustment_factor,
            source_name,
            created_at
        FROM tmp_prices_research_base
        ORDER BY symbol, bar_date
        """
    )

    # -------------------------------------------------------------------------
    # Étape D: flags qualité simples
    #
    # Ces flags sont volontairement conservateurs et auditables.
    # On marque notamment:
    # - close <= 0
    # - volume < 0
    # - retour brut 1j > 80% en absolu
    #
    # Cela ne bloque pas forcément tout downstream, mais donne une couche
    # d'observabilité à la qualité des données.
    # -------------------------------------------------------------------------
    con.execute("DELETE FROM price_quality_flags")
    con.execute(
        """
        INSERT INTO price_quality_flags (
            instrument_id,
            price_date,
            flag_type,
            flag_value,
            source_name,
            created_at
        )
        WITH ordered AS (
            SELECT
                instrument_id,
                symbol,
                bar_date,
                close,
                volume,
                source_name,
                LAG(close, 1) OVER (
                    PARTITION BY symbol
                    ORDER BY bar_date
                ) AS prev_close
            FROM tmp_prices_research_base
        ),
        flags AS (
            SELECT
                instrument_id,
                bar_date AS price_date,
                'close_nonpositive' AS flag_type,
                CAST(close AS DOUBLE) AS flag_value,
                source_name,
                CURRENT_TIMESTAMP AS created_at
            FROM ordered
            WHERE close <= 0

            UNION ALL

            SELECT
                instrument_id,
                bar_date AS price_date,
                'volume_negative' AS flag_type,
                CAST(volume AS DOUBLE) AS flag_value,
                source_name,
                CURRENT_TIMESTAMP AS created_at
            FROM ordered
            WHERE volume < 0

            UNION ALL

            SELECT
                instrument_id,
                bar_date AS price_date,
                'raw_return_abs_gt_80pct' AS flag_type,
                ABS((close / NULLIF(prev_close, 0)) - 1.0) AS flag_value,
                source_name,
                CURRENT_TIMESTAMP AS created_at
            FROM ordered
            WHERE
                prev_close IS NOT NULL
                AND prev_close > 0
                AND ABS((close / NULLIF(prev_close, 0)) - 1.0) > 0.80
        )
        SELECT
            instrument_id,
            price_date,
            flag_type,
            flag_value,
            source_name,
            created_at
        FROM flags
        """
    )

    # -------------------------------------------------------------------------
    # Étape E: résumé
    # -------------------------------------------------------------------------
    summary_row = con.execute(
        """
        SELECT
            (SELECT COUNT(*) FROM price_bars_unadjusted) AS price_bars_unadjusted_rows,
            (SELECT COUNT(*) FROM price_bars_adjusted) AS price_bars_adjusted_rows,
            (SELECT COUNT(*) FROM price_quality_flags) AS price_quality_flags_rows,
            (SELECT COUNT(DISTINCT symbol) FROM price_bars_adjusted) AS adjusted_symbol_count,
            (SELECT MIN(bar_date) FROM price_bars_adjusted) AS min_bar_date,
            (SELECT MAX(bar_date) FROM price_bars_adjusted) AS max_bar_date
        """
    ).fetchone()

    return {
        "source_table": source_table,
        "price_bars_unadjusted_rows": int(summary_row[0]),
        "price_bars_adjusted_rows": int(summary_row[1]),
        "price_quality_flags_rows": int(summary_row[2]),
        "adjusted_symbol_count": int(summary_row[3]),
        "min_bar_date": str(summary_row[4]) if summary_row[4] is not None else None,
        "max_bar_date": str(summary_row[5]) if summary_row[5] is not None else None,
    }


def main() -> int:
    args = parse_args()

    config = build_app_config(db_path=args.db_path)
    config.ensure_directories()

    if args.verbose:
        print(f"[build_prices_research] project_root={config.project_root}")
        print(f"[build_prices_research] db_path={config.db_path}")

    steps = tqdm(total=5, desc="build_prices_research", unit="step")

    session_factory = DuckDbSessionFactory(config.db_path)
    with DuckDbUnitOfWork(session_factory) as uow:
        con = uow.connection
        if con is None:
            raise RuntimeError("Active DuckDB connection is required")

        steps.set_description("build_prices_research:init_schema")
        SchemaManager(uow).validate()
        MasterDataSchemaManager(uow).initialize()
        PricesResearchSchemaManager(uow).initialize()
        steps.update(1)

        steps.set_description("build_prices_research:choose_source")
        source_table = choose_source_table(con, args.prefer_source_table)
        source_rows = table_row_count(con, source_table)
        steps.update(1)

        steps.set_description("build_prices_research:build_tables")
        build_summary = build_price_tables(con, source_table=source_table)
        steps.update(1)

        steps.set_description("build_prices_research:post_checks")
        post_checks = {
            "source_rows": source_rows,
            "unadjusted_rows": table_row_count(con, "price_bars_unadjusted"),
            "adjusted_rows": table_row_count(con, "price_bars_adjusted"),
            "quality_flag_rows": table_row_count(con, "price_quality_flags"),
        }
        steps.update(1)

        steps.set_description("build_prices_research:done")
        result = {
            "status": "SUCCESS",
            "db_path": str(config.db_path),
            "source_table": source_table,
            "summary": build_summary,
            "post_checks": post_checks,
        }
        print(json.dumps(result, indent=2, sort_keys=True))
        steps.update(1)

    steps.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
