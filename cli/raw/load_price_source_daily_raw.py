#!/usr/bin/env python3
from __future__ import annotations

# =============================================================================
# load_price_source_daily_raw.py
#
# Objectif
# --------
# Construire la table "effective" price_source_daily_raw à partir des tables
# bronze présentes dans la base.
#
# Ce script NE LIT PAS les fichiers Stooq directement.
#
# Les fichiers Stooq sont chargés auparavant dans:
#
#     price_source_daily_raw_all
#
# via:
#
#     load_price_source_daily_raw_all_from_stooq_dir.py
#
# Puis ce script applique les règles de filtrage métier.
#
# Architecture
# ------------
#
# Bronze
# ------
# price_source_daily_raw_all
#
# Effective
# ---------
# price_source_daily_raw
#
# Règles principales
# ------------------
#
# - symbol doit exister dans symbol_reference
# - on garde 1 ligne par (symbol, price_date)
# - priorité à la source Yahoo si multiple sources
#
# Philosophie SQL-first
# ---------------------
#
# Toute la logique est en SQL DuckDB.
# Python ne fait que :
#
# - parser les arguments
# - exécuter la requête
#
# =============================================================================

import argparse

from stock_quant.infrastructure.config.settings_loader import build_app_config
from stock_quant.infrastructure.db.duckdb_session_factory import DuckDbSessionFactory
from stock_quant.infrastructure.db.schema_manager import SchemaManager
from stock_quant.infrastructure.db.unit_of_work import DuckDbUnitOfWork


def parse_args() -> argparse.Namespace:

    parser = argparse.ArgumentParser(
        description="Build price_source_daily_raw from bronze tables."
    )

    parser.add_argument(
        "--db-path",
        default=None,
        help="DuckDB path."
    )

    parser.add_argument(
        "--truncate",
        action="store_true",
        help="Delete existing rows before rebuild."
    )

    parser.add_argument(
        "--verbose",
        action="store_true"
    )

    return parser.parse_args()


def main() -> int:

    args = parse_args()

    config = build_app_config(db_path=args.db_path)
    config.ensure_directories()

    session_factory = DuckDbSessionFactory(config.db_path)

    with DuckDbUnitOfWork(session_factory) as uow:

        schema_manager = SchemaManager(uow)
        schema_manager.validate()

        con = uow.connection

        if args.truncate:

            con.execute(
                """
                DELETE FROM price_source_daily_raw
                """
            )

        con.execute(
            """
            INSERT INTO price_source_daily_raw
            SELECT
                b.symbol,
                b.price_date,
                b.open,
                b.high,
                b.low,
                b.close,
                b.volume,
                b.source_name,
                b.ingested_at
            FROM price_source_daily_raw_all b
            JOIN symbol_reference s
                ON b.symbol = s.symbol
            QUALIFY
                ROW_NUMBER() OVER (
                    PARTITION BY b.symbol, b.price_date
                    ORDER BY
                        CASE
                            WHEN lower(b.source_name) LIKE '%yahoo%' THEN 0
                            ELSE 1
                        END,
                        b.ingested_at DESC
                ) = 1
            """
        )

        total = con.execute(
            "SELECT COUNT(*) FROM price_source_daily_raw"
        ).fetchone()[0]

    if args.verbose:
        print(f"price_source_daily_raw rows={total}")

    print(
        f"price_source_daily_raw rebuilt rows={total}"
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

