#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json

from stock_quant.app.services.market_universe_history_service import (
    MarketUniverseHistoryService,
)
from stock_quant.infrastructure.config.settings_loader import build_app_config
from stock_quant.infrastructure.db.duckdb_session_factory import DuckDbSessionFactory
from stock_quant.infrastructure.db.listing_history_schema import ListingHistorySchemaManager
from stock_quant.infrastructure.db.unit_of_work import DuckDbUnitOfWork
from stock_quant.infrastructure.repositories.duckdb_listing_history_repository import (
    DuckDbListingHistoryRepository,
)
from stock_quant.infrastructure.repositories.duckdb_market_universe_history_repository import (
    DuckDbMarketUniverseHistoryRepository,
)
from stock_quant.pipelines.market_universe_history_pipeline import (
    BuildMarketUniverseHistoryPipeline,
)


def parse_args() -> argparse.Namespace:
    """
    Parse les arguments CLI.

    Notes:
    - include_adr : permet d'inclure les ADR/ADS dans l'univers si désiré
    - allowed_exchanges : liste d'exchanges éligibles
    """
    parser = argparse.ArgumentParser(
        description="Build market_universe_history from active listing history."
    )
    parser.add_argument(
        "--db-path",
        default=None,
        help="Path to DuckDB database file.",
    )
    parser.add_argument(
        "--include-adr",
        action="store_true",
        help="Include ADR/ADS in the eligible universe.",
    )
    parser.add_argument(
        "--allowed-exchanges",
        nargs="+",
        default=["NASDAQ", "NYSE", "AMEX"],
        help="Allowed exchanges for inclusion in the market universe.",
    )
    parser.add_argument(
        "--rule-version",
        default="v1",
        help="Rule version label stored in market_universe_history.",
    )
    return parser.parse_args()


def main() -> int:
    """
    Point d'entrée principal.
    """
    args = parse_args()

    config = build_app_config(db_path=args.db_path)
    config.ensure_directories()

    session_factory = DuckDbSessionFactory(config.db_path)

    with DuckDbUnitOfWork(session_factory) as uow:
        ListingHistorySchemaManager(uow).initialize()

        listing_repository = DuckDbListingHistoryRepository(uow.connection)
        universe_repository = DuckDbMarketUniverseHistoryRepository(uow.connection)

        service = MarketUniverseHistoryService(
            listing_repository=listing_repository,
            universe_repository=universe_repository,
            include_adr=args.include_adr,
            allowed_exchanges=tuple(args.allowed_exchanges),
            rule_version=args.rule_version,
        )
        pipeline = BuildMarketUniverseHistoryPipeline(service)
        result = pipeline.run()

        # Dédup SQL-first en sortie:
        # plusieurs listing_id actifs peuvent pointer vers le même listing économique.
        # On garde une seule ligne par clé logique historisée.
        uow.connection.execute(
            """
            CREATE OR REPLACE TEMP TABLE tmp_market_universe_history_dedup AS
            WITH ranked AS (
                SELECT
                    listing_id,
                    company_id,
                    symbol,
                    cik,
                    exchange,
                    security_type,
                    eligible_flag,
                    eligible_reason,
                    rule_version,
                    effective_from,
                    effective_to,
                    is_active,
                    created_at,
                    updated_at,
                    ROW_NUMBER() OVER (
                        PARTITION BY
                            UPPER(TRIM(COALESCE(symbol, ''))),
                            COALESCE(effective_from, DATE '1900-01-01'),
                            COALESCE(effective_to, DATE '9999-12-31'),
                            COALESCE(is_active, FALSE)
                        ORDER BY
                            CASE WHEN COALESCE(cik, '') <> '' THEN 0 ELSE 1 END,
                            CASE WHEN COALESCE(company_id, '') <> '' THEN 0 ELSE 1 END,
                            updated_at DESC NULLS LAST,
                            created_at DESC NULLS LAST,
                            COALESCE(exchange, '') DESC,
                            COALESCE(security_type, '') DESC,
                            listing_id DESC
                    ) AS rn
                FROM market_universe_history
            )
            SELECT
                listing_id,
                company_id,
                symbol,
                cik,
                exchange,
                security_type,
                eligible_flag,
                eligible_reason,
                rule_version,
                effective_from,
                effective_to,
                is_active,
                created_at,
                updated_at
            FROM ranked
            WHERE rn = 1
            """
        )

        before_rows = uow.connection.execute(
            "SELECT COUNT(*) FROM market_universe_history"
        ).fetchone()[0]

        dup_groups = uow.connection.execute(
            """
            SELECT COUNT(*)
            FROM (
                SELECT
                    UPPER(TRIM(COALESCE(symbol, ''))) AS symbol_key,
                    COALESCE(effective_from, DATE '1900-01-01') AS effective_from_key,
                    COALESCE(effective_to, DATE '9999-12-31') AS effective_to_key,
                    COALESCE(is_active, FALSE) AS is_active_key,
                    COUNT(*) AS dup_count
                FROM market_universe_history
                GROUP BY 1,2,3,4
                HAVING COUNT(*) > 1
            ) d
            """
        ).fetchone()[0]

        uow.connection.execute("DELETE FROM market_universe_history")
        uow.connection.execute(
            """
            INSERT INTO market_universe_history (
                listing_id,
                company_id,
                symbol,
                cik,
                exchange,
                security_type,
                eligible_flag,
                eligible_reason,
                rule_version,
                effective_from,
                effective_to,
                is_active,
                created_at,
                updated_at
            )
            SELECT
                listing_id,
                company_id,
                symbol,
                cik,
                exchange,
                security_type,
                eligible_flag,
                eligible_reason,
                rule_version,
                effective_from,
                effective_to,
                is_active,
                created_at,
                updated_at
            FROM tmp_market_universe_history_dedup
            """
        )

        after_rows = uow.connection.execute(
            "SELECT COUNT(*) FROM market_universe_history"
        ).fetchone()[0]

        dup_groups_after = uow.connection.execute(
            """
            SELECT COUNT(*)
            FROM (
                SELECT
                    UPPER(TRIM(COALESCE(symbol, ''))) AS symbol_key,
                    COALESCE(effective_from, DATE '1900-01-01') AS effective_from_key,
                    COALESCE(effective_to, DATE '9999-12-31') AS effective_to_key,
                    COALESCE(is_active, FALSE) AS is_active_key,
                    COUNT(*) AS dup_count
                FROM market_universe_history
                GROUP BY 1,2,3,4
                HAVING COUNT(*) > 1
            ) d
            """
        ).fetchone()[0]

        summary = result.summary_dict()
        summary.setdefault("metrics", {})
        summary["metrics"]["dedup_rows_before"] = int(before_rows)
        summary["metrics"]["dedup_rows_after"] = int(after_rows)
        summary["metrics"]["dedup_duplicate_groups_before"] = int(dup_groups)
        summary["metrics"]["dedup_duplicate_groups_after"] = int(dup_groups_after)

    print(json.dumps(summary, indent=2))
    return 0 if summary.get("status") == "SUCCESS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
