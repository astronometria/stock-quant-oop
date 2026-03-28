#!/usr/bin/env python3
from __future__ import annotations

"""
build_universe_membership_history_from_market_universe_history.py

Objectif
--------
Construire `universe_membership_history` à partir des snapshots historiques
reconstruits, avec une logique strictement snapshot-primary.

Sources utilisées
-----------------
- market_universe_history   : source primaire d'observation et de membership
- symbol_reference_history  : enrichissement identité (fallback optionnel)
- listing_status_history    : enrichissement identité / listing_id / company_id / cik (fallback optionnel)

Décision de design
------------------
Une ligne de `market_universe_history` doit produire AU PLUS UNE ligne dans
`universe_membership_history` pour l'univers demandé.

C'est le point clé.

Pourquoi ?
----------
Parce que `market_universe_history` représente déjà l'observation historique
du membership. Les autres tables ne doivent pas introduire une seconde vérité
concurrente sur le statut ACTIVE/INACTIVE.

Règles
------
1. Base primaire :
   - market_universe_history

2. membership_status :
   - ACTIVE si eligible_flag = TRUE
   - INACTIVE sinon

3. reason :
   - priorité à eligible_reason
   - fallback explicite si absent

4. identity fallback :
   - priorité :
       mu.company_id
       sr.company_id
       ls.company_id
       mu.cik
       sr.cik
       ls.cik
       mu.listing_id
       sr.listing_id
       ls.listing_id
       mu.symbol

5. Déduplication :
   - une seule ligne par snapshot logique :
       symbol + effective_from + effective_to + eligible_flag + eligible_reason + universe_name
   - on préfère une ligne avec company_id présente
   - puis cik / listing_id si disponibles

Important
---------
- On ne mélange pas ici les règles métier de listing_status_history.
- listing_status_history n'est utilisé que comme fallback d'identité.
- On ne construit pas ticker_history ici.
"""

import argparse
import json

from stock_quant.infrastructure.config.settings_loader import build_app_config
from stock_quant.infrastructure.db.duckdb_session_factory import DuckDbSessionFactory
from stock_quant.infrastructure.db.schema_manager import SchemaManager
from stock_quant.infrastructure.db.unit_of_work import DuckDbUnitOfWork


DEFAULT_UNIVERSE_NAME = "RECONSTRUCTED_LISTED_TICKER_UNIVERSE"
DEFAULT_SOURCE_NAME = "build_universe_membership_history_from_market_universe_history"


def parse_args() -> argparse.Namespace:
    """
    Arguments CLI du builder.
    """
    parser = argparse.ArgumentParser(
        description=(
            "Build universe_membership_history from market_universe_history with "
            "snapshot-primary semantics and identity fallback only."
        )
    )
    parser.add_argument(
        "--db-path",
        default=None,
        help="Path to DuckDB database file.",
    )
    parser.add_argument(
        "--universe-name",
        default=DEFAULT_UNIVERSE_NAME,
        help="Universe name written to universe_membership_history.",
    )
    parser.add_argument(
        "--source-name",
        default=DEFAULT_SOURCE_NAME,
        help="Source name written to universe_membership_history.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose terminal metrics.",
    )
    return parser.parse_args()


def main() -> int:
    """
    Pipeline principal.

    Étapes :
    1. validate DB/schema
    2. stage one-row-per-snapshot membership rows
    3. full replace for selected universe_name
    4. final metrics
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
        # Probes de base
        # --------------------------------------------------------------------
        market_rows = int(con.execute("SELECT COUNT(*) FROM market_universe_history").fetchone()[0])

        if market_rows <= 0:
            raise RuntimeError("market_universe_history is empty; cannot build universe_membership_history")

        symbol_ref_exists = True
        try:
            symbol_ref_rows = int(con.execute("SELECT COUNT(*) FROM symbol_reference_history").fetchone()[0])
        except Exception:
            symbol_ref_exists = False
            symbol_ref_rows = 0

        listing_exists = True
        try:
            listing_rows = int(con.execute("SELECT COUNT(*) FROM listing_status_history").fetchone()[0])
        except Exception:
            listing_exists = False
            listing_rows = 0

        # --------------------------------------------------------------------
        # Stage SQL-first
        # --------------------------------------------------------------------
        #
        # Décision importante :
        # - mu = vérité primaire du membership
        # - sr/ls = fallback identité uniquement
        #
        # On utilise LEFT JOIN LATERAL + LIMIT 1 pour éviter de multiplier
        # les lignes lorsque plusieurs périodes de listing ou références
        # matchent temporellement.
        # --------------------------------------------------------------------
        con.execute("DROP TABLE IF EXISTS tmp_universe_membership_history_stage")

        con.execute(
            """
            CREATE TEMP TABLE tmp_universe_membership_history_stage AS
            WITH mu AS (
                SELECT
                    NULLIF(TRIM(CAST(listing_id AS VARCHAR)), '') AS listing_id,
                    NULLIF(TRIM(CAST(company_id AS VARCHAR)), '') AS company_id,
                    NULLIF(TRIM(CAST(cik AS VARCHAR)), '') AS cik,
                    UPPER(TRIM(CAST(symbol AS VARCHAR))) AS symbol,
                    CAST(exchange AS VARCHAR) AS exchange,
                    CAST(security_type AS VARCHAR) AS security_type,
                    CAST(eligible_flag AS BOOLEAN) AS eligible_flag,
                    CAST(eligible_reason AS VARCHAR) AS eligible_reason,
                    CAST(rule_version AS VARCHAR) AS rule_version,
                    CAST(effective_from AS DATE) AS effective_from,
                    CAST(effective_to AS DATE) AS effective_to,
                    CAST(is_active AS BOOLEAN) AS is_active
                FROM market_universe_history
                WHERE symbol IS NOT NULL
                  AND TRIM(CAST(symbol AS VARCHAR)) <> ''
                  AND effective_from IS NOT NULL
            ),
            sr_best AS (
                SELECT
                    mu.symbol,
                    mu.effective_from,
                    mu.effective_to,
                    sr_match.listing_id AS sr_listing_id,
                    sr_match.company_id AS sr_company_id,
                    sr_match.cik AS sr_cik
                FROM mu
                LEFT JOIN LATERAL (
                    SELECT
                        NULLIF(TRIM(CAST(sr.listing_id AS VARCHAR)), '') AS listing_id,
                        NULLIF(TRIM(CAST(sr.company_id AS VARCHAR)), '') AS company_id,
                        NULLIF(TRIM(CAST(sr.cik AS VARCHAR)), '') AS cik,
                        CAST(sr.effective_from AS DATE) AS sr_effective_from,
                        CAST(sr.effective_to AS DATE) AS sr_effective_to,
                        CAST(sr.is_active AS BOOLEAN) AS sr_is_active
                    FROM symbol_reference_history sr
                    WHERE UPPER(TRIM(CAST(sr.symbol AS VARCHAR))) = mu.symbol
                      AND (
                            sr.effective_from IS NULL
                            OR CAST(sr.effective_from AS DATE) <= mu.effective_from
                          )
                      AND (
                            sr.effective_to IS NULL
                            OR mu.effective_from <= CAST(sr.effective_to AS DATE)
                          )
                    ORDER BY
                        CASE WHEN sr.company_id IS NOT NULL THEN 0 ELSE 1 END,
                        CASE WHEN sr.cik IS NOT NULL THEN 0 ELSE 1 END,
                        CASE WHEN sr.listing_id IS NOT NULL THEN 0 ELSE 1 END,
                        sr.effective_from DESC NULLS LAST,
                        sr.effective_to DESC NULLS LAST
                    LIMIT 1
                ) sr_match ON TRUE
            ),
            ls_best AS (
                SELECT
                    mu.symbol,
                    mu.effective_from,
                    mu.effective_to,
                    ls_match.listing_id AS ls_listing_id,
                    ls_match.company_id AS ls_company_id,
                    ls_match.cik AS ls_cik
                FROM mu
                LEFT JOIN LATERAL (
                    SELECT
                        NULLIF(TRIM(CAST(ls.listing_id AS VARCHAR)), '') AS listing_id,
                        NULLIF(TRIM(CAST(ls.company_id AS VARCHAR)), '') AS company_id,
                        NULLIF(TRIM(CAST(ls.cik AS VARCHAR)), '') AS cik,
                        CAST(ls.effective_from AS DATE) AS ls_effective_from,
                        CAST(ls.effective_to AS DATE) AS ls_effective_to,
                        CAST(ls.is_active AS BOOLEAN) AS ls_is_active
                    FROM listing_status_history ls
                    WHERE UPPER(TRIM(CAST(ls.symbol AS VARCHAR))) = mu.symbol
                      AND (
                            ls.effective_from IS NULL
                            OR CAST(ls.effective_from AS DATE) <= mu.effective_from
                          )
                      AND (
                            ls.effective_to IS NULL
                            OR mu.effective_from <= CAST(ls.effective_to AS DATE)
                          )
                    ORDER BY
                        CASE WHEN ls.company_id IS NOT NULL THEN 0 ELSE 1 END,
                        CASE WHEN ls.cik IS NOT NULL THEN 0 ELSE 1 END,
                        CASE WHEN ls.listing_id IS NOT NULL THEN 0 ELSE 1 END,
                        ls.effective_from DESC NULLS LAST,
                        ls.effective_to DESC NULLS LAST
                    LIMIT 1
                ) ls_match ON TRUE
            ),
            joined AS (
                SELECT
                    COALESCE(
                        mu.company_id,
                        sr_best.sr_company_id,
                        ls_best.ls_company_id,
                        mu.cik,
                        sr_best.sr_cik,
                        ls_best.ls_cik,
                        mu.listing_id,
                        sr_best.sr_listing_id,
                        ls_best.ls_listing_id,
                        mu.symbol
                    ) AS instrument_id,
                    COALESCE(mu.company_id, sr_best.sr_company_id, ls_best.ls_company_id) AS company_id,
                    mu.symbol AS symbol,
                    mu.effective_from AS effective_from,
                    mu.effective_to AS effective_to,
                    CASE
                        WHEN COALESCE(mu.eligible_flag, FALSE) = TRUE THEN 'ACTIVE'
                        ELSE 'INACTIVE'
                    END AS membership_status,
                    CASE
                        WHEN COALESCE(mu.eligible_flag, FALSE) = TRUE THEN
                            COALESCE(mu.eligible_reason, 'OBSERVED_IN_MARKET_UNIVERSE_HISTORY')
                        ELSE
                            COALESCE(mu.eligible_reason, 'NOT_ELIGIBLE_IN_MARKET_UNIVERSE_HISTORY')
                    END AS reason,
                    mu.eligible_flag AS eligible_flag,
                    mu.exchange AS exchange,
                    mu.security_type AS security_type
                FROM mu
                LEFT JOIN sr_best
                  ON sr_best.symbol = mu.symbol
                 AND sr_best.effective_from = mu.effective_from
                 AND (
                        (sr_best.effective_to IS NULL AND mu.effective_to IS NULL)
                        OR sr_best.effective_to = mu.effective_to
                     )
                LEFT JOIN ls_best
                  ON ls_best.symbol = mu.symbol
                 AND ls_best.effective_from = mu.effective_from
                 AND (
                        (ls_best.effective_to IS NULL AND mu.effective_to IS NULL)
                        OR ls_best.effective_to = mu.effective_to
                     )
            ),
            dedup AS (
                SELECT
                    instrument_id,
                    company_id,
                    symbol,
                    effective_from,
                    effective_to,
                    membership_status,
                    reason,
                    ROW_NUMBER() OVER (
                        PARTITION BY
                            symbol,
                            effective_from,
                            COALESCE(effective_to, DATE '9999-12-31'),
                            membership_status,
                            reason
                        ORDER BY
                            CASE WHEN company_id IS NOT NULL THEN 0 ELSE 1 END,
                            CASE WHEN instrument_id IS NOT NULL THEN 0 ELSE 1 END,
                            instrument_id
                    ) AS rn
                FROM joined
                WHERE symbol IS NOT NULL
                  AND effective_from IS NOT NULL
                  AND membership_status IS NOT NULL
            )
            SELECT
                instrument_id,
                company_id,
                symbol,
                effective_from,
                effective_to,
                membership_status,
                reason
            FROM dedup
            WHERE rn = 1
            """
        )

        staged_rows = int(
            con.execute("SELECT COUNT(*) FROM tmp_universe_membership_history_stage").fetchone()[0]
        )

        staged_symbols = int(
            con.execute("SELECT COUNT(DISTINCT symbol) FROM tmp_universe_membership_history_stage").fetchone()[0]
        )

        staged_instruments = int(
            con.execute("SELECT COUNT(DISTINCT instrument_id) FROM tmp_universe_membership_history_stage").fetchone()[0]
        )

        staged_min_date, staged_max_date = con.execute(
            """
            SELECT MIN(effective_from), MAX(effective_from)
            FROM tmp_universe_membership_history_stage
            """
        ).fetchone()

        # --------------------------------------------------------------------
        # Full replace pour cet universe_name uniquement
        # --------------------------------------------------------------------
        con.execute(
            """
            DELETE FROM universe_membership_history
            WHERE universe_name = ?
            """,
            [args.universe_name],
        )

        con.execute(
            """
            INSERT INTO universe_membership_history (
                instrument_id,
                company_id,
                symbol,
                universe_name,
                effective_from,
                effective_to,
                membership_status,
                reason,
                source_name,
                created_at
            )
            SELECT
                instrument_id,
                company_id,
                symbol,
                ? AS universe_name,
                effective_from,
                effective_to,
                membership_status,
                reason,
                ? AS source_name,
                CURRENT_TIMESTAMP AS created_at
            FROM tmp_universe_membership_history_stage
            """,
            [args.universe_name, args.source_name],
        )

        written_rows = int(
            con.execute(
                """
                SELECT COUNT(*)
                FROM universe_membership_history
                WHERE universe_name = ?
                """,
                [args.universe_name],
            ).fetchone()[0]
        )

        written_symbols = int(
            con.execute(
                """
                SELECT COUNT(DISTINCT symbol)
                FROM universe_membership_history
                WHERE universe_name = ?
                """,
                [args.universe_name],
            ).fetchone()[0]
        )

        written_instruments = int(
            con.execute(
                """
                SELECT COUNT(DISTINCT instrument_id)
                FROM universe_membership_history
                WHERE universe_name = ?
                """,
                [args.universe_name],
            ).fetchone()[0]
        )

        active_rows = int(
            con.execute(
                """
                SELECT COUNT(*)
                FROM universe_membership_history
                WHERE universe_name = ?
                  AND membership_status = 'ACTIVE'
                """,
                [args.universe_name],
            ).fetchone()[0]
        )

        inactive_rows = int(
            con.execute(
                """
                SELECT COUNT(*)
                FROM universe_membership_history
                WHERE universe_name = ?
                  AND membership_status = 'INACTIVE'
                """,
                [args.universe_name],
            ).fetchone()[0]
        )

        open_rows = int(
            con.execute(
                """
                SELECT COUNT(*)
                FROM universe_membership_history
                WHERE universe_name = ?
                  AND effective_to IS NULL
                """,
                [args.universe_name],
            ).fetchone()[0]
        )

        # Probe de qualité : détection des doublons logiques restants.
        logical_duplicate_rows = int(
            con.execute(
                """
                WITH x AS (
                    SELECT
                        symbol,
                        effective_from,
                        COALESCE(effective_to, DATE '9999-12-31') AS effective_to_norm,
                        membership_status,
                        reason,
                        COUNT(*) AS cnt
                    FROM universe_membership_history
                    WHERE universe_name = ?
                    GROUP BY 1,2,3,4,5
                )
                SELECT COALESCE(SUM(cnt - 1), 0)
                FROM x
                WHERE cnt > 1
                """,
                [args.universe_name],
            ).fetchone()[0]
        )

        out = {
            "db_path": str(config.db_path),
            "universe_name": args.universe_name,
            "source_name": args.source_name,
            "market_universe_history_rows": market_rows,
            "symbol_reference_history_exists": symbol_ref_exists,
            "symbol_reference_history_rows": symbol_ref_rows,
            "listing_status_history_exists": listing_exists,
            "listing_status_history_rows": listing_rows,
            "staged_rows": staged_rows,
            "staged_symbols": staged_symbols,
            "staged_instruments": staged_instruments,
            "staged_min_effective_from": str(staged_min_date) if staged_min_date is not None else None,
            "staged_max_effective_from": str(staged_max_date) if staged_max_date is not None else None,
            "written_rows": written_rows,
            "written_symbols": written_symbols,
            "written_instruments": written_instruments,
            "active_rows": active_rows,
            "inactive_rows": inactive_rows,
            "open_rows": open_rows,
            "logical_duplicate_rows": logical_duplicate_rows,
            "mode": "historical_snapshot_to_universe_membership_history_sql_first_snapshot_primary",
        }

    if args.verbose:
        print(
            "[build_universe_membership_history_from_market_universe_history] "
            + json.dumps(out, ensure_ascii=False)
        )

    print(json.dumps(out, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
