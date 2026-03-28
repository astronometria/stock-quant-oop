#!/usr/bin/env python3
from __future__ import annotations

"""
enrich_listing_history_from_sec_submissions.py

But:
- enrichir listing_status_history à partir des filings SEC déjà présents
- ne jamais détruire l'historique observé
- ne jamais inventer de snapshots complets
- uniquement:
  * remplir company_id / cik quand fiable
  * journaliser des événements / audits si on détecte une preuve exploitable

Hypothèses minimales:
- sec_filing_raw_index existe
- listing_status_history existe
- listing_event_history existe
- history_reconstruction_audit existe

Cette version reste volontairement prudente:
- si la table SEC est vide, elle sort proprement
- elle enrichit par CIK / ticker quand l'information existe déjà
- elle écrit un audit trail
"""

import argparse
import json
from pathlib import Path
from uuid import uuid4

import duckdb
from tqdm import tqdm


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Enrich listing_status_history from SEC submissions without inventing missing snapshots."
    )
    p.add_argument("--db-path", required=True)
    p.add_argument(
        "--sec-source-table",
        default="sec_filing_raw_index",
        help="SEC source table to probe.",
    )
    return p.parse_args()


def table_exists(con: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    row = con.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_name = ?
        """,
        [table_name],
    ).fetchone()
    return bool(row and row[0] > 0)


def get_columns(con: duckdb.DuckDBPyConnection, table_name: str) -> set[str]:
    return {str(row[0]) for row in con.execute(f"DESCRIBE {table_name}").fetchall()}


def pick_expr(columns: set[str], *candidates: str, default: str = "NULL") -> str:
    for candidate in candidates:
        if candidate in columns:
            return candidate
    return default


def main() -> int:
    args = parse_args()
    db_path = Path(args.db_path).expanduser().resolve()
    con = duckdb.connect(str(db_path))
    steps = tqdm(total=6, desc="enrich_listing_history_from_sec", unit="step")

    try:
        steps.set_description("enrich_listing_history_from_sec:validate")
        required = [
            "listing_status_history",
            "listing_event_history",
            "history_reconstruction_audit",
            args.sec_source_table,
        ]
        missing = [t for t in required if not table_exists(con, t)]
        if missing:
            raise RuntimeError(f"Missing required tables: {missing}")
        steps.update(1)

        steps.set_description("enrich_listing_history_from_sec:ensure_support")
        sec_cols = get_columns(con, args.sec_source_table)

        cik_expr = pick_expr(sec_cols, "cik", default="NULL")
        symbol_expr = pick_expr(sec_cols, "ticker", "symbol", default="NULL")
        company_name_expr = pick_expr(sec_cols, "company_name", "name", default="NULL")
        filing_date_expr = pick_expr(sec_cols, "filing_date", "accepted_date", "report_date", default="NULL")
        source_url_expr = pick_expr(sec_cols, "filing_href", "document_url", "url", default="NULL")

        steps.update(1)

        steps.set_description("enrich_listing_history_from_sec:build_sec_snapshot")
        sec_source_rows = con.execute(
            f"SELECT COUNT(*) FROM {args.sec_source_table}"
        ).fetchone()[0]

        if int(sec_source_rows) == 0:
            steps.update(1)
            steps.set_description("enrich_listing_history_from_sec:probe_candidates")
            candidate_rows = 0
            steps.update(1)
            steps.set_description("enrich_listing_history_from_sec:apply")
            audit_rows = 0
            event_rows = 0
            steps.update(1)
        else:
            con.execute("DROP TABLE IF EXISTS tmp_sec_listing_enrichment_candidates")
            con.execute(
                f"""
                CREATE TEMP TABLE tmp_sec_listing_enrichment_candidates AS
                WITH sec_norm AS (
                    SELECT
                        NULLIF(TRIM(CAST({cik_expr} AS VARCHAR)), '') AS cik,
                        UPPER(NULLIF(TRIM(CAST({symbol_expr} AS VARCHAR)), '')) AS symbol,
                        NULLIF(TRIM(CAST({company_name_expr} AS VARCHAR)), '') AS company_name,
                        CAST({filing_date_expr} AS DATE) AS filing_date,
                        CAST({source_url_expr} AS VARCHAR) AS source_url
                    FROM {args.sec_source_table}
                ),
                sec_best AS (
                    SELECT
                        cik,
                        symbol,
                        company_name,
                        filing_date,
                        source_url
                    FROM sec_norm
                    WHERE symbol IS NOT NULL
                )
                SELECT
                    l.listing_id,
                    l.symbol AS listing_symbol,
                    l.cik AS listing_cik,
                    s.symbol AS sec_symbol,
                    s.cik AS sec_cik,
                    s.company_name AS sec_company_name,
                    s.filing_date,
                    s.source_url
                FROM listing_status_history l
                INNER JOIN sec_best s
                    ON UPPER(TRIM(COALESCE(l.symbol, ''))) = s.symbol
                WHERE
                    (
                        NULLIF(TRIM(COALESCE(l.cik, '')), '') IS NULL
                        AND s.cik IS NOT NULL
                    )
                    OR (
                        NULLIF(TRIM(COALESCE(l.company_id, '')), '') IS NULL
                        AND s.cik IS NOT NULL
                    )
                """
            )
            steps.update(1)

            steps.set_description("enrich_listing_history_from_sec:probe_candidates")
            candidate_rows = con.execute(
                "SELECT COUNT(*) FROM tmp_sec_listing_enrichment_candidates"
            ).fetchone()[0]
            steps.update(1)

            steps.set_description("enrich_listing_history_from_sec:apply")

            # Enrichissement strictement non destructif:
            # - company_id <- sec_cik si absent
            # - cik        <- sec_cik si absent
            con.execute(
                """
                UPDATE listing_status_history AS l
                SET
                    company_id = COALESCE(l.company_id, c.sec_cik),
                    cik = COALESCE(l.cik, c.sec_cik),
                    updated_at = CURRENT_TIMESTAMP
                FROM (
                    SELECT DISTINCT
                        listing_id,
                        sec_cik
                    FROM tmp_sec_listing_enrichment_candidates
                    WHERE sec_cik IS NOT NULL
                ) c
                WHERE l.listing_id = c.listing_id
                """
            )

            run_id = f"sec_enrich_{uuid4().hex}"

            con.execute(
                """
                INSERT INTO history_reconstruction_audit (
                    run_id,
                    entity_type,
                    entity_key,
                    action_type,
                    source_name,
                    evidence_type,
                    confidence_level,
                    notes,
                    created_at
                )
                SELECT
                    ? AS run_id,
                    'listing_status_history' AS entity_type,
                    listing_id AS entity_key,
                    'SEC_ENRICH_COMPANY_ID' AS action_type,
                    'sec_filing_raw_index' AS source_name,
                    'sec_submission_match_on_symbol' AS evidence_type,
                    'MEDIUM' AS confidence_level,
                    CONCAT(
                        'Filled missing company_id/cik from SEC; sec_cik=',
                        COALESCE(sec_cik, ''),
                        '; filing_date=',
                        COALESCE(CAST(filing_date AS VARCHAR), '')
                    ) AS notes,
                    CURRENT_TIMESTAMP AS created_at
                FROM (
                    SELECT DISTINCT
                        listing_id,
                        sec_cik,
                        filing_date
                    FROM tmp_sec_listing_enrichment_candidates
                    WHERE sec_cik IS NOT NULL
                )
                """,
                [run_id],
            )

            # Événements prudents: seulement si company name SEC diffère du nom courant
            # et qu'on a un filing date.
            con.execute(
                """
                INSERT INTO listing_event_history (
                    event_id,
                    listing_id,
                    symbol,
                    company_id,
                    cik,
                    event_type,
                    event_date,
                    old_name,
                    new_name,
                    source_name,
                    source_url,
                    evidence_type,
                    confidence_level,
                    notes,
                    created_at
                )
                SELECT
                    CONCAT('SEC_NAME_EVENT:', md5(
                        COALESCE(listing_id, '') || '|' ||
                        COALESCE(sec_company_name, '') || '|' ||
                        COALESCE(CAST(filing_date AS VARCHAR), '')
                    )) AS event_id,
                    listing_id,
                    listing_symbol AS symbol,
                    sec_cik AS company_id,
                    sec_cik AS cik,
                    'SEC_OBSERVED_COMPANY_NAME' AS event_type,
                    filing_date AS event_date,
                    NULL AS old_name,
                    sec_company_name AS new_name,
                    'sec_filing_raw_index' AS source_name,
                    source_url,
                    'sec_submission_symbol_match' AS evidence_type,
                    'LOW' AS confidence_level,
                    'Observed SEC filing name differs from current listing history name; review manually.' AS notes,
                    CURRENT_TIMESTAMP AS created_at
                FROM (
                    SELECT DISTINCT
                        c.listing_id,
                        c.listing_symbol,
                        c.sec_cik,
                        c.sec_company_name,
                        c.filing_date,
                        c.source_url
                    FROM tmp_sec_listing_enrichment_candidates c
                    INNER JOIN listing_status_history l
                        ON l.listing_id = c.listing_id
                    WHERE
                        c.sec_company_name IS NOT NULL
                        AND c.filing_date IS NOT NULL
                        AND UPPER(TRIM(COALESCE(c.sec_company_name, ''))) <>
                            UPPER(TRIM(COALESCE(l.company_name, '')))
                )
                """
            )

            audit_rows = con.execute(
                """
                SELECT COUNT(*)
                FROM history_reconstruction_audit
                WHERE source_name = 'sec_filing_raw_index'
                """
            ).fetchone()[0]
            event_rows = con.execute(
                """
                SELECT COUNT(*)
                FROM listing_event_history
                WHERE source_name = 'sec_filing_raw_index'
                """
            ).fetchone()[0]
            steps.update(1)

        steps.set_description("enrich_listing_history_from_sec:metrics")

        listing_rows = con.execute("SELECT COUNT(*) FROM listing_status_history").fetchone()[0]
        symbols_total = con.execute(
            "SELECT COUNT(DISTINCT symbol) FROM listing_status_history WHERE symbol IS NOT NULL"
        ).fetchone()[0]
        cik_total = con.execute(
            "SELECT COUNT(DISTINCT cik) FROM listing_status_history WHERE NULLIF(TRIM(COALESCE(cik, '')), '') IS NOT NULL"
        ).fetchone()[0]
        company_id_total = con.execute(
            "SELECT COUNT(DISTINCT company_id) FROM listing_status_history WHERE NULLIF(TRIM(COALESCE(company_id, '')), '') IS NOT NULL"
        ).fetchone()[0]
        steps.update(1)

        print(
            json.dumps(
                {
                    "db_path": str(db_path),
                    "sec_source_table": args.sec_source_table,
                    "sec_source_rows": int(sec_source_rows),
                    "candidate_rows": int(candidate_rows),
                    "listing_status_history_rows": int(listing_rows),
                    "symbols_total": int(symbols_total),
                    "distinct_cik_populated": int(cik_total),
                    "distinct_company_id_populated": int(company_id_total),
                    "audit_rows_total_for_sec_enrichment": int(audit_rows),
                    "event_rows_total_for_sec_enrichment": int(event_rows),
                    "mode": "sql_first_non_destructive_enrichment",
                },
                indent=2,
            )
        )
        return 0
    finally:
        steps.close()
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
