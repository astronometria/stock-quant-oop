#!/usr/bin/env python3
from __future__ import annotations

"""
Détecte et journalise des événements de listing à partir des snapshots déjà accumulés.

But
---
- ne pas inventer un historique complet
- détecter seulement des événements plausibles entre snapshots observés
- produire des événements auditables pour revue scientifique

Événements détectés
-------------------
- POSSIBLE_NEW_LISTING
- POSSIBLE_DISAPPEARANCE
- POSSIBLE_COMPANY_NAME_CHANGE
- POSSIBLE_EXCHANGE_CHANGE
- POSSIBLE_SECURITY_TYPE_CHANGE
"""

import argparse
import json
from pathlib import Path

import duckdb
from tqdm import tqdm


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Detect audit-grade listing events from observed metadata snapshots."
    )
    parser.add_argument("--db-path", required=True, help="Path to DuckDB database.")
    parser.add_argument(
        "--source-table",
        default="symbol_reference_source_raw",
        help="Observed metadata snapshot table to use.",
    )
    return parser.parse_args()


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


def ensure_event_tables(con: duckdb.DuckDBPyConnection) -> None:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS listing_event_history (
            event_id VARCHAR,
            listing_id VARCHAR,
            symbol VARCHAR,
            company_id VARCHAR,
            cik VARCHAR,
            event_type VARCHAR,
            event_date DATE,
            old_value VARCHAR,
            new_value VARCHAR,
            source_name VARCHAR,
            evidence_type VARCHAR,
            confidence_level VARCHAR,
            notes VARCHAR,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )

    con.execute(
        """
        CREATE TABLE IF NOT EXISTS history_reconstruction_audit (
            audit_id VARCHAR,
            entity_type VARCHAR,
            entity_key VARCHAR,
            action_type VARCHAR,
            source_name VARCHAR,
            evidence_type VARCHAR,
            confidence_level VARCHAR,
            old_value VARCHAR,
            new_value VARCHAR,
            notes VARCHAR,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )


def main() -> int:
    args = parse_args()
    db_path = Path(args.db_path).expanduser().resolve()

    con = duckdb.connect(str(db_path))
    steps = tqdm(total=6, desc="apply_listing_events_from_snapshots", unit="step")

    try:
        steps.set_description("apply_listing_events_from_snapshots:validate")
        if not table_exists(con, args.source_table):
            raise RuntimeError(f"Missing source table: {args.source_table}")
        if not table_exists(con, "listing_status_history"):
            raise RuntimeError("Missing required table: listing_status_history")
        ensure_event_tables(con)
        steps.update(1)

        steps.set_description("apply_listing_events_from_snapshots:prepare_observed")
        con.execute(
            f"""
            CREATE OR REPLACE TEMP TABLE tmp_snapshot_observed AS
            SELECT
                CAST(as_of_date AS DATE) AS as_of_date,
                UPPER(TRIM(CAST(symbol AS VARCHAR))) AS symbol,
                NULLIF(TRIM(CAST(cik AS VARCHAR)), '') AS cik,
                NULLIF(TRIM(CAST(company_name AS VARCHAR)), '') AS company_name,
                NULLIF(TRIM(CAST(exchange_raw AS VARCHAR)), '') AS exchange_raw,
                NULLIF(TRIM(CAST(security_type_raw AS VARCHAR)), '') AS security_type_raw,
                NULLIF(TRIM(CAST(source_name AS VARCHAR)), '') AS source_name
            FROM {args.source_table}
            WHERE as_of_date IS NOT NULL
              AND symbol IS NOT NULL
              AND TRIM(CAST(symbol AS VARCHAR)) <> ''
            """
        )
        steps.update(1)

        steps.set_description("apply_listing_events_from_snapshots:pair_dates")
        con.execute(
            """
            CREATE OR REPLACE TEMP TABLE tmp_snapshot_pairs AS
            WITH ordered_dates AS (
                SELECT DISTINCT as_of_date
                FROM tmp_snapshot_observed
            ),
            paired AS (
                SELECT
                    as_of_date AS current_date,
                    LEAD(as_of_date) OVER (ORDER BY as_of_date) AS next_date
                FROM ordered_dates
            )
            SELECT
                current_date,
                next_date
            FROM paired
            WHERE next_date IS NOT NULL
            """
        )
        pair_count = con.execute("SELECT COUNT(*) FROM tmp_snapshot_pairs").fetchone()[0]
        steps.update(1)

        steps.set_description("apply_listing_events_from_snapshots:detect_events")
        con.execute("BEGIN TRANSACTION")

        con.execute(
            """
            CREATE OR REPLACE TEMP TABLE tmp_detected_events AS
            WITH current_obs AS (
                SELECT * FROM tmp_snapshot_observed
            ),
            paired_current AS (
                SELECT
                    p.current_date,
                    p.next_date,
                    c.symbol,
                    c.cik,
                    c.company_name,
                    c.exchange_raw,
                    c.security_type_raw,
                    c.source_name
                FROM tmp_snapshot_pairs p
                INNER JOIN current_obs c
                    ON c.as_of_date = p.current_date
            ),
            next_obs AS (
                SELECT
                    as_of_date,
                    symbol,
                    cik,
                    company_name,
                    exchange_raw,
                    security_type_raw,
                    source_name
                FROM tmp_snapshot_observed
            ),
            comparison AS (
                SELECT
                    pc.current_date,
                    pc.next_date,
                    pc.symbol,
                    pc.cik AS current_cik,
                    pc.company_name AS current_company_name,
                    pc.exchange_raw AS current_exchange,
                    pc.security_type_raw AS current_security_type,
                    pc.source_name AS current_source_name,
                    no.cik AS next_cik,
                    no.company_name AS next_company_name,
                    no.exchange_raw AS next_exchange,
                    no.security_type_raw AS next_security_type,
                    no.source_name AS next_source_name
                FROM paired_current pc
                LEFT JOIN next_obs no
                    ON no.as_of_date = pc.next_date
                   AND no.symbol = pc.symbol
            )
            SELECT DISTINCT
                symbol,
                current_cik,
                current_date,
                next_date,
                CASE
                    WHEN next_cik IS NULL THEN 'POSSIBLE_DISAPPEARANCE'
                    WHEN COALESCE(UPPER(TRIM(current_company_name)), '') <> COALESCE(UPPER(TRIM(next_company_name)), '')
                        THEN 'POSSIBLE_COMPANY_NAME_CHANGE'
                    WHEN COALESCE(UPPER(TRIM(current_exchange)), '') <> COALESCE(UPPER(TRIM(next_exchange)), '')
                        THEN 'POSSIBLE_EXCHANGE_CHANGE'
                    WHEN COALESCE(UPPER(TRIM(current_security_type)), '') <> COALESCE(UPPER(TRIM(next_security_type)), '')
                        THEN 'POSSIBLE_SECURITY_TYPE_CHANGE'
                    ELSE NULL
                END AS event_type,
                current_company_name,
                next_company_name,
                current_exchange,
                next_exchange,
                current_security_type,
                next_security_type,
                COALESCE(next_source_name, current_source_name, 'snapshot_observed') AS source_name
            FROM comparison
            WHERE
                (
                    next_cik IS NULL
                    OR COALESCE(UPPER(TRIM(current_company_name)), '') <> COALESCE(UPPER(TRIM(next_company_name)), '')
                    OR COALESCE(UPPER(TRIM(current_exchange)), '') <> COALESCE(UPPER(TRIM(next_exchange)), '')
                    OR COALESCE(UPPER(TRIM(current_security_type)), '') <> COALESCE(UPPER(TRIM(next_security_type)), '')
                )
            """
        )

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
                old_value,
                new_value,
                source_name,
                evidence_type,
                confidence_level,
                notes,
                created_at
            )
            SELECT
                'EVENT:SNAPSHOT:' || event_type || ':' || symbol || ':' || CAST(current_date AS VARCHAR) || ':' || CAST(next_date AS VARCHAR),
                NULL,
                symbol,
                NULL,
                current_cik,
                event_type,
                next_date,
                CASE
                    WHEN event_type = 'POSSIBLE_COMPANY_NAME_CHANGE' THEN COALESCE(current_company_name, '')
                    WHEN event_type = 'POSSIBLE_EXCHANGE_CHANGE' THEN COALESCE(current_exchange, '')
                    WHEN event_type = 'POSSIBLE_SECURITY_TYPE_CHANGE' THEN COALESCE(current_security_type, '')
                    WHEN event_type = 'POSSIBLE_DISAPPEARANCE' THEN 'OBSERVED_IN_PREVIOUS_SNAPSHOT'
                    ELSE ''
                END,
                CASE
                    WHEN event_type = 'POSSIBLE_COMPANY_NAME_CHANGE' THEN COALESCE(next_company_name, '')
                    WHEN event_type = 'POSSIBLE_EXCHANGE_CHANGE' THEN COALESCE(next_exchange, '')
                    WHEN event_type = 'POSSIBLE_SECURITY_TYPE_CHANGE' THEN COALESCE(next_security_type, '')
                    WHEN event_type = 'POSSIBLE_DISAPPEARANCE' THEN 'MISSING_IN_NEXT_OBSERVED_SNAPSHOT'
                    ELSE ''
                END,
                source_name,
                'OBSERVED_SNAPSHOT_DIFF',
                'MEDIUM',
                'Event derived from adjacent observed snapshots only.',
                CURRENT_TIMESTAMP
            FROM tmp_detected_events
            WHERE event_type IS NOT NULL
            """
        )

        con.execute(
            """
            INSERT INTO history_reconstruction_audit (
                audit_id,
                entity_type,
                entity_key,
                action_type,
                source_name,
                evidence_type,
                confidence_level,
                old_value,
                new_value,
                notes,
                created_at
            )
            SELECT
                'AUDIT:SNAPSHOT_EVENT:' || event_type || ':' || symbol || ':' || CAST(current_date AS VARCHAR) || ':' || CAST(next_date AS VARCHAR),
                'listing_event_history',
                symbol,
                event_type,
                source_name,
                'OBSERVED_SNAPSHOT_DIFF',
                'MEDIUM',
                CAST(current_date AS VARCHAR),
                CAST(next_date AS VARCHAR),
                'Snapshot-to-snapshot derived event candidate.',
                CURRENT_TIMESTAMP
            FROM tmp_detected_events
            WHERE event_type IS NOT NULL
            """
        )

        con.execute("COMMIT")
        steps.update(1)

        steps.set_description("apply_listing_events_from_snapshots:metrics")
        metrics = con.execute(
            """
            SELECT
                COUNT(*) AS event_rows,
                COUNT(DISTINCT symbol) AS symbols_total,
                COUNT(DISTINCT event_type) AS event_types_total
            FROM tmp_detected_events
            WHERE event_type IS NOT NULL
            """
        ).fetchone()

        event_breakdown = con.execute(
            """
            SELECT
                event_type,
                COUNT(*) AS rows_total
            FROM tmp_detected_events
            WHERE event_type IS NOT NULL
            GROUP BY 1
            ORDER BY 2 DESC, 1
            """
        ).fetchall()
        steps.update(1)

        steps.set_description("apply_listing_events_from_snapshots:done")
        steps.update(1)
        steps.close()

        print(json.dumps(
            {
                "db_path": str(db_path),
                "source_table": args.source_table,
                "snapshot_pairs": int(pair_count),
                "event_rows_detected": int(metrics[0]),
                "symbols_total_with_events": int(metrics[1]),
                "event_types_total": int(metrics[2]),
                "event_breakdown": [
                    {"event_type": row[0], "rows_total": int(row[1])}
                    for row in event_breakdown
                ],
                "mode": "observed_snapshot_diff_only",
            },
            indent=2,
        ))
        return 0
    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
