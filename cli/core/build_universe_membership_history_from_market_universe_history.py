#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb
from tqdm import tqdm


def parse_args() -> argparse.Namespace:
    """
    Construit les arguments CLI.

    Notes:
    - on part strictement de market_universe_history
    - on ne crée aucune période non observée
    - on déduplique les doublons logiques avant insertion
    """
    p = argparse.ArgumentParser(
        description="Build universe_membership_history from market_universe_history without inventing non-observed periods."
    )
    p.add_argument("--db-path", required=True)
    p.add_argument(
        "--universe-name",
        default="market_universe_history",
        help="Universe name label to store in universe_membership_history.",
    )
    return p.parse_args()


def table_exists(con: duckdb.DuckDBPyConnection, name: str) -> bool:
    """
    Vérifie la présence d'une table.
    """
    row = con.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_name = ?
        """,
        [name],
    ).fetchone()
    return bool(row and row[0] > 0)


def main() -> int:
    """
    Reconstruit universe_membership_history de manière SQL-first et idempotente.

    Logique:
    - DELETE target
    - lire market_universe_history
    - normaliser les colonnes clés
    - dédupliquer les doublons logiques
    - insérer une seule ligne par membership logique
    """
    args = parse_args()
    db_path = Path(args.db_path).expanduser().resolve()

    con = duckdb.connect(str(db_path))
    steps = tqdm(total=5, desc="build_universe_membership_history", unit="step")

    try:
        steps.set_description("build_universe_membership_history:validate")
        required = ["market_universe_history", "universe_membership_history"]
        missing = [t for t in required if not table_exists(con, t)]
        if missing:
            raise RuntimeError(f"Missing required tables: {missing}")
        steps.update(1)

        steps.set_description("build_universe_membership_history:clear_target")
        con.execute("DELETE FROM universe_membership_history")
        steps.update(1)

        steps.set_description("build_universe_membership_history:insert_deduped")
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
            WITH src AS (
                SELECT
                    CAST(listing_id AS VARCHAR) AS instrument_id,
                    CAST(company_id AS VARCHAR) AS company_id,
                    UPPER(TRIM(CAST(symbol AS VARCHAR))) AS symbol,
                    CAST(effective_from AS DATE) AS effective_from,
                    CAST(effective_to AS DATE) AS effective_to,
                    CASE
                        WHEN COALESCE(is_active, FALSE) THEN 'ACTIVE'
                        ELSE 'INACTIVE'
                    END AS membership_status,
                    CAST(rule_version AS VARCHAR) AS reason,
                    'market_universe_history' AS source_name,
                    CAST(exchange AS VARCHAR) AS exchange,
                    CAST(security_type AS VARCHAR) AS security_type,
                    CAST(cik AS VARCHAR) AS cik,
                    CAST(updated_at AS TIMESTAMP) AS updated_at,
                    CAST(created_at AS TIMESTAMP) AS created_at
                FROM market_universe_history
                WHERE symbol IS NOT NULL
                  AND TRIM(CAST(symbol AS VARCHAR)) <> ''
            ),
            ranked AS (
                SELECT
                    instrument_id,
                    company_id,
                    symbol,
                    effective_from,
                    effective_to,
                    membership_status,
                    reason,
                    source_name,
                    created_at,
                    ROW_NUMBER() OVER (
                        PARTITION BY
                            symbol,
                            COALESCE(effective_from, DATE '1900-01-01'),
                            COALESCE(effective_to, DATE '9999-12-31'),
                            membership_status
                        ORDER BY
                            CASE WHEN COALESCE(cik, '') <> '' THEN 0 ELSE 1 END,
                            CASE WHEN COALESCE(company_id, '') <> '' THEN 0 ELSE 1 END,
                            updated_at DESC NULLS LAST,
                            created_at DESC NULLS LAST,
                            COALESCE(exchange, '') DESC,
                            COALESCE(security_type, '') DESC,
                            instrument_id DESC
                    ) AS rn
                FROM src
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
                source_name,
                CURRENT_TIMESTAMP
            FROM ranked
            WHERE rn = 1
            """,
            [args.universe_name],
        )
        steps.update(1)

        steps.set_description("build_universe_membership_history:metrics")
        row = con.execute(
            """
            SELECT
                COUNT(*) AS rows_total,
                COUNT(DISTINCT symbol) AS symbols_total,
                MIN(effective_from) AS min_date,
                MAX(COALESCE(effective_to, DATE '9999-12-31')) AS max_date
            FROM universe_membership_history
            """
        ).fetchone()
        dup_row = con.execute(
            """
            SELECT COUNT(*)
            FROM (
                SELECT
                    symbol,
                    universe_name,
                    effective_from,
                    COALESCE(effective_to, DATE '9999-12-31') AS effective_to_norm,
                    membership_status,
                    COUNT(*) AS dup_count
                FROM universe_membership_history
                GROUP BY 1,2,3,4,5
                HAVING COUNT(*) > 1
            ) d
            """
        ).fetchone()
        steps.update(1)

        steps.set_description("build_universe_membership_history:done")
        print(
            json.dumps(
                {
                    "table_name": "universe_membership_history",
                    "rows_total": int(row[0]),
                    "symbols_total": int(row[1]),
                    "min_effective_from": str(row[2]) if row[2] is not None else None,
                    "max_effective_to_or_open": str(row[3]) if row[3] is not None else None,
                    "duplicate_groups_remaining": int(dup_row[0]),
                    "mode": "strict_from_market_universe_history_deduped",
                },
                indent=2,
            )
        )
        steps.update(1)
        steps.close()
        return 0
    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
