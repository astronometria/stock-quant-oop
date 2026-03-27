#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb
from tqdm import tqdm


def parse_args() -> argparse.Namespace:
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
    args = parse_args()
    db_path = Path(args.db_path).expanduser().resolve()

    con = duckdb.connect(str(db_path))
    steps = tqdm(total=4, desc="build_universe_membership_history", unit="step")

    try:
        steps.set_description("build_universe_membership_history:validate")
        required = ["market_universe_history", "universe_membership_history"]
        missing = [t for t in required if not table_exists(con, t)]
        if missing:
            raise RuntimeError(f"Missing required tables: {missing}")
        steps.update(1)

        steps.set_description("build_universe_membership_history:replace")
        con.execute("DELETE FROM universe_membership_history")
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
                    UPPER(TRIM(symbol)) AS symbol,
                    CAST(effective_from AS DATE) AS effective_from,
                    CAST(effective_to AS DATE) AS effective_to,
                    CASE WHEN COALESCE(is_active, FALSE) THEN 'ACTIVE' ELSE 'INACTIVE' END AS membership_status,
                    CAST(rule_version AS VARCHAR) AS reason,
                    'market_universe_history' AS source_name
                FROM market_universe_history
                WHERE symbol IS NOT NULL
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
            FROM src
            """
            ,
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
        steps.update(1)

        steps.set_description("build_universe_membership_history:done")
        print(json.dumps({
            "table_name": "universe_membership_history",
            "rows_total": int(row[0]),
            "symbols_total": int(row[1]),
            "min_effective_from": str(row[2]) if row[2] is not None else None,
            "max_effective_to_or_open": str(row[3]) if row[3] is not None else None,
            "mode": "strict_from_market_universe_history",
        }, indent=2))
        steps.update(1)
        steps.close()
        return 0
    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
