#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

import duckdb


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Bootstrap research prerequisites from canonical production tables."
    )
    parser.add_argument("--db-path", required=True, help="Path to DuckDB database file.")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging.")
    return parser.parse_args()


def table_exists(con: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    row = con.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_schema = 'main'
          AND table_name = ?
        """,
        [table_name],
    ).fetchone()
    return bool(row and row[0] > 0)


def column_exists(con: duckdb.DuckDBPyConnection, table_name: str, column_name: str) -> bool:
    row = con.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.columns
        WHERE table_schema = 'main'
          AND table_name = ?
          AND column_name = ?
        """,
        [table_name, column_name],
    ).fetchone()
    return bool(row and row[0] > 0)


def main() -> int:
    args = parse_args()
    db_path = Path(args.db_path).expanduser().resolve()
    con = duckdb.connect(str(db_path))

    # ------------------------------------------------------------------
    # 1) price_history -> price_bars_adjusted
    # ------------------------------------------------------------------
    if not table_exists(con, "price_bars_adjusted"):
        raise SystemExit("missing table price_bars_adjusted; run init_feature_engine_foundation first")

    con.execute("DELETE FROM price_bars_adjusted")

    price_columns = {row[0] for row in con.execute("DESCRIBE price_bars_adjusted").fetchall()}

    # Mapping minimal et robuste vers le schéma research historique.
    select_parts = [
        "symbol",
        "price_date AS bar_date",
        "open",
        "high",
        "low",
        "close",
        "volume",
    ]

    if "adjusted_close" in price_columns:
        select_parts.append("close AS adjusted_close")
    if "source_name" in price_columns:
        select_parts.append("source_name")
    if "created_at" in price_columns:
        select_parts.append("CURRENT_TIMESTAMP AS created_at")
    if "ingested_at" in price_columns:
        select_parts.append("ingested_at")

    insert_cols = [row[0] for row in con.execute("DESCRIBE price_bars_adjusted").fetchall()]
    select_sql = ",\n                ".join(select_parts)
    insert_sql = ", ".join(insert_cols)

    con.execute(
        f"""
        INSERT INTO price_bars_adjusted ({insert_sql})
        SELECT
            {select_sql}
        FROM price_history
        """
    )

    # ------------------------------------------------------------------
    # 2) market_universe -> research_universe
    # ------------------------------------------------------------------
    if not table_exists(con, "research_universe"):
        raise SystemExit("missing table research_universe; initialize research foundations first")

    con.execute("DELETE FROM research_universe")

    research_cols = {row[0] for row in con.execute("DESCRIBE research_universe").fetchall()}

    ru_insert_cols = []
    ru_select_cols = []

    mapping = {
        "instrument_id": "COALESCE(sr.symbol, mu.symbol) AS instrument_id",
        "company_id": "COALESCE(sr.cik, mu.cik, mu.symbol) AS company_id",
        "symbol": "mu.symbol",
        "universe_name": "'research_default' AS universe_name",
        "effective_date": "mu.as_of_date AS effective_date",
        "membership_status": "'active' AS membership_status",
        "reason": "NULL AS reason",
        "source_name": "COALESCE(mu.source_name, 'bootstrap_research_inputs') AS source_name",
        "created_at": "CURRENT_TIMESTAMP AS created_at",
    }

    for col in mapping:
        if col in research_cols:
            ru_insert_cols.append(col)
            ru_select_cols.append(mapping[col])

    con.execute(
        f"""
        INSERT INTO research_universe ({", ".join(ru_insert_cols)})
        SELECT
            {", ".join(ru_select_cols)}
        FROM market_universe mu
        LEFT JOIN symbol_reference sr
          ON UPPER(TRIM(sr.symbol)) = UPPER(TRIM(mu.symbol))
        WHERE mu.include_in_universe = TRUE
        """
    )

    # ------------------------------------------------------------------
    # 3) research_universe -> universe_membership_history
    # ------------------------------------------------------------------
    if not table_exists(con, "universe_membership_history"):
        raise SystemExit("missing table universe_membership_history")

    con.execute("DELETE FROM universe_membership_history")

    umh_cols = {row[0] for row in con.execute("DESCRIBE universe_membership_history").fetchall()}

    umh_insert_cols = []
    umh_select_cols = []

    umh_mapping = {
        "instrument_id": "instrument_id",
        "company_id": "company_id",
        "symbol": "symbol",
        "universe_name": "universe_name",
        "effective_from": "COALESCE(effective_date, CURRENT_DATE) AS effective_from",
        "effective_to": "NULL AS effective_to",
        "membership_status": "COALESCE(membership_status, 'active') AS membership_status",
        "reason": "reason",
        "source_name": "COALESCE(source_name, 'bootstrap_research_inputs') AS source_name",
        "created_at": "CURRENT_TIMESTAMP AS created_at",
    }

    for col in umh_mapping:
        if col in umh_cols:
            umh_insert_cols.append(col)
            umh_select_cols.append(umh_mapping[col])

    con.execute(
        f"""
        INSERT INTO universe_membership_history ({", ".join(umh_insert_cols)})
        SELECT
            {", ".join(umh_select_cols)}
        FROM research_universe
        """
    )

    if args.verbose:
        print(f"[bootstrap_research_inputs] db_path={db_path}")
        for table_name in [
            "price_history",
            "price_bars_adjusted",
            "research_universe",
            "universe_membership_history",
        ]:
            count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            print(f"[bootstrap_research_inputs] {table_name} rows={count}")

    con.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
