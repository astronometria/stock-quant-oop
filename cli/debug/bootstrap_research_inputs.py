#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

import duckdb


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Bootstrap research prerequisites from existing canonical tables."
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


def get_columns(con: duckdb.DuckDBPyConnection, table_name: str) -> list[str]:
    rows = con.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'main'
          AND table_name = ?
        ORDER BY ordinal_position
        """,
        [table_name],
    ).fetchall()
    return [row[0] for row in rows]


def has_column(con: duckdb.DuckDBPyConnection, table_name: str, column_name: str) -> bool:
    return column_name in get_columns(con, table_name)


def scalar(con: duckdb.DuckDBPyConnection, sql: str) -> int:
    row = con.execute(sql).fetchone()
    return int(row[0]) if row and row[0] is not None else 0


def bootstrap_price_bars_adjusted(con: duckdb.DuckDBPyConnection) -> None:
    if not table_exists(con, "price_bars_adjusted"):
        raise RuntimeError("missing table price_bars_adjusted")

    if not table_exists(con, "price_history"):
        raise RuntimeError("missing table price_history")

    con.execute("DELETE FROM price_bars_adjusted")

    source_cols = set(get_columns(con, "price_history"))
    target_cols = get_columns(con, "price_bars_adjusted")

    expressions: dict[str, str] = {
        "symbol": "symbol",
        "bar_date": "price_date",
        "price_date": "price_date",
        "open": "open",
        "high": "high",
        "low": "low",
        "close": "close",
        "adjusted_close": "close",
        "volume": "volume",
        "source_name": "source_name" if "source_name" in source_cols else "'bootstrap_research_inputs'",
        "ingested_at": "ingested_at" if "ingested_at" in source_cols else "CURRENT_TIMESTAMP",
        "created_at": "CURRENT_TIMESTAMP",
        "updated_at": "CURRENT_TIMESTAMP",
    }

    insert_cols: list[str] = []
    select_exprs: list[str] = []

    for col in target_cols:
        if col in expressions:
            insert_cols.append(col)
            select_exprs.append(f"{expressions[col]} AS {col}")
        else:
            # colonne inconnue du bridge actuel : on la laisse à NULL
            insert_cols.append(col)
            select_exprs.append(f"NULL AS {col}")

    con.execute(
        f"""
        INSERT INTO price_bars_adjusted ({", ".join(insert_cols)})
        SELECT
            {", ".join(select_exprs)}
        FROM price_history
        """
    )


def bootstrap_research_universe(con: duckdb.DuckDBPyConnection) -> None:
    if not table_exists(con, "research_universe"):
        raise RuntimeError("missing table research_universe")
    if not table_exists(con, "market_universe"):
        raise RuntimeError("missing table market_universe")

    con.execute("DELETE FROM research_universe")

    target_cols = get_columns(con, "research_universe")
    source_cols = set(get_columns(con, "market_universe"))

    expressions: dict[str, str] = {
        "symbol": "symbol",
        "venue_group": "COALESCE(exchange_normalized, exchange_raw, 'UNKNOWN')",
        "asset_class": "COALESCE(security_type, 'UNKNOWN')",
        "is_adr": "COALESCE(is_adr, FALSE)",
        "is_etf": "COALESCE(is_etf, FALSE)",
        "is_common_stock": "COALESCE(is_common_stock, FALSE)",
        "include_in_research": "TRUE",
        "survivor_bias_aware": "TRUE",
        "manual_override_include": "FALSE",
        "manual_override_reason": "NULL",
        "created_at": "CURRENT_TIMESTAMP",
    }

    insert_cols: list[str] = []
    select_exprs: list[str] = []

    for col in target_cols:
        if col in expressions:
            insert_cols.append(col)
            select_exprs.append(f"{expressions[col]} AS {col}")
        else:
            insert_cols.append(col)
            select_exprs.append(f"NULL AS {col}")

    con.execute(
        f"""
        INSERT INTO research_universe ({", ".join(insert_cols)})
        SELECT
            {", ".join(select_exprs)}
        FROM market_universe
        WHERE include_in_universe = TRUE
        """
    )


def bootstrap_universe_membership_history(con: duckdb.DuckDBPyConnection) -> None:
    if not table_exists(con, "universe_membership_history"):
        raise RuntimeError("missing table universe_membership_history")
    if not table_exists(con, "research_universe"):
        raise RuntimeError("missing table research_universe")
    if not table_exists(con, "symbol_reference"):
        raise RuntimeError("missing table symbol_reference")

    con.execute("DELETE FROM universe_membership_history")

    target_cols = get_columns(con, "universe_membership_history")

    expressions: dict[str, str] = {
        "instrument_id": "COALESCE(sr.symbol, ru.symbol)",
        "company_id": "COALESCE(sr.cik, ru.symbol)",
        "symbol": "ru.symbol",
        "universe_name": "'research_default'",
        "effective_from": "CURRENT_DATE",
        "effective_to": "NULL",
        "membership_status": "'active'",
        "reason": "'bootstrap_research_inputs'",
        "source_name": "'bootstrap_research_inputs'",
        "created_at": "CURRENT_TIMESTAMP",
    }

    insert_cols: list[str] = []
    select_exprs: list[str] = []

    for col in target_cols:
        if col in expressions:
            insert_cols.append(col)
            select_exprs.append(f"{expressions[col]} AS {col}")
        else:
            insert_cols.append(col)
            select_exprs.append(f"NULL AS {col}")

    con.execute(
        f"""
        INSERT INTO universe_membership_history ({", ".join(insert_cols)})
        SELECT
            {", ".join(select_exprs)}
        FROM research_universe ru
        LEFT JOIN symbol_reference sr
          ON UPPER(TRIM(sr.symbol)) = UPPER(TRIM(ru.symbol))
        """
    )


def main() -> int:
    args = parse_args()
    db_path = Path(args.db_path).expanduser().resolve()
    con = duckdb.connect(str(db_path))

    bootstrap_price_bars_adjusted(con)
    bootstrap_research_universe(con)
    bootstrap_universe_membership_history(con)

    if args.verbose:
        for table_name in [
            "price_history",
            "price_bars_adjusted",
            "research_universe",
            "universe_membership_history",
        ]:
            print(f"[bootstrap_research_inputs] {table_name} rows={scalar(con, f'SELECT COUNT(*) FROM {table_name}')}")
    con.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
