"""
Builder de la couche raw canonique Yahoo-only / raw-only.

Objectif:
- créer / peupler price_source_daily_raw à partir de la meilleure source raw disponible
- priorité:
    1. price_source_daily_raw_yahoo si peuplée
    2. price_source_daily_raw_all si peuplée
- ne PAS retomber automatiquement sur price_history, car price_history peut être
  un mini sous-ensemble fixture et non la vraie source raw complète
"""

from __future__ import annotations

import json
import duckdb


def table_exists(connection: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    row = connection.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
          AND table_name = ?
        """,
        [table_name],
    ).fetchone()
    return bool(row and row[0] > 0)


def table_row_count(connection: duckdb.DuckDBPyConnection, table_name: str) -> int:
    if not table_exists(connection, table_name):
        return 0
    row = connection.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()
    return int(row[0]) if row else 0


def build_price_source_daily_raw(
    connection: duckdb.DuckDBPyConnection,
) -> dict[str, object]:
    """
    Construit price_source_daily_raw à partir de la meilleure source raw disponible.
    """
    yahoo_rows = table_row_count(connection, "price_source_daily_raw_yahoo")
    all_rows = table_row_count(connection, "price_source_daily_raw_all")

    connection.execute("DROP TABLE IF EXISTS price_source_daily_raw")

    if yahoo_rows > 0:
        source_sql = """
        SELECT
            symbol,
            price_date,
            open,
            high,
            low,
            close,
            close AS adj_close,
            volume,
            source_name,
            ingested_at
        FROM price_source_daily_raw_yahoo
        """
        source_mode = "price_source_daily_raw_yahoo"
    elif all_rows > 0:
        source_sql = """
        SELECT
            symbol,
            price_date,
            open,
            high,
            low,
            close,
            close AS adj_close,
            volume,
            source_name,
            ingested_at
        FROM price_source_daily_raw_all
        """
        source_mode = "price_source_daily_raw_all"
    else:
        raise RuntimeError(
            "No usable raw price source found. Expected populated "
            "price_source_daily_raw_yahoo or price_source_daily_raw_all."
        )

    connection.execute(
        f"""
        CREATE TABLE price_source_daily_raw AS
        SELECT
            UPPER(TRIM(CAST(symbol AS VARCHAR))) AS symbol,
            CAST(price_date AS DATE) AS price_date,
            CAST(open AS DOUBLE) AS open,
            CAST(high AS DOUBLE) AS high,
            CAST(low AS DOUBLE) AS low,
            CAST(close AS DOUBLE) AS close,
            CAST(adj_close AS DOUBLE) AS adj_close,
            CAST(volume AS BIGINT) AS volume,
            CAST(source_name AS VARCHAR) AS source_name,
            CAST(ingested_at AS TIMESTAMP) AS ingested_at
        FROM (
            {source_sql}
        ) src
        WHERE symbol IS NOT NULL
          AND price_date IS NOT NULL
          AND close IS NOT NULL
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY symbol, price_date
            ORDER BY ingested_at DESC NULLS LAST
        ) = 1
        """
    )

    metrics_row = connection.execute(
        """
        SELECT
            COUNT(*) AS row_count,
            MIN(price_date) AS min_price_date,
            MAX(price_date) AS max_price_date,
            COUNT(DISTINCT symbol) AS symbol_count
        FROM price_source_daily_raw
        """
    ).fetchone()

    return {
        "table_name": "price_source_daily_raw",
        "source_mode": source_mode,
        "row_count": metrics_row[0],
        "min_price_date": str(metrics_row[1]) if metrics_row[1] is not None else None,
        "max_price_date": str(metrics_row[2]) if metrics_row[2] is not None else None,
        "symbol_count": metrics_row[3],
    }


def main() -> None:
    connection = duckdb.connect("market.duckdb")
    try:
        metrics = build_price_source_daily_raw(connection)
        print(json.dumps(metrics, indent=2))
    finally:
        connection.close()


if __name__ == "__main__":
    main()
