#!/usr/bin/env python3
"""
Builder modulaire pour feature_price_momentum_daily.

Version corrigée:
- source canonique depuis price_bars_unadjusted / price_bars_adjusted / instrument_master
- pas de nested window functions
- RSI calculé en plusieurs étapes SQL compatibles DuckDB
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb

from stock_quant.features.price_momentum.registry import ALL_SPECS


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build modular feature_price_momentum_daily table."
    )
    parser.add_argument(
        "--db-path",
        default="market.duckdb",
        help="Path to DuckDB database file.",
    )
    return parser.parse_args()


def table_exists(connection: duckdb.DuckDBPyConnection, table_name: str) -> bool:
    row = connection.execute(
        f"""
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
          AND table_name = '{table_name}'
        """
    ).fetchone()
    return bool(row and row[0] > 0)


def ensure_required_tables_exist(connection: duckdb.DuckDBPyConnection) -> None:
    required_tables = [
        "price_bars_unadjusted",
        "price_bars_adjusted",
        "instrument_master",
    ]
    missing = [name for name in required_tables if not table_exists(connection, name)]
    if missing:
        raise RuntimeError(f"Missing required source tables: {missing}")


def get_table_columns(
    connection: duckdb.DuckDBPyConnection,
    table_name: str,
) -> set[str]:
    rows = connection.execute(
        f"""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
          AND table_name = '{table_name}'
        """
    ).fetchall()
    return {row[0] for row in rows}


def ensure_required_columns_exist(
    connection: duckdb.DuckDBPyConnection,
    table_name: str,
    required_columns: set[str],
) -> None:
    existing = get_table_columns(connection, table_name)
    missing = sorted(required_columns - existing)
    if missing:
        raise RuntimeError(f"Missing required columns in {table_name}: {missing}")


def ensure_required_schema(connection: duckdb.DuckDBPyConnection) -> None:
    ensure_required_tables_exist(connection)

    ensure_required_columns_exist(
        connection=connection,
        table_name="price_bars_unadjusted",
        required_columns={
            "instrument_id",
            "symbol",
            "bar_date",
            "open",
            "high",
            "low",
            "close",
            "volume",
        },
    )

    ensure_required_columns_exist(
        connection=connection,
        table_name="price_bars_adjusted",
        required_columns={
            "instrument_id",
            "symbol",
            "bar_date",
            "adj_close",
        },
    )

    ensure_required_columns_exist(
        connection=connection,
        table_name="instrument_master",
        required_columns={
            "instrument_id",
            "company_id",
            "symbol",
        },
    )


def collect_required_columns_for_base_source() -> set[str]:
    required = {
        "instrument_id",
        "company_id",
        "symbol",
        "as_of_date",
        "close",
    }
    for spec in ALL_SPECS:
        required.update(spec.required_input_columns)
    return required


def build_dynamic_indicator_sql() -> str:
    expressions: list[str] = []
    for spec in ALL_SPECS:
        for expression in spec.sql_select_expressions:
            expressions.append(expression)
    return ",\n        ".join(expressions)


def build_table(connection: duckdb.DuckDBPyConnection) -> dict[str, object]:
    ensure_required_schema(connection)

    produced_base_columns = {
        "instrument_id",
        "company_id",
        "symbol",
        "as_of_date",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "returns_1d",
        "returns_5d",
        "returns_20d",
        "rsi_14",
    }
    missing_from_builder_contract = sorted(
        collect_required_columns_for_base_source() - produced_base_columns
    )
    if missing_from_builder_contract:
        raise RuntimeError(
            "The modular momentum specs require columns not yet produced by "
            f"this builder contract: {missing_from_builder_contract}"
        )

    dynamic_indicator_sql = build_dynamic_indicator_sql()

    connection.execute("DROP TABLE IF EXISTS feature_price_momentum_daily")

    sql = f"""
    CREATE TABLE feature_price_momentum_daily AS
    WITH canonical_prices AS (
        SELECT
            u.instrument_id AS instrument_id,
            im.company_id AS company_id,
            u.symbol AS symbol,
            u.bar_date AS as_of_date,
            u.open AS open,
            u.high AS high,
            u.low AS low,
            COALESCE(a.adj_close, u.close) AS close,
            u.volume AS volume
        FROM price_bars_unadjusted u
        LEFT JOIN price_bars_adjusted a
            ON u.instrument_id = a.instrument_id
           AND u.symbol = a.symbol
           AND u.bar_date = a.bar_date
        LEFT JOIN instrument_master im
            ON u.instrument_id = im.instrument_id
    ),

    lagged AS (
        SELECT
            instrument_id,
            company_id,
            symbol,
            as_of_date,
            open,
            high,
            low,
            close,
            volume,

            LAG(close, 1) OVER price_w AS prev_close,
            LAG(close, 5) OVER price_w AS close_lag_5,
            LAG(close, 20) OVER price_w AS close_lag_20

        FROM canonical_prices
        WINDOW price_w AS (
            PARTITION BY symbol
            ORDER BY as_of_date
        )
    ),

    returns_stage AS (
        SELECT
            instrument_id,
            company_id,
            symbol,
            as_of_date,
            open,
            high,
            low,
            close,
            volume,
            prev_close,

            CASE
                WHEN prev_close IS NULL OR prev_close = 0 THEN NULL
                ELSE (close / prev_close) - 1
            END AS returns_1d,

            CASE
                WHEN close_lag_5 IS NULL OR close_lag_5 = 0 THEN NULL
                ELSE (close / close_lag_5) - 1
            END AS returns_5d,

            CASE
                WHEN close_lag_20 IS NULL OR close_lag_20 = 0 THEN NULL
                ELSE (close / close_lag_20) - 1
            END AS returns_20d,

            CASE
                WHEN prev_close IS NULL THEN NULL
                ELSE GREATEST(close - prev_close, 0)
            END AS gain_1d,

            CASE
                WHEN prev_close IS NULL THEN NULL
                ELSE GREATEST(prev_close - close, 0)
            END AS loss_1d

        FROM lagged
    ),

    rsi_stage AS (
        SELECT
            instrument_id,
            company_id,
            symbol,
            as_of_date,
            open,
            high,
            low,
            close,
            volume,
            returns_1d,
            returns_5d,
            returns_20d,

            AVG(gain_1d) OVER (
                PARTITION BY symbol
                ORDER BY as_of_date
                ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
            ) AS avg_gain_14,

            AVG(loss_1d) OVER (
                PARTITION BY symbol
                ORDER BY as_of_date
                ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
            ) AS avg_loss_14

        FROM returns_stage
    ),

    enriched AS (
        SELECT
            instrument_id,
            company_id,
            symbol,
            as_of_date,
            close,
            high,
            low,
            returns_1d,
            returns_5d,
            returns_20d,

            CASE
                WHEN avg_loss_14 IS NULL THEN NULL
                WHEN avg_loss_14 = 0 AND avg_gain_14 = 0 THEN 50
                WHEN avg_loss_14 = 0 THEN 100
                ELSE 100 - (100 / (1 + (avg_gain_14 / avg_loss_14)))
            END AS rsi_14

        FROM rsi_stage
    )

    SELECT
        instrument_id,
        company_id,
        symbol,
        as_of_date,
        close,
        returns_1d,
        returns_5d,
        returns_20d,
        rsi_14,
        {dynamic_indicator_sql}
    FROM enriched
    WINDOW price_w AS (
        PARTITION BY symbol
        ORDER BY as_of_date
    )
    """

    connection.execute(sql)

    metrics_row = connection.execute(
        """
        SELECT
            COUNT(*) AS rows_written,
            MIN(as_of_date) AS min_date,
            MAX(as_of_date) AS max_date,
            COUNT(DISTINCT symbol) AS symbol_count,
            COUNT(DISTINCT instrument_id) AS instrument_count
        FROM feature_price_momentum_daily
        """
    ).fetchone()

    return {
        "table_name": "feature_price_momentum_daily",
        "rows_written": metrics_row[0],
        "min_date": str(metrics_row[1]) if metrics_row[1] is not None else None,
        "max_date": str(metrics_row[2]) if metrics_row[2] is not None else None,
        "symbol_count": metrics_row[3],
        "instrument_count": metrics_row[4],
        "source_contract": {
            "price_source_unadjusted": "price_bars_unadjusted",
            "price_source_adjusted": "price_bars_adjusted",
            "identity_source": "instrument_master",
            "date_column": "bar_date -> as_of_date",
            "close_rule": "COALESCE(adj_close, close)",
        },
        "indicators_loaded": [spec.name for spec in ALL_SPECS],
        "output_columns_added": [
            column_name
            for spec in ALL_SPECS
            for column_name in spec.output_columns
        ],
    }


def main() -> None:
    args = parse_args()
    db_path = Path(args.db_path)

    connection = duckdb.connect(str(db_path))
    try:
        metrics = build_table(connection)
        print(json.dumps(metrics, indent=2))
    finally:
        connection.close()


if __name__ == "__main__":
    main()
