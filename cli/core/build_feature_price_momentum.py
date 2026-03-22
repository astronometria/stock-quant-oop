#!/usr/bin/env python3
"""
Builder modulaire pour feature_price_momentum_daily.

Objectif du refactor
--------------------
Ce script ne doit plus devenir un gros fichier monolithique rempli
d'indicateurs hardcodés. Il doit rester un orchestrateur mince qui:

1. ouvre la base DuckDB
2. vérifie la présence des tables source minimales
3. charge le registre modulaire des indicateurs momentum
4. assemble dynamiquement les expressions SQL
5. construit / remplit feature_price_momentum_daily

Principes
---------
- garder la logique indicateur dans stock_quant.features.price_momentum.*
- garder ici surtout l'orchestration, les validations, et l'I/O SQL
- beaucoup de commentaires pour rendre le code facile à maintenir
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import duckdb

from stock_quant.features.price_momentum.registry import ALL_SPECS


def parse_args() -> argparse.Namespace:
    """
    Parse les arguments CLI.

    On garde une interface simple et stable pour rester compatible avec
    le reste du repo et les scripts existants.
    """
    parser = argparse.ArgumentParser(
        description="Build modular feature_price_momentum_daily table."
    )
    parser.add_argument(
        "--db-path",
        default="market.duckdb",
        help="Path to DuckDB database file.",
    )
    return parser.parse_args()


def ensure_source_table_exists(connection: duckdb.DuckDBPyConnection) -> None:
    """
    Vérifie que la table source utilisée par le builder existe.

    Ici on part de l'hypothèse que le repo alimente price_history.
    Si ton repo utilise un autre nom canonique plus tard, ce point sera
    facile à adapter à un seul endroit.
    """
    row = connection.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_name = 'price_history'
        """
    ).fetchone()

    if not row or row[0] == 0:
        raise RuntimeError(
            "Missing required source table: price_history"
        )


def ensure_required_columns_exist(
    connection: duckdb.DuckDBPyConnection,
    table_name: str,
    required_columns: set[str],
) -> None:
    """
    Vérifie que les colonnes requises existent dans la table source.

    C'est important pour échouer tôt et clairement si un indicateur
    modulaire demande une colonne absente.
    """
    rows = connection.execute(
        f"""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = '{table_name}'
        """
    ).fetchall()

    existing = {row[0] for row in rows}
    missing = sorted(required_columns - existing)

    if missing:
        raise RuntimeError(
            f"Missing required columns in {table_name}: {missing}"
        )


def collect_required_columns() -> set[str]:
    """
    Agrège toutes les colonnes nécessaires aux specs momentum.

    On ajoute aussi les colonnes d'identité minimales requises par la table
    de sortie.
    """
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
    """
    Construit le bloc SQL dynamique des indicateurs modulaires.

    Chaque spec publie une ou plusieurs expressions SQL déjà aliasées.
    On les concatène ici proprement.
    """
    expressions: list[str] = []

    for spec in ALL_SPECS:
        for expression in spec.sql_select_expressions:
            expressions.append(expression)

    # On indente légèrement pour garder un SQL lisible dans les logs/debug.
    return ",\n        ".join(expressions)


def build_table(connection: duckdb.DuckDBPyConnection) -> dict[str, object]:
    """
    Construit complètement la table feature_price_momentum_daily.

    Stratégie simple:
    - drop/create pour un premier refactor clair et robuste
    - on pourra optimiser en incrémental plus tard quand la structure sera stable
    """
    ensure_source_table_exists(connection)

    required_columns = collect_required_columns()
    ensure_required_columns_exist(
        connection=connection,
        table_name="price_history",
        required_columns=required_columns,
    )

    dynamic_indicator_sql = build_dynamic_indicator_sql()

    connection.execute("DROP TABLE IF EXISTS feature_price_momentum_daily")

    # Notes importantes:
    # - "price_w" est utilisé par plusieurs specs modulaires
    # - on calcule ici des colonnes de base déjà existantes dans l'ancien monde
    #   (returns_1d, returns_5d, returns_20d, rsi_14) + les nouvelles via registre
    # - cette version privilégie la clarté à l'optimisation maximale
    sql = f"""
    CREATE TABLE feature_price_momentum_daily AS
    WITH base AS (
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

            CASE
                WHEN LAG(close, 1) OVER price_w IS NULL
                  OR LAG(close, 1) OVER price_w = 0
                THEN NULL
                ELSE (close / LAG(close, 1) OVER price_w) - 1
            END AS returns_1d,

            CASE
                WHEN LAG(close, 5) OVER price_w IS NULL
                  OR LAG(close, 5) OVER price_w = 0
                THEN NULL
                ELSE (close / LAG(close, 5) OVER price_w) - 1
            END AS returns_5d,

            CASE
                WHEN LAG(close, 20) OVER price_w IS NULL
                  OR LAG(close, 20) OVER price_w = 0
                THEN NULL
                ELSE (close / LAG(close, 20) OVER price_w) - 1
            END AS returns_20d,

            AVG(
                GREATEST(close - LAG(close, 1) OVER price_w, 0)
            ) OVER (
                PARTITION BY symbol
                ORDER BY as_of_date
                ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
            ) AS avg_gain_14,

            AVG(
                GREATEST(LAG(close, 1) OVER price_w - close, 0)
            ) OVER (
                PARTITION BY symbol
                ORDER BY as_of_date
                ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
            ) AS avg_loss_14

        FROM price_history
        WINDOW price_w AS (
            PARTITION BY symbol
            ORDER BY as_of_date
        )
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

        FROM base
    )
    SELECT
        instrument_id,
        company_id,
        symbol,
        as_of_date,
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
            COUNT(DISTINCT symbol) AS symbol_count
        FROM feature_price_momentum_daily
        """
    ).fetchone()

    return {
        "table_name": "feature_price_momentum_daily",
        "rows_written": metrics_row[0],
        "min_date": str(metrics_row[1]) if metrics_row[1] is not None else None,
        "max_date": str(metrics_row[2]) if metrics_row[2] is not None else None,
        "symbol_count": metrics_row[3],
        "indicators_loaded": [spec.name for spec in ALL_SPECS],
        "output_columns_added": [
            column_name
            for spec in ALL_SPECS
            for column_name in spec.output_columns
        ],
    }


def main() -> None:
    """
    Point d'entrée principal du builder.
    """
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
