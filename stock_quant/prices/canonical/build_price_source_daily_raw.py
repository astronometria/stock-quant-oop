"""
Builder de la couche raw canonique Yahoo-only.

Objectif:
- créer / peupler price_source_daily_raw à partir de price_history
  ou d'une table raw Yahoo existante
- imposer un schéma brut propre et stable pour les étapes aval

Important:
- ce script ne calcule aucune feature
- ce script ne dépend que d'une seule source provider: Yahoo
- ce script garde la logique SQL-first
"""

from __future__ import annotations

import json

import duckdb


def build_price_source_daily_raw(
    connection: duckdb.DuckDBPyConnection,
) -> dict[str, object]:
    """
    Construit price_source_daily_raw.

    Stratégie actuelle:
    - si la table price_source_daily_raw_yahoo existe et contient des lignes,
      elle devient la source prioritaire
    - sinon on retombe sur price_history comme source intermédiaire legacy,
      uniquement pour réaligner le schéma dans la nouvelle canonical layer

    Cette approche permet de corriger progressivement la situation du repo
    sans casser les pipelines existants.
    """
    yahoo_exists = connection.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
          AND table_name = 'price_source_daily_raw_yahoo'
        """
    ).fetchone()[0] > 0

    yahoo_rows = 0
    if yahoo_exists:
        yahoo_rows = connection.execute(
            "SELECT COUNT(*) FROM price_source_daily_raw_yahoo"
        ).fetchone()[0]

    price_history_exists = connection.execute(
        """
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
          AND table_name = 'price_history'
        """
    ).fetchone()[0] > 0

    price_history_rows = 0
    if price_history_exists:
        price_history_rows = connection.execute(
            "SELECT COUNT(*) FROM price_history"
        ).fetchone()[0]

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
    elif price_history_rows > 0:
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
        FROM price_history
        """
        source_mode = "price_history_legacy_bridge"
    else:
        raise RuntimeError(
            "No usable Yahoo raw price source found. "
            "Expected price_source_daily_raw_yahoo or price_history."
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
    """
    Exécution standalone pratique pour debug ponctuel.
    """
    connection = duckdb.connect("market.duckdb")
    try:
        metrics = build_price_source_daily_raw(connection)
        print(json.dumps(metrics, indent=2))
    finally:
        connection.close()


if __name__ == "__main__":
    main()
