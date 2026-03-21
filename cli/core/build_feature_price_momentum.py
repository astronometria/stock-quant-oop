#!/usr/bin/env python3
from __future__ import annotations

"""
Build feature_price_momentum_daily incrementally.

Objectif
--------
Construire / rafraîchir la table canonique des features momentum prix
à partir de `price_history` (ou `price_bars_adjusted` si disponible).

Pourquoi incrémental
--------------------
Le fichier précédent faisait un:
- DROP TABLE
- CREATE TABLE AS SELECT ...

Ce pattern rebuild tout l'historique à chaque run, ce qui devient inutilement
coûteux au quotidien.

Nouveau pattern
---------------
- on garde la table cible
- on calcule une fenêtre de recouvrement
- on relit une petite fenêtre source plus large pour que les LAG / RSI restent justes
- on DELETE seulement la fenêtre cible
- on INSERT seulement les lignes recalculées

Fenêtres
--------
Momentum a besoin:
- returns_1d
- returns_5d
- returns_20d
- RSI 14

Donc:
- overlap_days cible = 45
- source_lookback_days = 30
"""

import argparse
import json
from pathlib import Path

import duckdb


# Fenêtre cible retraitée à chaque run.
TARGET_OVERLAP_DAYS = 45

# Fenêtre source supplémentaire nécessaire pour calculer correctement les indicateurs
# au début de la fenêtre cible.
SOURCE_LOOKBACK_DAYS = 30


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--db-path", required=True)
    p.add_argument("--memory-limit", default="24GB")
    p.add_argument("--threads", type=int, default=4)
    p.add_argument("--temp-dir", default="/home/marty/stock-quant-oop/tmp")
    p.add_argument("--verbose", action="store_true")
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
    return bool(row and int(row[0]) > 0)


def table_count(con: duckdb.DuckDBPyConnection, table_name: str) -> int:
    return int(con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0])


def get_columns(con: duckdb.DuckDBPyConnection, table_name: str) -> set[str]:
    return {
        str(row[1]).strip()
        for row in con.execute(f"PRAGMA table_info('{table_name}')").fetchall()
    }


def pick_first(existing: set[str], candidates: list[str], label: str) -> str:
    for candidate in candidates:
        if candidate in existing:
            return candidate
    raise RuntimeError(
        f"Could not find a suitable {label} column. "
        f"Available columns: {sorted(existing)}; candidates tried: {candidates}"
    )


def ensure_target_table(con: duckdb.DuckDBPyConnection) -> None:
    """
    Crée la table cible si elle n'existe pas encore.
    Le schéma reste compatible avec l'existant du projet.
    """
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS feature_price_momentum_daily (
            instrument_id VARCHAR,
            symbol VARCHAR,
            as_of_date DATE,
            close DOUBLE,
            returns_1d DOUBLE,
            returns_5d DOUBLE,
            returns_20d DOUBLE,
            rsi_14 DOUBLE,
            source_name VARCHAR,
            created_at TIMESTAMP WITH TIME ZONE
        )
        """
    )


def main() -> int:
    args = parse_args()
    db_path = Path(args.db_path).expanduser().resolve()

    con = duckdb.connect(str(db_path))
    try:
        con.execute(f"PRAGMA memory_limit='{args.memory_limit}'")
        con.execute(f"PRAGMA threads={int(args.threads)}")
        con.execute("PRAGMA preserve_insertion_order=false")

        temp_dir_sql = str(Path(args.temp_dir).expanduser().resolve()).replace("'", "''")
        con.execute(f"PRAGMA temp_directory='{temp_dir_sql}'")

        ensure_target_table(con)

        # ------------------------------------------------------------------
        # Déterminer la meilleure table source prix.
        # ------------------------------------------------------------------
        if table_exists(con, "price_bars_adjusted"):
            adjusted_rows = int(con.execute("SELECT COUNT(*) FROM price_bars_adjusted").fetchone()[0])
            if adjusted_rows > 0:
                price_table = "price_bars_adjusted"
            elif table_exists(con, "price_history"):
                price_table = "price_history"
            else:
                raise RuntimeError(
                    "price_bars_adjusted exists but is empty, and price_history does not exist"
                )
        elif table_exists(con, "price_history"):
            price_table = "price_history"
        else:
            raise RuntimeError("Neither price_bars_adjusted nor price_history exists")

        price_cols = get_columns(con, price_table)
        symbol_col = pick_first(price_cols, ["symbol"], "symbol")
        date_col = pick_first(price_cols, ["bar_date", "price_date", "as_of_date", "date"], "date")
        close_col = pick_first(price_cols, ["adj_close", "close", "adjusted_close"], "close")
        instrument_id_col = "instrument_id" if "instrument_id" in price_cols else None

        target_rows_before = table_count(con, "feature_price_momentum_daily")
        max_target_date = con.execute(
            "SELECT MAX(as_of_date) FROM feature_price_momentum_daily"
        ).fetchone()[0]

        # Si la table est vide: full build depuis le début.
        # Sinon: on retraite une fenêtre récente.
        con.execute("DROP TABLE IF EXISTS tmp_feature_price_momentum_window")
        con.execute(
            f"""
            CREATE TEMP TABLE tmp_feature_price_momentum_window AS
            SELECT
                CASE
                    WHEN ? IS NULL THEN DATE '1900-01-01'
                    ELSE CAST(? - INTERVAL '{TARGET_OVERLAP_DAYS} days' AS DATE)
                END AS target_window_start,
                CASE
                    WHEN ? IS NULL THEN DATE '1900-01-01'
                    ELSE CAST(? - INTERVAL '{TARGET_OVERLAP_DAYS + SOURCE_LOOKBACK_DAYS} days' AS DATE)
                END AS source_window_start
            """,
            [max_target_date, max_target_date, max_target_date, max_target_date],
        )

        target_window_start, source_window_start = con.execute(
            """
            SELECT target_window_start, source_window_start
            FROM tmp_feature_price_momentum_window
            """
        ).fetchone()

        if args.verbose:
            print(f"[momentum] price_table={price_table}", flush=True)
            if table_exists(con, "price_bars_adjusted"):
                adjusted_rows = int(con.execute("SELECT COUNT(*) FROM price_bars_adjusted").fetchone()[0])
                print(f"[momentum] price_bars_adjusted_rows={adjusted_rows}", flush=True)
            if table_exists(con, "price_history"):
                history_rows = int(con.execute("SELECT COUNT(*) FROM price_history").fetchone()[0])
                print(f"[momentum] price_history_rows={history_rows}", flush=True)
            print(f"[momentum] symbol_col={symbol_col}", flush=True)
            print(f"[momentum] date_col={date_col}", flush=True)
            print(f"[momentum] close_col={close_col}", flush=True)
            print(f"[momentum] instrument_id_col={instrument_id_col}", flush=True)
            print(f"[momentum] target_rows_before={target_rows_before}", flush=True)
            print(f"[momentum] max_target_date={max_target_date}", flush=True)
            print(f"[momentum] target_window_start={target_window_start}", flush=True)
            print(f"[momentum] source_window_start={source_window_start}", flush=True)

        instrument_id_sql = (
            f"CAST(p.{instrument_id_col} AS VARCHAR) AS instrument_id"
            if instrument_id_col
            else "NULL::VARCHAR AS instrument_id"
        )

        # ------------------------------------------------------------------
        # Stage incrémental recalculé.
        # On relit une fenêtre source un peu plus large pour les LAG / RSI.
        # Puis on ne garde dans le stage final que la fenêtre cible.
        # ------------------------------------------------------------------
        con.execute("DROP TABLE IF EXISTS tmp_feature_price_momentum_stage")
        con.execute(
            f"""
            CREATE TEMP TABLE tmp_feature_price_momentum_stage AS
            WITH price_base AS (
                SELECT
                    {instrument_id_sql},
                    CAST(p.{symbol_col} AS VARCHAR) AS symbol,
                    CAST(p.{date_col} AS DATE) AS as_of_date,
                    CAST(p.{close_col} AS DOUBLE) AS close
                FROM {price_table} p
                WHERE p.{symbol_col} IS NOT NULL
                  AND p.{date_col} IS NOT NULL
                  AND p.{close_col} IS NOT NULL
                  AND CAST(p.{date_col} AS DATE) >= CAST(? AS DATE)
            ),
            ordered AS (
                SELECT
                    instrument_id,
                    symbol,
                    as_of_date,
                    close,
                    LAG(close, 1) OVER (PARTITION BY symbol ORDER BY as_of_date) AS close_lag_1,
                    LAG(close, 5) OVER (PARTITION BY symbol ORDER BY as_of_date) AS close_lag_5,
                    LAG(close, 20) OVER (PARTITION BY symbol ORDER BY as_of_date) AS close_lag_20,
                    close - LAG(close, 1) OVER (PARTITION BY symbol ORDER BY as_of_date) AS delta
                FROM price_base
            ),
            enriched AS (
                SELECT
                    instrument_id,
                    symbol,
                    as_of_date,
                    close,
                    CASE
                        WHEN close_lag_1 IS NULL OR close_lag_1 = 0 THEN NULL
                        ELSE (close / close_lag_1) - 1
                    END AS returns_1d,
                    CASE
                        WHEN close_lag_5 IS NULL OR close_lag_5 = 0 THEN NULL
                        ELSE (close / close_lag_5) - 1
                    END AS returns_5d,
                    CASE
                        WHEN close_lag_20 IS NULL OR close_lag_20 = 0 THEN NULL
                        ELSE (close / close_lag_20) - 1
                    END AS returns_20d,
                    CASE WHEN delta > 0 THEN delta ELSE 0 END AS gain,
                    CASE WHEN delta < 0 THEN -delta ELSE 0 END AS loss
                FROM ordered
            ),
            final_calc AS (
                SELECT
                    instrument_id,
                    symbol,
                    as_of_date,
                    close,
                    returns_1d,
                    returns_5d,
                    returns_20d,
                    CASE
                        WHEN AVG(loss) OVER (
                            PARTITION BY symbol
                            ORDER BY as_of_date
                            ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
                        ) IS NULL
                         OR AVG(gain) OVER (
                            PARTITION BY symbol
                            ORDER BY as_of_date
                            ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
                        ) IS NULL
                        THEN NULL
                        WHEN AVG(loss) OVER (
                            PARTITION BY symbol
                            ORDER BY as_of_date
                            ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
                        ) = 0
                         AND AVG(gain) OVER (
                            PARTITION BY symbol
                            ORDER BY as_of_date
                            ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
                        ) = 0
                        THEN 50.0
                        WHEN AVG(loss) OVER (
                            PARTITION BY symbol
                            ORDER BY as_of_date
                            ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
                        ) = 0
                        THEN 100.0
                        ELSE 100 - (
                            100 / (
                                1 + (
                                    AVG(gain) OVER (
                                        PARTITION BY symbol
                                        ORDER BY as_of_date
                                        ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
                                    ) / NULLIF(
                                        AVG(loss) OVER (
                                            PARTITION BY symbol
                                            ORDER BY as_of_date
                                            ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
                                        ),
                                        0
                                    )
                                )
                            )
                        )
                    END AS rsi_14,
                    'build_feature_price_momentum.py' AS source_name,
                    CURRENT_TIMESTAMP AS created_at
                FROM enriched
            )
            SELECT
                instrument_id,
                symbol,
                as_of_date,
                close,
                returns_1d,
                returns_5d,
                returns_20d,
                rsi_14,
                source_name,
                created_at
            FROM final_calc
            WHERE as_of_date >= CAST(? AS DATE)
            """,
            [source_window_start, target_window_start],
        )

        stage_rows = table_count(con, "tmp_feature_price_momentum_stage")

        # ------------------------------------------------------------------
        # Delete + insert uniquement sur la fenêtre cible.
        # ------------------------------------------------------------------
        deleted_rows = con.execute(
            """
            DELETE FROM feature_price_momentum_daily
            WHERE as_of_date >= CAST(? AS DATE)
            """,
            [target_window_start],
        ).fetchone()[0]

        inserted_rows = con.execute(
            """
            INSERT INTO feature_price_momentum_daily (
                instrument_id,
                symbol,
                as_of_date,
                close,
                returns_1d,
                returns_5d,
                returns_20d,
                rsi_14,
                source_name,
                created_at
            )
            SELECT
                instrument_id,
                symbol,
                as_of_date,
                close,
                returns_1d,
                returns_5d,
                returns_20d,
                rsi_14,
                source_name,
                created_at
            FROM tmp_feature_price_momentum_stage
            """
        ).fetchone()[0]

        row = con.execute(
            """
            SELECT
                COUNT(*) AS total_rows,
                COUNT(returns_1d) AS returns_1d_rows,
                COUNT(returns_5d) AS returns_5d_rows,
                COUNT(returns_20d) AS returns_20d_rows,
                COUNT(rsi_14) AS rsi_14_rows,
                MIN(as_of_date) AS min_date,
                MAX(as_of_date) AS max_date
            FROM feature_price_momentum_daily
            """
        ).fetchone()

        print(
            json.dumps(
                {
                    "table_name": "feature_price_momentum_daily",
                    "rows": int(row[0]),
                    "returns_1d_rows": int(row[1]),
                    "returns_5d_rows": int(row[2]),
                    "returns_20d_rows": int(row[3]),
                    "rsi_14_rows": int(row[4]),
                    "min_date": str(row[5]) if row[5] is not None else None,
                    "max_date": str(row[6]) if row[6] is not None else None,
                    "target_rows_before": int(target_rows_before),
                    "target_window_start": str(target_window_start) if target_window_start is not None else None,
                    "source_window_start": str(source_window_start) if source_window_start is not None else None,
                    "stage_rows": int(stage_rows),
                    "deleted_rows": int(deleted_rows),
                    "inserted_rows": int(inserted_rows),
                },
                indent=2,
            ),
            flush=True,
        )

        return 0
    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
