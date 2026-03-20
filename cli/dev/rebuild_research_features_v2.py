#!/usr/bin/env python3
from __future__ import annotations

"""
Rebuild research_features_daily v2 (SQL-first, research-grade).

Objectif
--------
Créer une vraie couche de features calculées d'avance, indépendante du
train/valid/test, pour pouvoir ensuite consommer seulement les indicateurs
souhaités dans les recherches futures.

Batch v2 implémenté
-------------------
- close
- returns_1d
- returns_5d
- returns_20d
- sma_20
- sma_50
- sma_200
- close_to_sma_20
- rsi_14
- atr_14                  (si high/low disponibles dans la source prix)
- volatility_20

Notes de design
---------------
- SQL-first
- PIT-safe
- aucun lookahead
- gros volume traité dans DuckDB
- beaucoup de commentaires pour faciliter la maintenance
"""

import argparse
from pathlib import Path

import duckdb


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db-path", default="/home/marty/stock-quant-oop/market.duckdb")
    parser.add_argument("--memory-limit", default="24GB")
    parser.add_argument("--threads", type=int, default=6)
    parser.add_argument("--temp-dir", default="/home/marty/stock-quant-oop/tmp")
    parser.add_argument("--verbose", action="store_true")
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
    return bool(row and int(row[0]) > 0)


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


def pick_optional(existing: set[str], candidates: list[str]) -> str | None:
    for candidate in candidates:
        if candidate in existing:
            return candidate
    return None


def main() -> int:
    args = parse_args()
    db_path = Path(args.db_path).expanduser().resolve()

    con = duckdb.connect(str(db_path))
    try:
        # ------------------------------------------------------------------
        # Aligner ce script avec le reste du codebase research-grade.
        # ------------------------------------------------------------------
        con.execute(f"PRAGMA memory_limit='{args.memory_limit}'")
        con.execute(f"PRAGMA threads={int(args.threads)}")
        con.execute("PRAGMA preserve_insertion_order=false")

        temp_dir_path = Path(args.temp_dir).expanduser().resolve()
        temp_dir_path.mkdir(parents=True, exist_ok=True)
        temp_dir_sql = str(temp_dir_path).replace("'", "''")
        con.execute(f"PRAGMA temp_directory='{temp_dir_sql}'")

        # ------------------------------------------------------------------
        # Choix de la source prix.
        # Préférence: price_bars_adjusted si disponible, sinon price_history.
        # ------------------------------------------------------------------
        if table_exists(con, "price_bars_adjusted"):
            price_table = "price_bars_adjusted"
        elif table_exists(con, "price_history"):
            price_table = "price_history"
        else:
            raise RuntimeError("Neither price_bars_adjusted nor price_history exists")

        price_cols = get_columns(con, price_table)
        short_cols = get_columns(con, "short_features_daily") if table_exists(con, "short_features_daily") else set()
        instrument_master_exists = table_exists(con, "instrument_master")
        instrument_master_cols = get_columns(con, "instrument_master") if instrument_master_exists else set()

        # ------------------------------------------------------------------
        # Détection prudente des colonnes source.
        # ------------------------------------------------------------------
        symbol_col = pick_first(price_cols, ["symbol"], "price symbol")
        date_col = pick_first(price_cols, ["bar_date", "price_date", "as_of_date", "date", "trade_date"], "price date")
        close_col = pick_first(price_cols, ["adj_close", "close", "adjusted_close"], "price close")
        instrument_id_col = pick_optional(price_cols, ["instrument_id"])
        high_col = pick_optional(price_cols, ["high", "adj_high"])
        low_col = pick_optional(price_cols, ["low", "adj_low"])

        short_symbol_col = pick_optional(short_cols, ["symbol"])
        short_date_col = pick_optional(short_cols, ["as_of_date", "price_date", "bar_date", "date"])
        short_ratio_col = pick_optional(short_cols, ["short_volume_ratio"])
        short_interest_col = pick_optional(short_cols, ["short_interest"])
        days_to_cover_col = pick_optional(short_cols, ["days_to_cover"])
        short_interest_change_pct_col = pick_optional(short_cols, ["short_interest_change_pct"])
        short_squeeze_score_col = pick_optional(short_cols, ["short_squeeze_score"])
        short_pressure_zscore_col = pick_optional(short_cols, ["short_pressure_zscore"])
        days_to_cover_zscore_col = pick_optional(short_cols, ["days_to_cover_zscore"])

        company_join_sql = "NULL::VARCHAR AS company_id"
        if instrument_master_exists and instrument_id_col and "company_id" in instrument_master_cols:
            company_join_sql = "im.company_id AS company_id"

        if args.verbose:
            print(f"[rebuild_v2] db_path={db_path}", flush=True)
            print(f"[rebuild_v2] price_table={price_table}", flush=True)
            print(f"[rebuild_v2] symbol_col={symbol_col}", flush=True)
            print(f"[rebuild_v2] date_col={date_col}", flush=True)
            print(f"[rebuild_v2] close_col={close_col}", flush=True)
            print(f"[rebuild_v2] instrument_id_col={instrument_id_col}", flush=True)
            print(f"[rebuild_v2] high_col={high_col}", flush=True)
            print(f"[rebuild_v2] low_col={low_col}", flush=True)
            print(f"[rebuild_v2] short_features_available={bool(short_cols)}", flush=True)

        # ------------------------------------------------------------------
        # Préparation de quelques fragments SQL paramétrés par le schéma live.
        # ------------------------------------------------------------------
        instrument_id_sql = f"p.{instrument_id_col} AS instrument_id" if instrument_id_col else "NULL::VARCHAR AS instrument_id"
        instrument_master_join = (
            f"LEFT JOIN instrument_master im ON p.{instrument_id_col} = im.instrument_id"
            if instrument_master_exists and instrument_id_col and "company_id" in instrument_master_cols
            else ""
        )
        high_sql = f"CAST(p.{high_col} AS DOUBLE)" if high_col else "NULL::DOUBLE"
        low_sql = f"CAST(p.{low_col} AS DOUBLE)" if low_col else "NULL::DOUBLE"

        short_select_sql = ""
        short_join_sql = ""
        if short_cols and short_symbol_col and short_date_col:
            # On projette seulement les colonnes réellement présentes.
            short_interest_sql = f"CAST({short_interest_col} AS DOUBLE)" if short_interest_col else "NULL::DOUBLE"
            days_to_cover_sql = f"CAST({days_to_cover_col} AS DOUBLE)" if days_to_cover_col else "NULL::DOUBLE"
            short_ratio_sql = f"CAST({short_ratio_col} AS DOUBLE)" if short_ratio_col else "NULL::DOUBLE"
            short_interest_change_pct_sql = (
                f"CAST({short_interest_change_pct_col} AS DOUBLE)" if short_interest_change_pct_col else "NULL::DOUBLE"
            )
            short_squeeze_score_sql = (
                f"CAST({short_squeeze_score_col} AS DOUBLE)" if short_squeeze_score_col else "NULL::DOUBLE"
            )
            short_pressure_zscore_sql = (
                f"CAST({short_pressure_zscore_col} AS DOUBLE)" if short_pressure_zscore_col else "NULL::DOUBLE"
            )
            days_to_cover_zscore_sql = (
                f"CAST({days_to_cover_zscore_col} AS DOUBLE)" if days_to_cover_zscore_col else "NULL::DOUBLE"
            )

            short_select_sql = f"""
            , short_base AS (
                SELECT
                    CAST({short_symbol_col} AS VARCHAR) AS symbol,
                    CAST({short_date_col} AS DATE) AS as_of_date,
                    {short_interest_sql} AS short_interest,
                    {days_to_cover_sql} AS days_to_cover,
                    {short_ratio_sql} AS short_volume_ratio,
                    {short_interest_change_pct_sql} AS short_interest_change_pct,
                    {short_squeeze_score_sql} AS short_squeeze_score,
                    {short_pressure_zscore_sql} AS short_pressure_zscore,
                    {days_to_cover_zscore_sql} AS days_to_cover_zscore
                FROM short_features_daily
                WHERE {short_symbol_col} IS NOT NULL
                  AND {short_date_col} IS NOT NULL
            )
            """
            short_join_sql = """
            LEFT JOIN short_base s
              ON UPPER(TRIM(s.symbol)) = UPPER(TRIM(f.symbol))
             AND s.as_of_date = f.as_of_date
            """
        else:
            short_join_sql = ""

        print("[rebuild_v2] dropping research_features_daily...", flush=True)
        con.execute("DROP TABLE IF EXISTS research_features_daily")

        print("[rebuild_v2] creating research_features_daily...", flush=True)
        con.execute(
            f"""
            CREATE TABLE research_features_daily AS
            WITH price_base AS (
                SELECT
                    {instrument_id_sql},
                    {company_join_sql},
                    CAST(p.{symbol_col} AS VARCHAR) AS symbol,
                    CAST(p.{date_col} AS DATE) AS as_of_date,
                    CAST(p.{close_col} AS DOUBLE) AS close,
                    {high_sql} AS high_price,
                    {low_sql} AS low_price
                FROM {price_table} p
                {instrument_master_join}
                WHERE p.{symbol_col} IS NOT NULL
                  AND p.{date_col} IS NOT NULL
                  AND p.{close_col} IS NOT NULL
            ),
            ordered AS (
                SELECT
                    instrument_id,
                    company_id,
                    symbol,
                    as_of_date,
                    close,
                    high_price,
                    low_price,
                    LAG(close, 1) OVER (PARTITION BY symbol ORDER BY as_of_date) AS close_lag_1,
                    LAG(close, 5) OVER (PARTITION BY symbol ORDER BY as_of_date) AS close_lag_5,
                    LAG(close, 20) OVER (PARTITION BY symbol ORDER BY as_of_date) AS close_lag_20,
                    close - LAG(close, 1) OVER (PARTITION BY symbol ORDER BY as_of_date) AS delta
                FROM price_base
            ),
            enriched AS (
                SELECT
                    instrument_id,
                    company_id,
                    symbol,
                    as_of_date,
                    close,
                    high_price,
                    low_price,

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

                    AVG(close) OVER (
                        PARTITION BY symbol
                        ORDER BY as_of_date
                        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                    ) AS sma_20,

                    AVG(close) OVER (
                        PARTITION BY symbol
                        ORDER BY as_of_date
                        ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
                    ) AS sma_50,

                    AVG(close) OVER (
                        PARTITION BY symbol
                        ORDER BY as_of_date
                        ROWS BETWEEN 199 PRECEDING AND CURRENT ROW
                    ) AS sma_200,

                    CASE WHEN delta > 0 THEN delta ELSE 0 END AS gain,
                    CASE WHEN delta < 0 THEN -delta ELSE 0 END AS loss,

                    CASE
                        WHEN high_price IS NULL OR low_price IS NULL THEN NULL
                        WHEN close_lag_1 IS NULL THEN high_price - low_price
                        ELSE GREATEST(
                            high_price - low_price,
                            ABS(high_price - close_lag_1),
                            ABS(low_price - close_lag_1)
                        )
                    END AS true_range
                FROM ordered
            ),
            indicators AS (
                SELECT
                    instrument_id,
                    company_id,
                    symbol,
                    as_of_date,
                    close,
                    returns_1d,
                    returns_5d,
                    returns_20d,
                    sma_20,
                    sma_50,
                    sma_200,

                    CASE
                        WHEN sma_20 IS NULL OR sma_20 = 0 THEN NULL
                        ELSE (close / sma_20) - 1
                    END AS close_to_sma_20,

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
                                    ) /
                                    NULLIF(
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

                    AVG(true_range) OVER (
                        PARTITION BY symbol
                        ORDER BY as_of_date
                        ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
                    ) AS atr_14,

                    STDDEV_SAMP(returns_1d) OVER (
                        PARTITION BY symbol
                        ORDER BY as_of_date
                        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                    ) AS volatility_20
                FROM enriched
            )
            {short_select_sql}
            SELECT
                f.instrument_id,
                f.company_id,
                f.symbol,
                f.as_of_date,
                f.close,
                f.returns_1d,
                f.returns_5d,
                f.returns_20d,
                f.sma_20,
                f.sma_50,
                f.sma_200,
                f.close_to_sma_20,
                f.rsi_14,
                f.atr_14,
                f.volatility_20,
                s.short_interest,
                s.days_to_cover,
                s.short_volume_ratio,
                s.short_interest_change_pct,
                s.short_squeeze_score,
                s.short_pressure_zscore,
                s.days_to_cover_zscore,
                'rebuild_research_features_v2.py' AS source_name,
                CURRENT_TIMESTAMP AS created_at
            FROM indicators f
            {short_join_sql}
            """
        )

        row = con.execute(
            """
            SELECT
                COUNT(*) AS total_rows,
                COUNT(rsi_14) AS rsi_rows,
                COUNT(sma_20) AS sma20_rows,
                COUNT(sma_50) AS sma50_rows,
                COUNT(sma_200) AS sma200_rows,
                COUNT(atr_14) AS atr_rows,
                COUNT(volatility_20) AS vol20_rows,
                COUNT(short_volume_ratio) AS short_rows,
                MIN(as_of_date) AS min_date,
                MAX(as_of_date) AS max_date
            FROM research_features_daily
            """
        ).fetchone()

        print(f"[rebuild_v2] coverage={row}", flush=True)
        return 0
    finally:
        con.close()


if __name__ == "__main__":
    raise SystemExit(main())
