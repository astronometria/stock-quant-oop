from __future__ import annotations

"""
DuckDbPriceRepository

Responsabilités :
- résoudre le scope canonique des symboles
- sonder la couverture de price_history
- écrire les prix de manière idempotente dans price_history
- rafraîchir price_latest comme table de serving dérivée

IMPORTANT :
- price_history = table canonique
- price_latest = serving only
"""

import duckdb
from datetime import date, datetime
from typing import Optional, List

import pandas as pd


class DuckDbPriceRepository:
    def __init__(self, db_path: str):
        self._db_path = db_path

    def _connect(self):
        return duckdb.connect(self._db_path)

    # ------------------------------------------------------------------
    # COVERAGE / WINDOW PROBES
    # ------------------------------------------------------------------
    def get_price_date_counts(self, symbols: Optional[List[str]] = None):
        with self._connect() as con:
            if symbols:
                query = f"""
                    SELECT price_date, COUNT(DISTINCT symbol)
                    FROM price_history
                    WHERE symbol IN ({",".join(["?"] * len(symbols))})
                    GROUP BY price_date
                    ORDER BY price_date DESC
                    LIMIT 30
                """
                return con.execute(query, symbols).fetchall()

            return con.execute(
                """
                SELECT price_date, COUNT(DISTINCT symbol)
                FROM price_history
                GROUP BY price_date
                ORDER BY price_date DESC
                LIMIT 30
                """
            ).fetchall()

    def get_latest_complete_price_date(
        self,
        symbols: Optional[List[str]] = None,
    ) -> tuple[Optional[date], Optional[date], int, int]:
        rows = self.get_price_date_counts(symbols)

        if not rows:
            return None, None, 0, 0

        last_any_date, latest_count = rows[0]
        expected_count = max(r[1] for r in rows)

        last_complete_date = None
        for d, cnt in rows:
            if cnt >= expected_count:
                last_complete_date = d
                break

        return last_complete_date, last_any_date, expected_count, latest_count

    # ------------------------------------------------------------------
    # SYMBOL SCOPE
    # ------------------------------------------------------------------
    def get_refresh_symbols(
        self,
        symbols: Optional[List[str]] = None,
    ) -> tuple[List[str], str]:
        if symbols:
            cleaned = sorted({str(s).strip().upper() for s in symbols if str(s).strip()})
            return cleaned, "explicit_args"

        with self._connect() as con:
            tables = {row[0] for row in con.execute("SHOW TABLES").fetchall()}

            if "market_universe" in tables:
                latest_as_of_row = con.execute(
                    """
                    SELECT MAX(as_of_date)
                    FROM market_universe
                    """
                ).fetchone()
                latest_as_of = latest_as_of_row[0] if latest_as_of_row else None

                if latest_as_of is not None:
                    rows = con.execute(
                        """
                        SELECT DISTINCT symbol
                        FROM market_universe
                        WHERE as_of_date = ?
                          AND include_in_universe = TRUE
                          AND symbol IS NOT NULL
                          AND TRIM(symbol) <> ''
                        ORDER BY symbol
                        """,
                        [latest_as_of],
                    ).fetchall()

                    values = [row[0] for row in rows if row[0]]
                    if values:
                        return values, "market_universe_latest_as_of"

            if "symbol_reference" in tables:
                rows = con.execute(
                    """
                    SELECT DISTINCT symbol
                    FROM symbol_reference
                    WHERE symbol IS NOT NULL
                      AND TRIM(symbol) <> ''
                    ORDER BY symbol
                    """
                ).fetchall()

                values = [row[0] for row in rows if row[0]]
                if values:
                    return values, "symbol_reference_all"

            if "price_history" in tables:
                rows = con.execute(
                    """
                    SELECT DISTINCT symbol
                    FROM price_history
                    WHERE symbol IS NOT NULL
                      AND TRIM(symbol) <> ''
                    ORDER BY symbol
                    """
                ).fetchall()

                values = [row[0] for row in rows if row[0]]
                if values:
                    return values, "price_history_distinct"

        return [], "none"

    def get_symbols_needing_refresh_through(
        self,
        *,
        target_date: date,
        symbols: List[str],
    ) -> list[str]:
        """
        Retourne seulement les symboles dont la dernière date prix connue
        est strictement antérieure à `target_date`, ou absente.

        Pourquoi
        --------
        Le pipeline actuel est incrémental en dates, mais pas en symboles.
        Cette méthode réduit le scope provider aux symboles réellement
        en retard jusqu'à la date cible.

        Note technique
        --------------
        On utilise un DataFrame enregistré temporairement plutôt qu'une très
        longue liste de placeholders SQL. Cela évite les erreurs de binding
        et reste lisible.
        """
        cleaned = sorted({str(s).strip().upper() for s in symbols if str(s).strip()})
        if not cleaned:
            return []

        scope_df = pd.DataFrame({"symbol": cleaned})

        with self._connect() as con:
            con.register("price_refresh_scope_df", scope_df)

            rows = con.execute(
                """
                WITH scope AS (
                    SELECT UPPER(TRIM(CAST(symbol AS VARCHAR))) AS symbol
                    FROM price_refresh_scope_df
                ),
                latest AS (
                    SELECT
                        UPPER(TRIM(symbol)) AS symbol,
                        MAX(price_date) AS max_price_date
                    FROM price_history
                    WHERE UPPER(TRIM(symbol)) IN (SELECT symbol FROM scope)
                    GROUP BY 1
                )
                SELECT s.symbol
                FROM scope s
                LEFT JOIN latest l
                  ON l.symbol = s.symbol
                WHERE l.max_price_date IS NULL
                   OR l.max_price_date < CAST(? AS DATE)
                ORDER BY s.symbol
                """,
                [target_date],
            ).fetchall()

            con.unregister("price_refresh_scope_df")

        return [str(row[0]).strip().upper() for row in rows if row[0]]

    # ------------------------------------------------------------------
    # WRITES
    # ------------------------------------------------------------------
    def upsert_price_history(
        self,
        frame: pd.DataFrame,
        *,
        source_name: str = "yfinance",
        ingested_at: Optional[datetime] = None,
    ) -> dict:
        """
        Upsert idempotent vers price_history.

        Clé logique :
        - (symbol, price_date)

        Stratégie :
        - staging temporaire
        - delete matching keys
        - insert normalized rows
        """
        if frame is None or frame.empty:
            return {
                "input_rows": 0,
                "staged_rows": 0,
                "deleted_existing_rows": 0,
                "inserted_rows": 0,
            }

        working = frame.copy()

        # Normalisation défensive.
        working["symbol"] = working["symbol"].astype(str).str.strip().str.upper()
        working["price_date"] = pd.to_datetime(working["price_date"]).dt.date
        working["source_name"] = source_name
        working["ingested_at"] = ingested_at or datetime.utcnow()

        required = ["symbol", "price_date", "open", "high", "low", "close", "volume", "source_name", "ingested_at"]
        working = working[required].dropna(subset=["symbol", "price_date", "close"])
        working = working.drop_duplicates(subset=["symbol", "price_date"]).reset_index(drop=True)

        if working.empty:
            return {
                "input_rows": len(frame),
                "staged_rows": 0,
                "deleted_existing_rows": 0,
                "inserted_rows": 0,
            }

        with self._connect() as con:
            con.register("price_upsert_stage_df", working)

            con.execute(
                """
                CREATE TEMP TABLE price_upsert_stage AS
                SELECT
                    CAST(symbol AS VARCHAR) AS symbol,
                    CAST(price_date AS DATE) AS price_date,
                    CAST(open AS DOUBLE) AS open,
                    CAST(high AS DOUBLE) AS high,
                    CAST(low AS DOUBLE) AS low,
                    CAST(close AS DOUBLE) AS close,
                    CAST(volume AS BIGINT) AS volume,
                    CAST(source_name AS VARCHAR) AS source_name,
                    CAST(ingested_at AS TIMESTAMP) AS ingested_at
                FROM price_upsert_stage_df
                """
            )

            staged_rows = con.execute("SELECT COUNT(*) FROM price_upsert_stage").fetchone()[0]

            deleted_existing_rows = con.execute(
                """
                DELETE FROM price_history
                USING price_upsert_stage s
                WHERE price_history.symbol = s.symbol
                  AND price_history.price_date = s.price_date
                """
            ).fetchone()[0]

            inserted_rows = con.execute(
                """
                INSERT INTO price_history (
                    symbol,
                    price_date,
                    open,
                    high,
                    low,
                    close,
                    volume,
                    source_name,
                    ingested_at
                )
                SELECT
                    symbol,
                    price_date,
                    open,
                    high,
                    low,
                    close,
                    volume,
                    source_name,
                    ingested_at
                FROM price_upsert_stage
                """
            ).fetchone()[0]

            con.unregister("price_upsert_stage_df")

        return {
            "input_rows": len(frame),
            "staged_rows": staged_rows,
            "deleted_existing_rows": deleted_existing_rows,
            "inserted_rows": inserted_rows,
        }

    def refresh_price_latest(self, symbols: Optional[List[str]] = None) -> dict:
        """
        Rebuild ciblé de price_latest à partir de price_history.
        """
        with self._connect() as con:
            if symbols:
                clean_symbols = sorted({str(s).strip().upper() for s in symbols if str(s).strip()})
                if not clean_symbols:
                    return {"deleted_rows": 0, "inserted_rows": 0}

                placeholders = ",".join(["?"] * len(clean_symbols))

                deleted_rows = con.execute(
                    f"DELETE FROM price_latest WHERE symbol IN ({placeholders})",
                    clean_symbols,
                ).fetchone()[0]

                inserted_rows = con.execute(
                    f"""
                    INSERT INTO price_latest (
                        symbol,
                        latest_price_date,
                        close,
                        volume,
                        source_name,
                        updated_at
                    )
                    WITH ranked AS (
                        SELECT
                            symbol,
                            price_date,
                            close,
                            volume,
                            source_name,
                            ROW_NUMBER() OVER (
                                PARTITION BY symbol
                                ORDER BY price_date DESC, ingested_at DESC
                            ) AS rn
                        FROM price_history
                        WHERE symbol IN ({placeholders})
                    )
                    SELECT
                        symbol,
                        price_date AS latest_price_date,
                        close,
                        volume,
                        source_name,
                        CURRENT_TIMESTAMP AS updated_at
                    FROM ranked
                    WHERE rn = 1
                    """,
                    clean_symbols,
                ).fetchone()[0]

                return {"deleted_rows": deleted_rows, "inserted_rows": inserted_rows}

            deleted_rows = con.execute("DELETE FROM price_latest").fetchone()[0]

            inserted_rows = con.execute(
                """
                INSERT INTO price_latest (
                    symbol,
                    latest_price_date,
                    close,
                    volume,
                    source_name,
                    updated_at
                )
                WITH ranked AS (
                    SELECT
                        symbol,
                        price_date,
                        close,
                        volume,
                        source_name,
                        ROW_NUMBER() OVER (
                            PARTITION BY symbol
                            ORDER BY price_date DESC, ingested_at DESC
                        ) AS rn
                    FROM price_history
                )
                SELECT
                    symbol,
                    price_date AS latest_price_date,
                    close,
                    volume,
                    source_name,
                    CURRENT_TIMESTAMP AS updated_at
                FROM ranked
                WHERE rn = 1
                """
            ).fetchone()[0]

            return {"deleted_rows": deleted_rows, "inserted_rows": inserted_rows}
