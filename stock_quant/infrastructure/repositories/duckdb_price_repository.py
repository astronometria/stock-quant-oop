from __future__ import annotations

"""
DuckDbPriceRepository (coverage-aware + symbol-scope aware)

Priorité de scope :
1) symboles explicitement demandés
2) market_universe latest as_of_date where include_in_universe = TRUE
3) symbol_reference
4) distinct symbol from price_history

IMPORTANT :
- price_history reste la source canonique pour la couverture prix
- price_latest n'est jamais utilisé pour la recherche ni pour décider du refresh
"""

import duckdb
from datetime import date
from typing import Optional, List


class DuckDbPriceRepository:
    def __init__(self, db_path: str):
        self._db_path = db_path

    def _connect(self):
        return duckdb.connect(self._db_path)

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

            return con.execute("""
                SELECT price_date, COUNT(DISTINCT symbol)
                FROM price_history
                GROUP BY price_date
                ORDER BY price_date DESC
                LIMIT 30
            """).fetchall()

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

    def get_refresh_symbols(
        self,
        symbols: Optional[List[str]] = None,
    ) -> tuple[List[str], str]:
        """
        Retourne:
        (
            resolved_symbols,
            symbol_scope_source,
        )
        """

        if symbols:
            cleaned = sorted({str(s).strip().upper() for s in symbols if str(s).strip()})
            return cleaned, "explicit_args"

        with self._connect() as con:
            tables = {row[0] for row in con.execute("SHOW TABLES").fetchall()}

            # --------------------------------------------------------------
            # 1) market_universe latest as_of_date
            # --------------------------------------------------------------
            if "market_universe" in tables:
                latest_as_of_row = con.execute("""
                    SELECT MAX(as_of_date)
                    FROM market_universe
                """).fetchone()
                latest_as_of = latest_as_of_row[0] if latest_as_of_row else None

                if latest_as_of is not None:
                    rows = con.execute("""
                        SELECT DISTINCT symbol
                        FROM market_universe
                        WHERE as_of_date = ?
                          AND include_in_universe = TRUE
                          AND symbol IS NOT NULL
                          AND TRIM(symbol) <> ''
                        ORDER BY symbol
                    """, [latest_as_of]).fetchall()

                    values = [row[0] for row in rows if row[0]]
                    if values:
                        return values, "market_universe_latest_as_of"

            # --------------------------------------------------------------
            # 2) symbol_reference
            # --------------------------------------------------------------
            if "symbol_reference" in tables:
                rows = con.execute("""
                    SELECT DISTINCT symbol
                    FROM symbol_reference
                    WHERE symbol IS NOT NULL
                      AND TRIM(symbol) <> ''
                    ORDER BY symbol
                """).fetchall()

                values = [row[0] for row in rows if row[0]]
                if values:
                    return values, "symbol_reference_all"

            # --------------------------------------------------------------
            # 3) price_history fallback
            # --------------------------------------------------------------
            if "price_history" in tables:
                rows = con.execute("""
                    SELECT DISTINCT symbol
                    FROM price_history
                    WHERE symbol IS NOT NULL
                      AND TRIM(symbol) <> ''
                    ORDER BY symbol
                """).fetchall()

                values = [row[0] for row in rows if row[0]]
                if values:
                    return values, "price_history_distinct"

        return [], "none"
