from __future__ import annotations

"""
DuckDbPriceRepository (coverage-aware)

Ajouts :
- get_price_date_counts()
- get_latest_complete_price_date()

IMPORTANT :
- travaille uniquement sur price_history
- évite faux "up_to_date" avec données partielles
"""

import duckdb
from datetime import date
from typing import Optional, List


class DuckDbPriceRepository:
    def __init__(self, db_path: str):
        self._db_path = db_path

    def _connect(self):
        return duckdb.connect(self._db_path)

    # ------------------------------------------------------------------
    # BASE
    # ------------------------------------------------------------------
    def get_max_price_date(self, symbols: Optional[List[str]] = None) -> Optional[date]:
        with self._connect() as con:
            if symbols:
                query = f"""
                    SELECT MAX(price_date)
                    FROM price_history
                    WHERE symbol IN ({",".join(["?"] * len(symbols))})
                """
                return con.execute(query, symbols).fetchone()[0]

            return con.execute("SELECT MAX(price_date) FROM price_history").fetchone()[0]

    # ------------------------------------------------------------------
    # COVERAGE
    # ------------------------------------------------------------------
    def get_price_date_counts(self, symbols: Optional[List[str]] = None):
        """
        Retourne:
        [
          (price_date, distinct_symbol_count)
        ]
        trié DESC
        """

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
        """
        Retourne :
        (
            last_complete_date,
            last_any_date,
            expected_count,
            observed_latest_count
        )
        """

        rows = self.get_price_date_counts(symbols)

        if not rows:
            return None, None, 0, 0

        last_any_date, latest_count = rows[0]

        # heuristique simple robuste :
        # expected_count = max count observé récemment
        expected_count = max(r[1] for r in rows)

        # trouve dernière date complète
        last_complete_date = None

        for d, cnt in rows:
            if cnt >= expected_count:
                last_complete_date = d
                break

        return last_complete_date, last_any_date, expected_count, latest_count
