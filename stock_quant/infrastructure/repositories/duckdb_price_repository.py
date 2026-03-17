from __future__ import annotations

"""
DuckDbPriceRepository

Ajout clé :
- get_max_price_date()

IMPORTANT :
- lecture sur price_history UNIQUEMENT
- jamais price_latest (évite bias)
"""

import duckdb
from datetime import date
from typing import Optional


class DuckDbPriceRepository:
    def __init__(self, db_path: str):
        self._db_path = db_path

    def _connect(self):
        return duckdb.connect(self._db_path)

    def get_max_price_date(self, symbols: Optional[list[str]] = None) -> Optional[date]:
        """
        Retourne la dernière date disponible dans price_history.

        Si symbols est fourni → filtrage.
        """

        with self._connect() as con:

            if symbols:
                query = """
                    SELECT MAX(price_date)
                    FROM price_history
                    WHERE symbol IN ({})
                """.format(",".join(["?"] * len(symbols)))

                result = con.execute(query, symbols).fetchone()[0]

            else:
                query = "SELECT MAX(price_date) FROM price_history"
                result = con.execute(query).fetchone()[0]

        return result
