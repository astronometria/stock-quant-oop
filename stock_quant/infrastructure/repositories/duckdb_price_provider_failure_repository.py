from __future__ import annotations

"""
DuckDbPriceProviderFailureRepository

Responsabilités :
- stocker les échecs provider (yfinance)
- éviter les retry inutiles
- fournir une blacklist dynamique

IMPORTANT :
- utilisé AVANT fetch pour filtrer
- utilisé APRÈS fetch pour enregistrer les erreurs
"""

import duckdb
from datetime import datetime, timedelta
from typing import List, Dict


class DuckDbPriceProviderFailureRepository:
    def __init__(self, db_path: str):
        self._db_path = db_path

    def _connect(self):
        return duckdb.connect(self._db_path)

    # ---------------------------------------------------------
    # INIT TABLE (idempotent)
    # ---------------------------------------------------------
    def ensure_table(self):
        with self._connect() as con:
            con.execute("""
                CREATE TABLE IF NOT EXISTS price_provider_failures (
                    provider_name VARCHAR,
                    canonical_symbol VARCHAR,
                    provider_symbol VARCHAR,
                    failure_reason VARCHAR,
                    failure_count INTEGER,
                    first_seen TIMESTAMP,
                    last_seen TIMESTAMP
                )
            """)

    # ---------------------------------------------------------
    # READ — blacklist active
    # ---------------------------------------------------------
    def get_recent_failures(
        self,
        provider_name: str,
        max_age_days: int = 7,
        min_failure_count: int = 2,
    ) -> List[str]:
        """
        Retourne les provider_symbol à exclure du fetch.

        On ne bloque que :
        - erreurs répétées
        - récentes
        """
        with self._connect() as con:
            rows = con.execute("""
                SELECT provider_symbol
                FROM price_provider_failures
                WHERE provider_name = ?
                  AND failure_count >= ?
                  AND last_seen >= NOW() - INTERVAL ? DAY
            """, [provider_name, min_failure_count, max_age_days]).fetchall()

        return [r[0] for r in rows if r[0]]

    # ---------------------------------------------------------
    # WRITE — upsert failures
    # ---------------------------------------------------------
    def upsert_failures(
        self,
        provider_name: str,
        failures: List[Dict],
    ):
        """
        failures = [
            {
                "canonical_symbol": "...",
                "provider_symbol": "...",
                "failure_reason": "..."
            }
        ]
        """
        if not failures:
            return

        now = datetime.utcnow()

        with self._connect() as con:
            for f in failures:
                con.execute("""
                    MERGE INTO price_provider_failures t
                    USING (
                        SELECT
                            ? AS provider_name,
                            ? AS canonical_symbol,
                            ? AS provider_symbol,
                            ? AS failure_reason
                    ) s
                    ON t.provider_name = s.provider_name
                       AND t.provider_symbol = s.provider_symbol
                    WHEN MATCHED THEN UPDATE SET
                        failure_count = t.failure_count + 1,
                        last_seen = ?
                    WHEN NOT MATCHED THEN INSERT (
                        provider_name,
                        canonical_symbol,
                        provider_symbol,
                        failure_reason,
                        failure_count,
                        first_seen,
                        last_seen
                    )
                    VALUES (?, ?, ?, ?, 1, ?, ?)
                """, [
                    provider_name,
                    f["canonical_symbol"],
                    f["provider_symbol"],
                    f["failure_reason"],
                    now,
                    provider_name,
                    f["canonical_symbol"],
                    f["provider_symbol"],
                    f["failure_reason"],
                    now,
                    now,
                ])
