from __future__ import annotations

"""
DuckDbPriceProviderFailureRepository

Responsabilités :
- stocker les échecs provider (yfinance)
- éviter les retries inutiles sur les mêmes symboles morts
- fournir une blacklist dynamique récente

IMPORTANT :
- filtrage AVANT fetch
- enregistrement APRÈS fetch
- pas de logique métier Yahoo complexe ici : seulement persistence / lecture
"""

import duckdb
from datetime import datetime, timedelta
from typing import Dict, List


class DuckDbPriceProviderFailureRepository:
    def __init__(self, db_path: str):
        self._db_path = db_path

    def _connect(self):
        return duckdb.connect(self._db_path)

    # ---------------------------------------------------------
    # INIT TABLE (idempotent)
    # ---------------------------------------------------------
    def ensure_table(self) -> None:
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
        Retourne les provider_symbol à exclure temporairement du fetch.

        Politique :
        - uniquement les erreurs répétées
        - uniquement les erreurs récentes

        NOTE :
        DuckDB ne supporte pas bien `INTERVAL ? DAY` avec placeholder.
        On calcule donc le cutoff en Python, puis on passe un timestamp normal.
        """
        cutoff_ts = datetime.utcnow() - timedelta(days=max_age_days)

        with self._connect() as con:
            rows = con.execute("""
                SELECT provider_symbol
                FROM price_provider_failures
                WHERE provider_name = ?
                  AND failure_count >= ?
                  AND last_seen >= ?
            """, [provider_name, min_failure_count, cutoff_ts]).fetchall()

        return [row[0] for row in rows if row[0]]

    # ---------------------------------------------------------
    # WRITE — upsert failures
    # ---------------------------------------------------------
    def upsert_failures(
        self,
        provider_name: str,
        failures: List[Dict],
    ) -> None:
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
            for failure in failures:
                con.execute("""
                    MERGE INTO price_provider_failures AS target
                    USING (
                        SELECT
                            ? AS provider_name,
                            ? AS canonical_symbol,
                            ? AS provider_symbol,
                            ? AS failure_reason
                    ) AS src
                    ON target.provider_name = src.provider_name
                       AND target.provider_symbol = src.provider_symbol
                    WHEN MATCHED THEN UPDATE SET
                        failure_count = target.failure_count + 1,
                        failure_reason = src.failure_reason,
                        canonical_symbol = src.canonical_symbol,
                        last_seen = ?
                    WHEN NOT MATCHED THEN
                        INSERT (
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
                    failure["canonical_symbol"],
                    failure["provider_symbol"],
                    failure["failure_reason"],
                    now,
                    provider_name,
                    failure["canonical_symbol"],
                    failure["provider_symbol"],
                    failure["failure_reason"],
                    now,
                    now,
                ])
