"""
duckdb_listing_history_repository.py

Repository DuckDB pour la table historisée listing_status_history.

Philosophie
-----------
Ce repository est volontairement SQL-first :
- lectures bulk en SQL
- filtres et recherches en SQL
- écritures simples via INSERT / UPDATE

Le service applicatif garde la logique métier.
Le repository gère surtout la persistance et les requêtes ciblées.

Important
---------
Pour rester cohérent avec le codebase existant, ce repository prend
directement une connexion DuckDB active, pas un UnitOfWork.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Any

from stock_quant.domain.entities.listing_observation import ListingObservation
from stock_quant.domain.entities.listing_version import ListingVersion
from stock_quant.shared.exceptions import RepositoryError


class DuckDbListingHistoryRepository:
    """
    Repository principal de l'historique des listings.
    """

    def __init__(self, con: Any) -> None:
        self.con = con

    # -------------------------------------------------------------------------
    # Helpers
    # -------------------------------------------------------------------------
    @staticmethod
    def _safe_str(value: Any) -> str | None:
        if value is None:
            return None
        return str(value)

    @staticmethod
    def _safe_datetime(value: Any) -> datetime | None:
        return value

    @staticmethod
    def _safe_date(value: Any) -> date | None:
        return value

    def _require_connection(self) -> None:
        if self.con is None:
            raise RepositoryError("DuckDbListingHistoryRepository requires an active connection")

    @staticmethod
    def _row_to_listing_version(row: tuple[Any, ...]) -> ListingVersion:
        """
        Convertit une ligne SQL en entité domaine ListingVersion.
        """
        return ListingVersion(
            listing_id=row[0],
            company_id=row[1],
            cik=row[2],
            symbol=row[3],
            company_name=row[4],
            company_name_clean=row[5],
            exchange=row[6],
            security_type=row[7],
            source_name=row[8],
            listing_status=row[9],
            status_reason=row[10],
            first_seen_at=row[11],
            last_seen_at=row[12],
            effective_from=row[13],
            effective_to=row[14],
            is_active=row[15],
            created_at=row[16],
            updated_at=row[17],
        )

    # -------------------------------------------------------------------------
    # Source observations
    # -------------------------------------------------------------------------
    def load_listing_observations_for_date(
        self,
        as_of_date: date,
    ) -> list[ListingObservation]:
        """
        Charge les observations source pour une date métier donnée.

        Règles:
        - on préfère symbol_reference_source_raw si disponible
        - fallback sur symbol_reference si nécessaire
        - on évite toute auto-référence d'alias SQL dans le SELECT
        - le nettoyage de company_name_clean est finalisé en Python
        """
        self._require_connection()

        tables = {row[0] for row in self.con.execute("SHOW TABLES").fetchall()}

        if "symbol_reference_source_raw" in tables:
            source_table = "symbol_reference_source_raw"
        elif "symbol_reference" in tables:
            source_table = "symbol_reference"
        else:
            raise RepositoryError(
                "Missing source table: expected symbol_reference_source_raw or symbol_reference"
            )

        cols = {row[0] for row in self.con.execute(f"DESCRIBE {source_table}").fetchall()}

        def has_col(name: str) -> bool:
            return name in cols

        if has_col("as_of_date"):
            as_of_date_expr = "as_of_date"
        elif has_col("created_at"):
            as_of_date_expr = "CAST(created_at AS DATE)"
        else:
            raise RepositoryError(
                f"{source_table} must expose as_of_date or created_at for listing history build"
            )

        if has_col("ingested_at"):
            ingested_at_expr = "ingested_at"
        elif has_col("created_at"):
            ingested_at_expr = "created_at"
        else:
            ingested_at_expr = "CURRENT_TIMESTAMP"

        cik_expr = "NULLIF(TRIM(CAST(cik AS VARCHAR)), '')" if has_col("cik") else "NULL"
        company_name_expr = (
            "COALESCE(TRIM(CAST(company_name AS VARCHAR)), '')"
            if has_col("company_name")
            else "''"
        )
        company_name_clean_expr = (
            "COALESCE(TRIM(CAST(company_name_clean AS VARCHAR)), '')"
            if has_col("company_name_clean")
            else "''"
        )
        exchange_expr = (
            "NULLIF(TRIM(CAST(exchange_raw AS VARCHAR)), '')"
            if has_col("exchange_raw")
            else (
                "NULLIF(TRIM(CAST(exchange AS VARCHAR)), '')"
                if has_col("exchange")
                else "NULL"
            )
        )
        security_type_expr = (
            "NULLIF(TRIM(CAST(security_type_raw AS VARCHAR)), '')"
            if has_col("security_type_raw")
            else (
                "NULLIF(TRIM(CAST(security_type AS VARCHAR)), '')"
                if has_col("security_type")
                else "NULL"
            )
        )
        source_name_expr = (
            "COALESCE(TRIM(CAST(source_name AS VARCHAR)), '')"
            if has_col("source_name")
            else f"'{source_table}'"
        )

        rows = self.con.execute(
            f"""
            WITH source_rows AS (
                SELECT
                    TRIM(CAST(symbol AS VARCHAR)) AS symbol,
                    {cik_expr} AS cik,
                    {company_name_expr} AS company_name,
                    {company_name_clean_expr} AS company_name_clean_raw,
                    {exchange_expr} AS exchange_raw,
                    {security_type_expr} AS security_type_raw,
                    {source_name_expr} AS source_name_raw,
                    {as_of_date_expr} AS snapshot_as_of_date,
                    {ingested_at_expr} AS snapshot_ingested_at
                FROM {source_table}
                WHERE {as_of_date_expr} = ?
                  AND symbol IS NOT NULL
                  AND TRIM(CAST(symbol AS VARCHAR)) <> ''
            ),
            normalized AS (
                SELECT
                    symbol,
                    cik,
                    company_name,
                    company_name_clean_raw,
                    exchange_raw,
                    security_type_raw,
                    source_name_raw,
                    snapshot_as_of_date,
                    snapshot_ingested_at,
                    UPPER(TRIM(COALESCE(symbol, ''))) AS symbol_key,
                    UPPER(TRIM(COALESCE(cik, ''))) AS cik_key,
                    UPPER(
                        TRIM(
                            COALESCE(
                                NULLIF(company_name_clean_raw, ''),
                                company_name,
                                ''
                            )
                        )
                    ) AS company_name_key,
                    UPPER(TRIM(COALESCE(exchange_raw, ''))) AS exchange_key,
                    UPPER(TRIM(COALESCE(security_type_raw, ''))) AS security_type_key
                FROM source_rows
            ),
            deduped AS (
                SELECT
                    symbol,
                    cik,
                    company_name,
                    company_name_clean_raw,
                    exchange_raw,
                    security_type_raw,
                    source_name_raw,
                    snapshot_as_of_date,
                    snapshot_ingested_at
                FROM normalized
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY
                        symbol_key,
                        exchange_key,
                        security_type_key,
                        snapshot_as_of_date
                    ORDER BY
                        CASE WHEN cik_key <> '' THEN 0 ELSE 1 END,
                        CASE WHEN company_name_key <> '' THEN 0 ELSE 1 END,
                        snapshot_ingested_at DESC NULLS LAST,
                        source_name_raw DESC
                ) = 1
            )
            SELECT
                symbol,
                cik,
                company_name,
                company_name_clean_raw,
                exchange_raw,
                security_type_raw,
                source_name_raw,
                snapshot_as_of_date,
                snapshot_ingested_at
            FROM deduped
            ORDER BY
                1, 5, 6, 7
            """,
            [as_of_date],
        ).fetchall()

        observations: list[ListingObservation] = []
        for row in rows:
            company_name = row[2] or ""
            company_name_clean = (row[3] or "").strip().upper()
            if not company_name_clean:
                company_name_clean = company_name.strip().upper()

            observations.append(
                ListingObservation(
                    symbol=row[0],
                    cik=row[1],
                    company_name=company_name,
                    company_name_clean=company_name_clean,
                    exchange=(row[4] or None),
                    security_type=(row[5] or None),
                    source_name=(row[6] or ""),
                    as_of_date=row[7],
                    ingested_at=row[8],
                )
            )
        return observations

    def load_available_as_of_dates(self) -> list[date]:
        """
        Retourne toutes les dates disponibles dans la staging source.

        Règles:
        - préfère symbol_reference_source_raw
        - fallback sur symbol_reference
        """
        self._require_connection()

        tables = {row[0] for row in self.con.execute("SHOW TABLES").fetchall()}

        if "symbol_reference_source_raw" in tables:
            source_table = "symbol_reference_source_raw"
        elif "symbol_reference" in tables:
            source_table = "symbol_reference"
        else:
            raise RepositoryError(
                "Missing source table: expected symbol_reference_source_raw or symbol_reference"
            )

        cols = {row[0] for row in self.con.execute(f"DESCRIBE {source_table}").fetchall()}

        if "as_of_date" in cols:
            date_expr = "as_of_date"
        elif "created_at" in cols:
            date_expr = "CAST(created_at AS DATE)"
        else:
            raise RepositoryError(
                f"{source_table} must expose as_of_date or created_at"
            )

        rows = self.con.execute(
            f"""
            SELECT DISTINCT {date_expr} AS as_of_date
            FROM {source_table}
            WHERE {date_expr} IS NOT NULL
            ORDER BY as_of_date
            """
        ).fetchall()
        return [row[0] for row in rows]

    # -------------------------------------------------------------------------
    # Active versions
    # -------------------------------------------------------------------------
    def find_active_listing_version_by_identity_key(
        self,
        identity_key: str,
    ) -> ListingVersion | None:
        """
        Recherche une version active par identité société.
        """
        self._require_connection()

        if identity_key.startswith("CIK:"):
            cik = identity_key.removeprefix("CIK:")
            row = self.con.execute(
                """
                SELECT
                    listing_id, company_id, cik, symbol, company_name, company_name_clean,
                    exchange, security_type, source_name, listing_status, status_reason,
                    first_seen_at, last_seen_at, effective_from, effective_to,
                    is_active, created_at, updated_at
                FROM listing_status_history
                WHERE is_active = TRUE
                  AND effective_to IS NULL
                  AND TRIM(CAST(cik AS VARCHAR)) = ?
                ORDER BY updated_at DESC NULLS LAST, created_at DESC NULLS LAST
                LIMIT 1
                """,
                [cik],
            ).fetchone()
            return None if row is None else self._row_to_listing_version(row)

        if identity_key.startswith("COMPANY:"):
            company_id = identity_key.removeprefix("COMPANY:")
            row = self.con.execute(
                """
                SELECT
                    listing_id, company_id, cik, symbol, company_name, company_name_clean,
                    exchange, security_type, source_name, listing_status, status_reason,
                    first_seen_at, last_seen_at, effective_from, effective_to,
                    is_active, created_at, updated_at
                FROM listing_status_history
                WHERE is_active = TRUE
                  AND effective_to IS NULL
                  AND TRIM(CAST(company_id AS VARCHAR)) = ?
                ORDER BY updated_at DESC NULLS LAST, created_at DESC NULLS LAST
                LIMIT 1
                """,
                [company_id],
            ).fetchone()
            return None if row is None else self._row_to_listing_version(row)

        if identity_key.startswith("FALLBACK:"):
            fallback_value = identity_key.removeprefix("FALLBACK:")
            parts = fallback_value.split("|", 1)
            symbol = parts[0].strip().upper() if parts else ""
            company_name_clean = parts[1].strip().upper() if len(parts) > 1 else ""

            row = self.con.execute(
                """
                SELECT
                    listing_id, company_id, cik, symbol, company_name, company_name_clean,
                    exchange, security_type, source_name, listing_status, status_reason,
                    first_seen_at, last_seen_at, effective_from, effective_to,
                    is_active, created_at, updated_at
                FROM listing_status_history
                WHERE is_active = TRUE
                  AND effective_to IS NULL
                  AND UPPER(TRIM(CAST(symbol AS VARCHAR))) = ?
                  AND UPPER(TRIM(CAST(company_name_clean AS VARCHAR))) = ?
                ORDER BY updated_at DESC NULLS LAST, created_at DESC NULLS LAST
                LIMIT 1
                """,
                [symbol, company_name_clean],
            ).fetchone()
            return None if row is None else self._row_to_listing_version(row)

        return None

    def load_all_active_listing_versions(self) -> list[ListingVersion]:
        """
        Charge toutes les versions actives.
        """
        self._require_connection()
        rows = self.con.execute(
            """
            SELECT
                listing_id, company_id, cik, symbol, company_name, company_name_clean,
                exchange, security_type, source_name, listing_status, status_reason,
                first_seen_at, last_seen_at, effective_from, effective_to,
                is_active, created_at, updated_at
            FROM listing_status_history
            WHERE is_active = TRUE
              AND effective_to IS NULL
            ORDER BY symbol, exchange, security_type, effective_from
            """
        ).fetchall()
        return [self._row_to_listing_version(row) for row in rows]

    # -------------------------------------------------------------------------
    # Writes
    # -------------------------------------------------------------------------
    def insert_listing_version(
        self,
        version: ListingVersion,
    ) -> None:
        self._require_connection()
        self.con.execute(
            """
            INSERT INTO listing_status_history (
                listing_id,
                company_id,
                cik,
                symbol,
                company_name,
                company_name_clean,
                exchange,
                security_type,
                source_name,
                listing_status,
                status_reason,
                first_seen_at,
                last_seen_at,
                effective_from,
                effective_to,
                is_active,
                created_at,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                version.listing_id,
                version.company_id,
                version.cik,
                version.symbol,
                version.company_name,
                version.company_name_clean,
                version.exchange,
                version.security_type,
                version.source_name,
                version.listing_status,
                version.status_reason,
                version.first_seen_at,
                version.last_seen_at,
                version.effective_from,
                version.effective_to,
                version.is_active,
                version.created_at,
                version.updated_at,
            ],
        )

    def close_listing_version(
        self,
        *,
        listing_id: str,
        effective_to: date,
        new_status: str,
        status_reason: str,
        updated_at: datetime | None,
        last_seen_at: datetime | None,
    ) -> None:
        self._require_connection()
        self.con.execute(
            """
            UPDATE listing_status_history
            SET
                effective_to = ?,
                is_active = FALSE,
                listing_status = ?,
                status_reason = ?,
                updated_at = ?,
                last_seen_at = COALESCE(?, last_seen_at)
            WHERE listing_id = ?
              AND is_active = TRUE
              AND effective_to IS NULL
            """,
            [
                effective_to,
                new_status,
                status_reason,
                updated_at,
                last_seen_at,
                listing_id,
            ],
        )

    def touch_listing_version_last_seen(
        self,
        *,
        listing_id: str,
        last_seen_at: datetime | None,
        updated_at: datetime | None,
    ) -> None:
        self._require_connection()
        self.con.execute(
            """
            UPDATE listing_status_history
            SET
                last_seen_at = COALESCE(?, last_seen_at),
                updated_at = ?
            WHERE listing_id = ?
              AND is_active = TRUE
              AND effective_to IS NULL
            """,
            [
                last_seen_at,
                updated_at,
                listing_id,
            ],
        )

    # -------------------------------------------------------------------------
    # Missing observation support
    # -------------------------------------------------------------------------
    def load_missing_active_versions_for_date(
        self,
        as_of_date: date,
    ) -> list[ListingVersion]:
        """
        Charge les versions actives qui n'ont pas été observées à la date donnée.
        """
        self._require_connection()
        rows = self.con.execute(
            """
            WITH observed AS (
                SELECT DISTINCT
                    UPPER(TRIM(CAST(symbol AS VARCHAR))) AS symbol,
                    UPPER(TRIM(COALESCE(CAST(exchange_raw AS VARCHAR), ''))) AS exchange,
                    UPPER(TRIM(COALESCE(CAST(security_type_raw AS VARCHAR), ''))) AS security_type
                FROM symbol_reference_source_raw
                WHERE as_of_date = ?
                  AND symbol IS NOT NULL
                  AND TRIM(CAST(symbol AS VARCHAR)) <> ''
            )
            SELECT
                h.listing_id, h.company_id, h.cik, h.symbol, h.company_name, h.company_name_clean,
                h.exchange, h.security_type, h.source_name, h.listing_status, h.status_reason,
                h.first_seen_at, h.last_seen_at, h.effective_from, h.effective_to,
                h.is_active, h.created_at, h.updated_at
            FROM listing_status_history h
            LEFT JOIN observed o
              ON UPPER(TRIM(CAST(h.symbol AS VARCHAR))) = o.symbol
             AND UPPER(TRIM(COALESCE(CAST(h.exchange AS VARCHAR), ''))) = o.exchange
             AND UPPER(TRIM(COALESCE(CAST(h.security_type AS VARCHAR), ''))) = o.security_type
            WHERE h.is_active = TRUE
              AND h.effective_to IS NULL
              AND o.symbol IS NULL
            ORDER BY h.symbol, h.exchange, h.security_type
            """,
            [as_of_date],
        ).fetchall()
        return [self._row_to_listing_version(row) for row in rows]

    def count_consecutive_missing_snapshots(
        self,
        *,
        symbol: str,
        exchange: str | None,
        security_type: str | None,
        up_to_as_of_date: date,
    ) -> int:
        """
        Compte le nombre de snapshots consécutifs manquants pour un listing.
        """
        self._require_connection()

        available_dates = self.con.execute(
            """
            SELECT DISTINCT as_of_date
            FROM symbol_reference_source_raw
            WHERE as_of_date IS NOT NULL
              AND as_of_date <= ?
            ORDER BY as_of_date DESC
            """,
            [up_to_as_of_date],
        ).fetchall()

        normalized_symbol = symbol.strip().upper()
        normalized_exchange = (exchange or "").strip().upper()
        normalized_security_type = (security_type or "").strip().upper()

        missing_count = 0
        for (snapshot_date,) in available_dates:
            row = self.con.execute(
                """
                SELECT 1
                FROM symbol_reference_source_raw
                WHERE as_of_date = ?
                  AND UPPER(TRIM(CAST(symbol AS VARCHAR))) = ?
                  AND UPPER(TRIM(COALESCE(CAST(exchange_raw AS VARCHAR), ''))) = ?
                  AND UPPER(TRIM(COALESCE(CAST(security_type_raw AS VARCHAR), ''))) = ?
                LIMIT 1
                """,
                [
                    snapshot_date,
                    normalized_symbol,
                    normalized_exchange,
                    normalized_security_type,
                ],
            ).fetchone()
            if row is not None:
                break
            missing_count += 1

        return missing_count
