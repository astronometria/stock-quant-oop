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

Le CLI / pipeline devra donc injecter `uow.connection`.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Any, Iterable

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

        Hypothèse :
        - symbol_reference_source_raw contient les snapshots bruts append-only
        - on prend les lignes de la date demandée
        """
        self._require_connection()

        rows = self.con.execute(
            """
            SELECT
                TRIM(CAST(symbol AS VARCHAR)) AS symbol,
                NULLIF(TRIM(CAST(cik AS VARCHAR)), '') AS cik,
                COALESCE(TRIM(CAST(company_name AS VARCHAR)), '') AS company_name,
                COALESCE(TRIM(CAST(company_name_clean AS VARCHAR)), '') AS company_name_clean,
                NULLIF(TRIM(CAST(exchange AS VARCHAR)), '') AS exchange,
                NULLIF(TRIM(CAST(security_type AS VARCHAR)), '') AS security_type,
                COALESCE(TRIM(CAST(source_name AS VARCHAR)), '') AS source_name,
                as_of_date,
                ingested_at
            FROM symbol_reference_source_raw
            WHERE as_of_date = ?
              AND symbol IS NOT NULL
              AND TRIM(CAST(symbol AS VARCHAR)) <> ''
            ORDER BY symbol, exchange, security_type, source_name
            """,
            [as_of_date],
        ).fetchall()

        observations: list[ListingObservation] = []
        for row in rows:
            company_name_clean = row[3] or row[2].strip().upper()
            observations.append(
                ListingObservation(
                    symbol=row[0],
                    cik=row[1],
                    company_name=row[2],
                    company_name_clean=company_name_clean,
                    exchange=row[4],
                    security_type=row[5],
                    source_name=row[6],
                    as_of_date=row[7],
                    ingested_at=row[8],
                )
            )

        return observations

    def load_available_as_of_dates(self) -> list[date]:
        """
        Retourne toutes les dates disponibles dans la staging source.
        """
        self._require_connection()

        rows = self.con.execute(
            """
            SELECT DISTINCT as_of_date
            FROM symbol_reference_source_raw
            WHERE as_of_date IS NOT NULL
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

        Convention :
        - si identity_key commence par CIK:, on cherche par CIK
        - si identity_key commence par COMPANY:, on cherche par company_id
        - si fallback, on essaie un matching faible sur company_name_clean
        """
        self._require_connection()

        if identity_key.startswith("CIK:"):
            cik = identity_key.removeprefix("CIK:")
            row = self.con.execute(
                """
                SELECT
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
        """
        Insère une nouvelle version historisée.
        """
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
        """
        Ferme une version active existante.
        """
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
        """
        Met à jour le last_seen_at de la version active.
        """
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

        Cette méthode est utile pour les politiques de delisting.

        Attention :
        elle compare le snapshot du jour avec les versions actives.
        """
        self._require_connection()

        rows = self.con.execute(
            """
            WITH observed AS (
                SELECT DISTINCT
                    UPPER(TRIM(CAST(symbol AS VARCHAR))) AS symbol,
                    UPPER(TRIM(COALESCE(CAST(exchange AS VARCHAR), ''))) AS exchange,
                    UPPER(TRIM(COALESCE(CAST(security_type AS VARCHAR), ''))) AS security_type
                FROM symbol_reference_source_raw
                WHERE as_of_date = ?
                  AND symbol IS NOT NULL
                  AND TRIM(CAST(symbol AS VARCHAR)) <> ''
            )
            SELECT
                h.listing_id,
                h.company_id,
                h.cik,
                h.symbol,
                h.company_name,
                h.company_name_clean,
                h.exchange,
                h.security_type,
                h.source_name,
                h.listing_status,
                h.status_reason,
                h.first_seen_at,
                h.last_seen_at,
                h.effective_from,
                h.effective_to,
                h.is_active,
                h.created_at,
                h.updated_at
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

        Implémentation simple :
        - on récupère les dates disponibles <= up_to_as_of_date
        - on remonte à rebours
        - on s'arrête dès qu'on retrouve une observation du listing

        Ce n'est pas l'implémentation la plus optimale du monde,
        mais elle est claire et correcte pour une première itération.
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
                  AND UPPER(TRIM(COALESCE(CAST(exchange AS VARCHAR), ''))) = ?
                  AND UPPER(TRIM(COALESCE(CAST(security_type AS VARCHAR), ''))) = ?
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
