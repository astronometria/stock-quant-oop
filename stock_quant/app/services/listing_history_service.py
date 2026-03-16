"""
listing_history_service.py

Service applicatif chargé de construire et maintenir l'historique des listings.

Responsabilités
---------------

Ce service orchestre :

- lecture des observations source par date
- résolution de l'identité logique d'un listing
- détection d'un nouveau listing
- détection d'un changement structurel
- mise à jour du last_seen des versions actives
- fermeture prudente des versions absentes

Ce service reste volontairement mince :
- la logique de persistance est dans le repository
- les règles métier sont dans les policies
- la création des IDs techniques est faite ici de manière simple

Objectif
--------

Construire une base PIT / survivor-bias-aware réutilisable par :
- market_universe_history
- symbol_reference_history
- joins historiques aval
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from hashlib import sha1

from stock_quant.domain.entities.listing_observation import ListingObservation
from stock_quant.domain.entities.listing_version import ListingVersion
from stock_quant.domain.policies.delisting_policy import DelistingPolicy
from stock_quant.domain.policies.new_listing_policy import NewListingPolicy
from stock_quant.domain.policies.ticker_change_policy import TickerChangePolicy
from stock_quant.infrastructure.repositories.duckdb_listing_history_repository import (
    DuckDbListingHistoryRepository,
)


@dataclass(frozen=True, slots=True)
class ListingHistoryBuildResult:
    """
    Résultat d'exécution du service.
    """

    as_of_date_count: int
    observations_read: int
    versions_inserted: int
    versions_closed: int
    versions_touched: int
    skipped_observations: int


class ListingHistoryService:
    """
    Service applicatif principal de l'historisation des listings.
    """

    def __init__(
        self,
        repository: DuckDbListingHistoryRepository,
        *,
        new_listing_policy: NewListingPolicy | None = None,
        delisting_policy: DelistingPolicy | None = None,
        ticker_change_policy: TickerChangePolicy | None = None,
    ) -> None:
        self.repository = repository
        self.new_listing_policy = new_listing_policy or NewListingPolicy()
        self.delisting_policy = delisting_policy or DelistingPolicy()
        self.ticker_change_policy = ticker_change_policy or TickerChangePolicy()

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    @staticmethod
    def _now_utc_naive() -> datetime:
        """
        Retourne un datetime UTC naïf.

        Le projet existant manipule déjà largement des timestamps naïfs.
        On reste cohérent pour limiter les frictions.
        """
        return datetime.now(timezone.utc).replace(tzinfo=None)

    @staticmethod
    def _build_listing_id(observation: ListingObservation) -> str:
        """
        Génère un identifiant technique stable-ish pour une version de listing.

        Important :
        - on ne veut pas que le listing_id soit juste le ticker
        - le ticker peut être réutilisé
        - le CIK est prioritaire si disponible

        Cette stratégie est suffisante pour une première itération.
        Une version future pourrait introduire un vrai surrogate key géré en DB.
        """
        base = "|".join(
            [
                observation.identity_key(),
                observation.listing_fingerprint(),
                observation.as_of_date.isoformat(),
            ]
        )
        return "LISTING:" + sha1(base.encode("utf-8")).hexdigest()

    @staticmethod
    def _build_company_id(observation: ListingObservation) -> str | None:
        """
        Construit un company_id minimal.

        Règle actuelle :
        - si CIK présent -> company_id = CIK normalisé
        - sinon None

        Plus tard :
        - on pourra brancher ici une vraie résolution via company_reference
        """
        return observation.normalized_cik()

    def _build_new_listing_version(
        self,
        observation: ListingObservation,
        *,
        listing_status: str,
        status_reason: str,
    ) -> ListingVersion:
        """
        Construit une nouvelle version de listing active à partir d'une observation.
        """
        now = self._now_utc_naive()
        return ListingVersion(
            listing_id=self._build_listing_id(observation),
            company_id=self._build_company_id(observation),
            cik=observation.normalized_cik(),
            symbol=observation.normalized_symbol(),
            company_name=observation.company_name.strip(),
            company_name_clean=observation.company_name_clean.strip().upper(),
            exchange=(observation.exchange or "").strip().upper() or None,
            security_type=(observation.security_type or "").strip().upper() or None,
            source_name=observation.source_name.strip(),
            listing_status=listing_status,
            status_reason=status_reason,
            first_seen_at=observation.ingested_at or now,
            last_seen_at=observation.ingested_at or now,
            effective_from=observation.as_of_date,
            effective_to=None,
            is_active=True,
            created_at=now,
            updated_at=now,
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def build_all_available_dates(self) -> ListingHistoryBuildResult:
        """
        Construit l'historique pour toutes les dates disponibles dans
        symbol_reference_source_raw.
        """
        available_dates = self.repository.load_available_as_of_dates()

        observations_read = 0
        versions_inserted = 0
        versions_closed = 0
        versions_touched = 0
        skipped_observations = 0

        for as_of_date in available_dates:
            result = self.process_as_of_date(as_of_date)
            observations_read += result.observations_read
            versions_inserted += result.versions_inserted
            versions_closed += result.versions_closed
            versions_touched += result.versions_touched
            skipped_observations += result.skipped_observations

        return ListingHistoryBuildResult(
            as_of_date_count=len(available_dates),
            observations_read=observations_read,
            versions_inserted=versions_inserted,
            versions_closed=versions_closed,
            versions_touched=versions_touched,
            skipped_observations=skipped_observations,
        )

    def process_as_of_date(self, as_of_date) -> ListingHistoryBuildResult:
        """
        Traite un snapshot source pour une date donnée.

        Étapes :
        1. lire les observations
        2. pour chaque observation :
           - trouver une version active potentiellement correspondante
           - décider si on met à jour / ferme / ouvre une nouvelle version
        3. détecter les versions actives manquantes
        4. décider si elles doivent être fermées
        """
        observations = self.repository.load_listing_observations_for_date(as_of_date)

        observations_read = len(observations)
        versions_inserted = 0
        versions_closed = 0
        versions_touched = 0
        skipped_observations = 0

        # ------------------------------------------------------------------
        # Traitement des observations présentes dans le snapshot
        # ------------------------------------------------------------------
        for observation in observations:
            if not observation.has_identity_key():
                skipped_observations += 1
                continue

            active_version = self.repository.find_active_listing_version_by_identity_key(
                observation.identity_key()
            )

            new_listing_decision = self.new_listing_policy.is_new_listing(
                observation=observation,
                active_version=active_version,
            )

            if active_version is None:
                new_version = self._build_new_listing_version(
                    observation,
                    listing_status="ACTIVE",
                    status_reason=new_listing_decision.reason,
                )
                self.repository.insert_listing_version(new_version)
                versions_inserted += 1
                continue

            structural_decision = self.ticker_change_policy.evaluate(
                active_version=active_version,
                observation=observation,
            )

            if structural_decision.requires_new_version:
                now = self._now_utc_naive()

                self.repository.close_listing_version(
                    listing_id=active_version.listing_id,
                    effective_to=observation.as_of_date,
                    new_status="INACTIVE",
                    status_reason=structural_decision.reason,
                    updated_at=now,
                    last_seen_at=observation.ingested_at or now,
                )
                versions_closed += 1

                new_version = self._build_new_listing_version(
                    observation,
                    listing_status="ACTIVE",
                    status_reason=structural_decision.reason,
                )
                self.repository.insert_listing_version(new_version)
                versions_inserted += 1
                continue

            # ------------------------------------------------------------------
            # Aucun changement structurel.
            # On met simplement à jour last_seen sur la version active.
            # ------------------------------------------------------------------
            now = self._now_utc_naive()
            self.repository.touch_listing_version_last_seen(
                listing_id=active_version.listing_id,
                last_seen_at=observation.ingested_at or now,
                updated_at=now,
            )
            versions_touched += 1

        # ------------------------------------------------------------------
        # Traitement des versions actives absentes du snapshot
        # ------------------------------------------------------------------
        missing_versions = self.repository.load_missing_active_versions_for_date(as_of_date)

        for missing_version in missing_versions:
            consecutive_missing_snapshots = self.repository.count_consecutive_missing_snapshots(
                symbol=missing_version.symbol,
                exchange=missing_version.exchange,
                security_type=missing_version.security_type,
                up_to_as_of_date=as_of_date,
            )

            delisting_decision = self.delisting_policy.evaluate(
                as_of_date=as_of_date,
                consecutive_missing_snapshots=consecutive_missing_snapshots,
                explicit_delisting_signal=False,
                source_name=missing_version.source_name,
            )

            if delisting_decision.should_close_version:
                now = self._now_utc_naive()
                self.repository.close_listing_version(
                    listing_id=missing_version.listing_id,
                    effective_to=as_of_date,
                    new_status=delisting_decision.new_listing_status,
                    status_reason=delisting_decision.reason,
                    updated_at=now,
                    last_seen_at=missing_version.last_seen_at,
                )
                versions_closed += 1

        return ListingHistoryBuildResult(
            as_of_date_count=1,
            observations_read=observations_read,
            versions_inserted=versions_inserted,
            versions_closed=versions_closed,
            versions_touched=versions_touched,
            skipped_observations=skipped_observations,
        )
