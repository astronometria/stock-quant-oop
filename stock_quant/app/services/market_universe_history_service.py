"""
market_universe_history_service.py

Service applicatif responsable de dériver l'historique de l'univers
de marché depuis l'historique des listings.

But
---

Séparer clairement :
- l'existence d'un listing
- son éligibilité à l'univers

Pourquoi c'est important
------------------------

Un listing peut exister historiquement tout en étant exclu de l'univers :
- OTC
- ETF
- preferred
- warrant
- right
- unit
- ADR selon politique

Pour éviter le survivor bias, cette décision doit être historisée
dans le temps.

Philosophie
-----------

Ce service lit les versions actives des listings et construit une version
d'éligibilité correspondante. Plus tard, il pourra être enrichi pour traiter
des snapshots datés en bulk.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterable

from stock_quant.domain.entities.listing_version import ListingVersion
from stock_quant.domain.entities.universe_membership_version import UniverseMembershipVersion
from stock_quant.infrastructure.repositories.duckdb_listing_history_repository import (
    DuckDbListingHistoryRepository,
)
from stock_quant.infrastructure.repositories.duckdb_market_universe_history_repository import (
    DuckDbMarketUniverseHistoryRepository,
)


@dataclass(frozen=True, slots=True)
class MarketUniverseHistoryBuildResult:
    """
    Résumé d'exécution du service.
    """

    listings_read: int
    versions_inserted: int
    versions_closed: int
    versions_unchanged: int


class MarketUniverseHistoryService:
    """
    Service de construction de market_universe_history.
    """

    def __init__(
        self,
        listing_repository: DuckDbListingHistoryRepository,
        universe_repository: DuckDbMarketUniverseHistoryRepository,
        *,
        include_adr: bool = False,
        allowed_exchanges: tuple[str, ...] = ("NASDAQ", "NYSE", "AMEX"),
        rule_version: str = "v1",
    ) -> None:
        self.listing_repository = listing_repository
        self.universe_repository = universe_repository
        self.include_adr = include_adr
        self.allowed_exchanges = tuple(exchange.strip().upper() for exchange in allowed_exchanges)
        self.rule_version = rule_version

    @staticmethod
    def _now_utc_naive() -> datetime:
        return datetime.now(timezone.utc).replace(tzinfo=None)

    def _evaluate_eligibility(
        self,
        listing: ListingVersion,
    ) -> tuple[bool, str]:
        """
        Applique les règles d'inclusion/exclusion à un listing.

        Version initiale simple, très lisible, facile à faire évoluer.
        """
        exchange = (listing.exchange or "").strip().upper()
        security_type = (listing.security_type or "").strip().upper()

        if exchange not in self.allowed_exchanges:
            return False, "EXCLUDED_EXCHANGE"

        if security_type in {"ETF", "ETN", "FUND"}:
            return False, "EXCLUDED_FUND_LIKE"

        if security_type in {"PREFERRED", "PREFERRED_STOCK"}:
            return False, "EXCLUDED_PREFERRED"

        if security_type in {"WARRANT", "RIGHT", "UNIT"}:
            return False, "EXCLUDED_DERIVATIVE_LIKE"

        if security_type in {"ADR", "ADS"} and not self.include_adr:
            return False, "EXCLUDED_ADR"

        if security_type in {"COMMON", "COMMON_STOCK", ""}:
            return True, "INCLUDED_COMMON_STOCK"

        # Par prudence, si le type n'est pas reconnu, on exclut.
        return False, "EXCLUDED_UNKNOWN_SECURITY_TYPE"

    def _build_entity(
        self,
        listing: ListingVersion,
        *,
        eligible_flag: bool,
        eligible_reason: str,
    ) -> UniverseMembershipVersion:
        """
        Construit une nouvelle version d'éligibilité pour un listing.
        """
        now = self._now_utc_naive()
        return UniverseMembershipVersion(
            listing_id=listing.listing_id,
            company_id=listing.company_id,
            symbol=listing.symbol,
            cik=listing.cik,
            exchange=listing.exchange,
            security_type=listing.security_type,
            eligible_flag=eligible_flag,
            eligible_reason=eligible_reason,
            rule_version=self.rule_version,
            effective_from=listing.effective_from,
            effective_to=None,
            is_active=True,
            created_at=now,
            updated_at=now,
        )

    def rebuild_from_active_listings(self) -> MarketUniverseHistoryBuildResult:
        """
        Recalcule une version active d'éligibilité à partir des versions actives
        des listings.

        C'est une première étape pratique.
        Une version plus avancée pourra traiter tout l'historique en bulk.
        """
        active_listings = self.listing_repository.load_all_active_listing_versions()

        listings_read = 0
        versions_inserted = 0
        versions_closed = 0
        versions_unchanged = 0

        for listing in active_listings:
            listings_read += 1

            eligible_flag, eligible_reason = self._evaluate_eligibility(listing)
            current = self.universe_repository.find_active_by_listing_id(listing.listing_id)

            candidate = self._build_entity(
                listing,
                eligible_flag=eligible_flag,
                eligible_reason=eligible_reason,
            )

            if current is None:
                self.universe_repository.insert_version(candidate)
                versions_inserted += 1
                continue

            if current.same_decision_as(candidate):
                versions_unchanged += 1
                continue

            now = self._now_utc_naive()
            self.universe_repository.close_active_version(
                listing_id=listing.listing_id,
                effective_to=listing.effective_from,
                updated_at=now,
            )
            versions_closed += 1

            self.universe_repository.insert_version(candidate)
            versions_inserted += 1

        return MarketUniverseHistoryBuildResult(
            listings_read=listings_read,
            versions_inserted=versions_inserted,
            versions_closed=versions_closed,
            versions_unchanged=versions_unchanged,
        )
