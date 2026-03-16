"""
symbol_reference_history_service.py

Service applicatif responsable de dériver l'historique de référence symbole
depuis l'historique des listings.

But
---

Construire une couche de référence symbole historisée utilisable pour :
- matching symbole -> société
- matching nom -> société
- rattachement PIT
- future vue courante symbol_reference

Pourquoi cette couche est distincte
-----------------------------------

Même si listing_status_history contient déjà une grosse partie de l'information,
symbol_reference_history permet de stocker explicitement :
- les alias texte
- l'activation du matching symbole
- l'activation du matching nom

Cela garde le design proche du projet actuel, tout en le rendant historisé.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from hashlib import sha1

from stock_quant.domain.entities.listing_version import ListingVersion
from stock_quant.domain.entities.symbol_reference_version import SymbolReferenceVersion
from stock_quant.infrastructure.repositories.duckdb_listing_history_repository import (
    DuckDbListingHistoryRepository,
)
from stock_quant.infrastructure.repositories.duckdb_symbol_reference_history_repository import (
    DuckDbSymbolReferenceHistoryRepository,
)


@dataclass(frozen=True, slots=True)
class SymbolReferenceHistoryBuildResult:
    """
    Résumé d'exécution du service.
    """

    listings_read: int
    versions_inserted: int
    versions_closed: int
    versions_unchanged: int


class SymbolReferenceHistoryService:
    """
    Service de construction de symbol_reference_history.
    """

    def __init__(
        self,
        listing_repository: DuckDbListingHistoryRepository,
        symbol_reference_repository: DuckDbSymbolReferenceHistoryRepository,
    ) -> None:
        self.listing_repository = listing_repository
        self.symbol_reference_repository = symbol_reference_repository

    @staticmethod
    def _now_utc_naive() -> datetime:
        return datetime.now(timezone.utc).replace(tzinfo=None)

    @staticmethod
    def _strip_company_suffixes(name: str) -> str:
        """
        Retire quelques suffixes fréquents pour générer des alias simples.

        Cette logique reste volontairement modeste.
        Elle pourra être déplacée plus tard dans un service dédié de
        normalisation canonique.
        """
        value = name.strip().upper()

        suffixes = [
            " INC",
            " INC.",
            " CORPORATION",
            " CORP",
            " CORP.",
            " LTD",
            " LTD.",
            " PLC",
            " S.A.",
            " SA",
            " ADR",
            " ADS",
            ", INC.",
            ", INC",
            ", CORP.",
            ", CORP",
        ]

        changed = True
        while changed:
            changed = False
            for suffix in suffixes:
                if value.endswith(suffix):
                    value = value[: -len(suffix)].rstrip(" ,.")
                    changed = True

        return value.strip()

    def _build_aliases_json(
        self,
        listing: ListingVersion,
    ) -> str:
        """
        Construit un JSON simple d'alias textuels.
        """
        aliases: list[str] = []

        canonical = listing.company_name_clean.strip().upper()
        stripped = self._strip_company_suffixes(canonical)

        if canonical:
            aliases.append(canonical)

        if stripped and stripped != canonical:
            aliases.append(stripped)

        # Déduplication en conservant l'ordre.
        deduped: list[str] = []
        seen: set[str] = set()
        for alias in aliases:
            if alias not in seen:
                deduped.append(alias)
                seen.add(alias)

        return json.dumps(deduped, ensure_ascii=False)

    @staticmethod
    def _build_symbol_reference_id(listing: ListingVersion) -> str:
        """
        Construit un identifiant technique pour symbol_reference_history.
        """
        base = "|".join(
            [
                listing.listing_id,
                listing.normalized_symbol(),
                listing.effective_from.isoformat(),
            ]
        )
        return "SYMREF:" + sha1(base.encode("utf-8")).hexdigest()

    def _build_entity(
        self,
        listing: ListingVersion,
    ) -> SymbolReferenceVersion:
        """
        Construit une version historisée de symbol reference à partir
        d'une version de listing.
        """
        now = self._now_utc_naive()
        return SymbolReferenceVersion(
            symbol_reference_id=self._build_symbol_reference_id(listing),
            listing_id=listing.listing_id,
            company_id=listing.company_id,
            symbol=listing.symbol,
            cik=listing.cik,
            company_name=listing.company_name,
            company_name_clean=listing.company_name_clean,
            aliases_json=self._build_aliases_json(listing),
            exchange=listing.exchange,
            symbol_match_enabled=True,
            name_match_enabled=True,
            effective_from=listing.effective_from,
            effective_to=None,
            is_active=True,
            created_at=now,
            updated_at=now,
        )

    @staticmethod
    def _is_same_reference(
        current: SymbolReferenceVersion,
        candidate: SymbolReferenceVersion,
    ) -> bool:
        """
        Détermine si la référence active couvre déjà correctement le listing.
        """
        return (
            current.symbol.strip().upper() == candidate.symbol.strip().upper()
            and (current.cik or "").strip() == (candidate.cik or "").strip()
            and current.company_name_clean.strip().upper()
            == candidate.company_name_clean.strip().upper()
            and (current.exchange or "").strip().upper()
            == (candidate.exchange or "").strip().upper()
            and (current.aliases_json or "") == (candidate.aliases_json or "")
            and current.symbol_match_enabled == candidate.symbol_match_enabled
            and current.name_match_enabled == candidate.name_match_enabled
        )

    def rebuild_from_active_listings(self) -> SymbolReferenceHistoryBuildResult:
        """
        Recalcule l'historique courant de référence symbole à partir
        des versions actives des listings.
        """
        active_listings = self.listing_repository.load_all_active_listing_versions()

        listings_read = 0
        versions_inserted = 0
        versions_closed = 0
        versions_unchanged = 0

        for listing in active_listings:
            listings_read += 1

            current = self.symbol_reference_repository.find_active_by_listing_id(
                listing.listing_id
            )
            candidate = self._build_entity(listing)

            if current is None:
                self.symbol_reference_repository.insert_version(candidate)
                versions_inserted += 1
                continue

            if self._is_same_reference(current, candidate):
                versions_unchanged += 1
                continue

            now = self._now_utc_naive()
            self.symbol_reference_repository.close_active_version(
                listing_id=listing.listing_id,
                effective_to=listing.effective_from,
                updated_at=now,
            )
            versions_closed += 1

            self.symbol_reference_repository.insert_version(candidate)
            versions_inserted += 1

        return SymbolReferenceHistoryBuildResult(
            listings_read=listings_read,
            versions_inserted=versions_inserted,
            versions_closed=versions_closed,
            versions_unchanged=versions_unchanged,
        )
