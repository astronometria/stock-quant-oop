"""
listing_version.py

Entité domaine représentant une version historisée d'un listing.

Pourquoi cette entité existe
----------------------------

Contrairement à ListingObservation, qui est une photo ponctuelle d'un
snapshot source, ListingVersion représente un intervalle de validité.

C'est l'objet métier central pour gérer :
- new listings
- delistings
- ticker changes
- exchange changes
- security_type changes

En d'autres termes :
- ListingObservation = "vu à telle date"
- ListingVersion = "valide de telle date à telle date"

Philosophie PIT
---------------

Un pipeline quant sérieux doit pouvoir répondre à la question :

"Quel était l'état valide de ce listing à la date D ?"

C'est exactement le rôle de cette entité.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import date, datetime


@dataclass(frozen=True, slots=True)
class ListingVersion:
    """
    Version historisée d'un listing.

    Notes :
    - effective_to = None signifie "toujours actif / ouvert"
    - is_active est stocké explicitement pour simplifier certains filtres SQL
    """

    listing_id: str
    company_id: str | None
    cik: str | None
    symbol: str
    company_name: str
    company_name_clean: str
    exchange: str | None
    security_type: str | None
    source_name: str
    listing_status: str
    status_reason: str | None
    first_seen_at: datetime | None
    last_seen_at: datetime | None
    effective_from: date
    effective_to: date | None
    is_active: bool
    created_at: datetime | None = None
    updated_at: datetime | None = None

    def normalized_symbol(self) -> str:
        """
        Symbole canonique minimal.
        """
        return self.symbol.strip().upper()

    def normalized_cik(self) -> str | None:
        """
        CIK canonique zero-padded si disponible.
        """
        if self.cik is None:
            return None

        value = self.cik.strip()
        if not value:
            return None

        return value.zfill(10)

    def is_open_ended(self) -> bool:
        """
        True si l'intervalle est encore ouvert.
        """
        return self.effective_to is None

    def is_effective_on(self, as_of_date: date) -> bool:
        """
        Indique si cette version est valide à une date donnée.
        """
        if as_of_date < self.effective_from:
            return False

        if self.effective_to is None:
            return True

        return as_of_date < self.effective_to

    def close(
        self,
        effective_to: date,
        status_reason: str | None,
        updated_at: datetime | None = None,
    ) -> "ListingVersion":
        """
        Ferme cette version.

        Convention choisie :
        - effective_from inclusif
        - effective_to exclusif

        Exemple :
        - version A valide à partir du 2025-01-01
        - si elle cesse avant la version suivante le 2025-03-01
          alors effective_to = 2025-03-01
        """
        if effective_to < self.effective_from:
            raise ValueError("effective_to cannot be earlier than effective_from")

        return replace(
            self,
            effective_to=effective_to,
            is_active=False,
            status_reason=status_reason,
            updated_at=updated_at,
        )

    def reopen(
        self,
        last_seen_at: datetime | None,
        updated_at: datetime | None = None,
    ) -> "ListingVersion":
        """
        Rouvre une version si nécessaire.

        Peu utilisé dans un modèle strict d'intervalle, mais utile si une source
        a produit un faux négatif temporaire et qu'on décide finalement que
        la version doit rester ouverte.
        """
        return replace(
            self,
            effective_to=None,
            is_active=True,
            last_seen_at=last_seen_at,
            updated_at=updated_at,
        )

    def with_last_seen(self, last_seen_at: datetime | None) -> "ListingVersion":
        """
        Met à jour la dernière date d'observation.
        """
        return replace(self, last_seen_at=last_seen_at)

    def structural_fingerprint(self) -> str:
        """
        Empreinte structurelle du listing.

        Sert à détecter qu'on doit clore une version et en ouvrir une nouvelle
        si la structure du listing change.
        """
        exchange = (self.exchange or "").strip().upper()
        security_type = (self.security_type or "").strip().upper()
        return f"{self.normalized_symbol()}|{exchange}|{security_type}"

    def company_identity_key(self) -> str:
        """
        Clé d'identité société.

        Priorité :
        1. company_id si disponible
        2. CIK sinon
        3. fallback très faible sur nom société nettoyé
        """
        if self.company_id and self.company_id.strip():
            return f"COMPANY:{self.company_id.strip()}"

        cik = self.normalized_cik()
        if cik:
            return f"CIK:{cik}"

        return f"NAME:{self.company_name_clean.strip().upper()}"
