"""
universe_membership_version.py

Entité domaine représentant l'état d'éligibilité d'un listing dans l'univers
de marché à travers le temps.

Pourquoi cette entité existe
----------------------------

Un listing peut exister sans être éligible à l'univers de recherche.

Exemples :
- ticker OTC -> observé, mais exclu
- preferred shares -> observé, mais exclu
- warrant / right / unit -> observé, mais exclu
- ADR -> inclus ou exclu selon politique
- common stock sur NASDAQ / NYSE -> généralement inclus

Pour éviter le survivor bias, l'éligibilité doit être historisée.
On ne veut pas seulement savoir "est-il éligible aujourd'hui ?"
mais "était-il éligible à la date D ?"
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import date, datetime


@dataclass(frozen=True, slots=True)
class UniverseMembershipVersion:
    """
    Version historisée de l'éligibilité à l'univers.

    eligible_flag
        True si le listing est inclus dans l'univers à cette période

    eligible_reason
        Motif d'inclusion ou d'exclusion
        Exemples :
        - INCLUDED_COMMON_STOCK
        - EXCLUDED_OTC
        - EXCLUDED_ETF
        - EXCLUDED_PREFERRED
        - EXCLUDED_WARRANT
    """

    listing_id: str
    company_id: str | None
    symbol: str
    cik: str | None
    exchange: str | None
    security_type: str | None
    eligible_flag: bool
    eligible_reason: str
    rule_version: str
    effective_from: date
    effective_to: date | None
    is_active: bool
    created_at: datetime | None = None
    updated_at: datetime | None = None

    def is_effective_on(self, as_of_date: date) -> bool:
        """
        Vérifie si cette version d'éligibilité est valide à la date demandée.
        """
        if as_of_date < self.effective_from:
            return False

        if self.effective_to is None:
            return True

        return as_of_date < self.effective_to

    def close(
        self,
        effective_to: date,
        updated_at: datetime | None = None,
    ) -> "UniverseMembershipVersion":
        """
        Ferme l'intervalle courant.

        Convention :
        - effective_from inclusif
        - effective_to exclusif
        """
        if effective_to < self.effective_from:
            raise ValueError("effective_to cannot be earlier than effective_from")

        return replace(
            self,
            effective_to=effective_to,
            is_active=False,
            updated_at=updated_at,
        )

    def same_decision_as(self, other: "UniverseMembershipVersion") -> bool:
        """
        Compare deux versions du point de vue métier.

        Si la décision d'inclusion/exclusion et sa justification n'ont pas
        changé, on peut souvent simplement prolonger la version active au lieu
        d'en créer une nouvelle.
        """
        return (
            self.eligible_flag == other.eligible_flag
            and self.eligible_reason == other.eligible_reason
            and (self.exchange or "").strip().upper() == (other.exchange or "").strip().upper()
            and (self.security_type or "").strip().upper()
            == (other.security_type or "").strip().upper()
        )
