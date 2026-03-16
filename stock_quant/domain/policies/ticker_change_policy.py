"""
ticker_change_policy.py

Policy domaine pour détecter les changements structurels de listing,
en particulier les changements de ticker.

Pourquoi cette policy existe
----------------------------

Un ticker peut changer alors que la société reste la même.
Exemples :
- rebranding
- fusion / scission
- migration de marché
- changement de classe d'action

Un bon système PIT doit :
- préserver la continuité de la société
- fermer l'ancienne version de listing
- ouvrir une nouvelle version de listing

Le ticker seul ne doit jamais être traité comme identité ultime.
La meilleure ancre est généralement le CIK ou company_id.
"""

from __future__ import annotations

from dataclasses import dataclass

from stock_quant.domain.entities.listing_observation import ListingObservation
from stock_quant.domain.entities.listing_version import ListingVersion


@dataclass(frozen=True, slots=True)
class TickerChangeDecision:
    """
    Résultat d'évaluation d'un changement structurel.
    """

    requires_new_version: bool
    is_ticker_change: bool
    is_exchange_change: bool
    is_security_type_change: bool
    reason: str


class TickerChangePolicy:
    """
    Policy de détection de changement de structure du listing.

    Logique :
    - si le ticker change pour la même identité société -> nouvelle version
    - si l'exchange change -> nouvelle version
    - si le security_type change -> nouvelle version
    - sinon pas de nouvelle version nécessaire
    """

    def evaluate(
        self,
        *,
        active_version: ListingVersion,
        observation: ListingObservation,
    ) -> TickerChangeDecision:
        """
        Compare une observation avec une version active existante.

        Retourne une décision détaillée permettant au service applicatif
        de savoir s'il faut fermer puis rouvrir une version.
        """
        active_symbol = active_version.normalized_symbol()
        observed_symbol = observation.normalized_symbol()

        active_exchange = (active_version.exchange or "").strip().upper()
        observed_exchange = (observation.exchange or "").strip().upper()

        active_security_type = (active_version.security_type or "").strip().upper()
        observed_security_type = (observation.security_type or "").strip().upper()

        is_ticker_change = active_symbol != observed_symbol
        is_exchange_change = active_exchange != observed_exchange
        is_security_type_change = active_security_type != observed_security_type

        requires_new_version = (
            is_ticker_change
            or is_exchange_change
            or is_security_type_change
        )

        if not requires_new_version:
            return TickerChangeDecision(
                requires_new_version=False,
                is_ticker_change=False,
                is_exchange_change=False,
                is_security_type_change=False,
                reason="NO_STRUCTURAL_CHANGE",
            )

        reasons: list[str] = []
        if is_ticker_change:
            reasons.append("TICKER_CHANGE")
        if is_exchange_change:
            reasons.append("EXCHANGE_CHANGE")
        if is_security_type_change:
            reasons.append("SECURITY_TYPE_CHANGE")

        return TickerChangeDecision(
            requires_new_version=True,
            is_ticker_change=is_ticker_change,
            is_exchange_change=is_exchange_change,
            is_security_type_change=is_security_type_change,
            reason="+".join(reasons),
        )
