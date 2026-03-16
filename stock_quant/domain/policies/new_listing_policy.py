"""
new_listing_policy.py

Policy domaine responsable de déterminer si une observation représente
un nouveau listing à historiser.

Pourquoi cette policy existe
----------------------------

Dans un système survivor-bias-aware, on ne veut pas juste écraser
un snapshot courant. On veut savoir quand une nouvelle entité de listing
apparait dans le temps.

Cas typiques :
- IPO / direct listing
- ticker jamais vu auparavant
- nouveau listing d'une société déjà connue
- réapparition d'un listing après une longue absence

Cette policy ne prend pas la décision finale d'écriture en base.
Elle encapsule seulement la logique métier de détection.
"""

from __future__ import annotations

from dataclasses import dataclass

from stock_quant.domain.entities.listing_observation import ListingObservation
from stock_quant.domain.entities.listing_version import ListingVersion


@dataclass(frozen=True, slots=True)
class NewListingDecision:
    """
    Résultat de la policy de détection de nouveau listing.
    """

    is_new_listing: bool
    reason: str


class NewListingPolicy:
    """
    Policy de détection d'un nouveau listing.

    Philosophie :
    - si aucune version active connue n'existe -> nouveau listing
    - si l'identité société est connue mais la structure de listing est nouvelle
      (ticker/exchange/security_type) -> nouveau listing/version
    - sinon ce n'est pas un nouveau listing
    """

    def is_new_listing(
        self,
        observation: ListingObservation,
        active_version: ListingVersion | None,
    ) -> NewListingDecision:
        """
        Détermine si l'observation doit être traitée comme un nouveau listing.

        Paramètres
        ----------
        observation
            Observation source à analyser

        active_version
            Version active connue pour cette identité/listing, si elle existe

        Retour
        ------
        NewListingDecision
            Décision métier détaillée
        """
        # ------------------------------------------------------------------
        # Cas 1 : aucune version active connue.
        # Dans ce cas, l'observation représente un nouveau listing.
        # ------------------------------------------------------------------
        if active_version is None:
            return NewListingDecision(
                is_new_listing=True,
                reason="NO_ACTIVE_VERSION_FOUND",
            )

        # ------------------------------------------------------------------
        # Cas 2 : même société, mais structure de listing différente.
        #
        # Exemples :
        # - ticker change
        # - exchange change
        # - security_type change
        #
        # Cela signifie qu'on doit ouvrir une nouvelle version historisée.
        # ------------------------------------------------------------------
        current_fingerprint = active_version.structural_fingerprint()
        incoming_fingerprint = observation.listing_fingerprint()

        if current_fingerprint != incoming_fingerprint:
            return NewListingDecision(
                is_new_listing=True,
                reason="STRUCTURAL_CHANGE_REQUIRES_NEW_VERSION",
            )

        # ------------------------------------------------------------------
        # Cas 3 : rien n'indique un nouveau listing.
        # ------------------------------------------------------------------
        return NewListingDecision(
            is_new_listing=False,
            reason="ACTIVE_VERSION_ALREADY_COVERS_OBSERVATION",
        )
