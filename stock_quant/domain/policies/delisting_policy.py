"""
delisting_policy.py

Policy domaine pour déterminer si une version active de listing doit être
considérée comme delisted / inactive.

Pourquoi cette policy existe
----------------------------

Une simple absence dans un snapshot ne doit pas automatiquement être
considérée comme un delisting.

Exemples de faux positifs :
- source partielle ou incomplète un jour donné
- bug de chargement source
- ticker temporairement absent d'un export
- problème réseau / fichier manquant

Cette policy introduit une logique prudente.

Principe
--------

On préfère :
- attendre plusieurs snapshots manquants
- ou disposer d'un signal explicite fort
plutôt que de fermer agressivement une version encore vivante.

Cette policy est volontairement simple pour commencer.
Elle pourra être raffinée par source / exchange / qualité de fournisseur.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date


@dataclass(frozen=True, slots=True)
class DelistingDecision:
    """
    Résultat de la policy de détection de delisting.
    """

    should_close_version: bool
    new_listing_status: str
    reason: str


class DelistingPolicy:
    """
    Policy prudente de détection de delisting.

    Paramètres
    ----------
    grace_missing_snapshots
        Nombre minimal de snapshots manquants avant de considérer
        qu'un listing est réellement absent.

    allow_immediate_explicit_delisting
        Si vrai, un signal explicite permet une fermeture immédiate.
    """

    def __init__(
        self,
        grace_missing_snapshots: int = 2,
        allow_immediate_explicit_delisting: bool = True,
    ) -> None:
        if grace_missing_snapshots < 1:
            raise ValueError("grace_missing_snapshots must be >= 1")

        self.grace_missing_snapshots = grace_missing_snapshots
        self.allow_immediate_explicit_delisting = allow_immediate_explicit_delisting

    def evaluate(
        self,
        *,
        as_of_date: date,
        consecutive_missing_snapshots: int,
        explicit_delisting_signal: bool = False,
        source_name: str | None = None,
    ) -> DelistingDecision:
        """
        Évalue si une version active doit être fermée.

        Paramètres
        ----------
        as_of_date
            Date métier du snapshot courant

        consecutive_missing_snapshots
            Nombre de snapshots consécutifs où le listing n'a pas été observé

        explicit_delisting_signal
            True si la source fournit explicitement une info de delisting
            ou d'inactivité forte

        source_name
            Nom de la source, utile pour extension future / logs

        Retour
        ------
        DelistingDecision
        """
        # ------------------------------------------------------------------
        # Cas 1 : signal explicite de delisting / inactivité forte.
        # Selon le paramètre, on peut fermer immédiatement.
        # ------------------------------------------------------------------
        if explicit_delisting_signal and self.allow_immediate_explicit_delisting:
            return DelistingDecision(
                should_close_version=True,
                new_listing_status="DELISTED",
                reason="EXPLICIT_DELISTING_SIGNAL",
            )

        # ------------------------------------------------------------------
        # Cas 2 : assez de snapshots manquants pour considérer que
        # l'absence n'est probablement pas accidentelle.
        # ------------------------------------------------------------------
        if consecutive_missing_snapshots >= self.grace_missing_snapshots:
            return DelistingDecision(
                should_close_version=True,
                new_listing_status="INACTIVE",
                reason=f"MISSING_FOR_{consecutive_missing_snapshots}_SNAPSHOTS",
            )

        # ------------------------------------------------------------------
        # Cas 3 : pas assez d'évidence pour fermer la version.
        # ------------------------------------------------------------------
        return DelistingDecision(
            should_close_version=False,
            new_listing_status="ACTIVE",
            reason="INSUFFICIENT_EVIDENCE_TO_CLOSE",
        )
