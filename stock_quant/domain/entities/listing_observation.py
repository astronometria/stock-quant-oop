"""
listing_observation.py

Entité domaine représentant une observation brute normalisée d'un listing
à une date donnée.

Pourquoi cette entité existe
----------------------------

Les données de référence sur les symboles arrivent souvent sous forme de
snapshots journaliers / périodiques provenant de fichiers source.

Exemples :
- un ticker apparait dans un fichier de référence du jour
- un ticker disparait du snapshot suivant
- un ticker change d'exchange
- un security_type change
- le company_name change légèrement

Cette entité représente UNE observation ponctuelle, et non pas une version
historisée complète.

Elle sert d'entrée au moteur qui construit :
- listing_status_history
- market_universe_history
- symbol_reference_history

Important
---------

Une observation n'est pas encore "la vérité historique".
C'est seulement un constat "vu dans la source X à la date Y".
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime


@dataclass(frozen=True, slots=True)
class ListingObservation:
    """
    Observation ponctuelle d'un listing dans une source brute.

    Champs principaux
    -----------------
    symbol
        ticker observé dans la source brute

    cik
        identifiant SEC si disponible.
        C'est souvent la meilleure clé d'identité société.

    company_name
        nom société tel que vu / normalisé

    company_name_clean
        nom société nettoyé pour matching / aliasing

    exchange
        exchange normalisé (NASDAQ / NYSE / AMEX / etc)

    security_type
        type de titre normalisé (COMMON_STOCK, ADR, ETF, ...)

    source_name
        source ayant fourni cette observation

    as_of_date
        date métier à laquelle l'observation est censée refléter l'état du marché

    ingested_at
        date/heure d'ingestion technique dans le système
    """

    symbol: str
    cik: str | None
    company_name: str
    company_name_clean: str
    exchange: str | None
    security_type: str | None
    source_name: str
    as_of_date: date
    ingested_at: datetime | None = None

    def normalized_symbol(self) -> str:
        """
        Retourne le ticker en forme canonique minimale.

        Remarque :
        on garde une logique simple ici.
        La vraie normalisation avancée peut vivre dans un service dédié.
        """
        return self.symbol.strip().upper()

    def normalized_cik(self) -> str | None:
        """
        Retourne le CIK zero-padded à 10 chiffres si présent.
        """
        if self.cik is None:
            return None

        value = self.cik.strip()
        if not value:
            return None

        return value.zfill(10)

    def has_identity_key(self) -> bool:
        """
        Indique si l'observation possède assez d'information pour être
        rattachée à une identité relativement stable.

        Règle actuelle :
        - CIK présent -> bon candidat
        - sinon ticker + company_name_clean -> candidat faible mais exploitable
        """
        if self.normalized_cik():
            return True

        return bool(self.normalized_symbol() and self.company_name_clean.strip())

    def identity_key(self) -> str:
        """
        Retourne une clé d'identité logique.

        Priorité :
        1. CIK
        2. fallback faible basé sur symbole + nom société nettoyé

        Attention :
        ce fallback n'est pas parfait.
        Il sert seulement à limiter les trous lorsque le CIK est absent.
        """
        cik = self.normalized_cik()
        if cik:
            return f"CIK:{cik}"

        return (
            "FALLBACK:"
            f"{self.normalized_symbol()}|"
            f"{self.company_name_clean.strip().upper()}"
        )

    def listing_fingerprint(self) -> str:
        """
        Retourne une empreinte de listing.

        But :
        distinguer un changement structurel du listing :
        - ticker
        - exchange
        - security_type

        Cette empreinte ne doit pas être utilisée comme clé historique finale,
        mais elle est utile pour détecter qu'une nouvelle version est nécessaire.
        """
        exchange = (self.exchange or "").strip().upper()
        security_type = (self.security_type or "").strip().upper()
        return f"{self.normalized_symbol()}|{exchange}|{security_type}"
