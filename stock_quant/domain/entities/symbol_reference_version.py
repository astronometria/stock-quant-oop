"""
symbol_reference_version.py

Entité domaine représentant une version historisée de la référence symbole.

Pourquoi cette entité existe
----------------------------

La table `symbol_reference` actuelle est utile pour un usage courant :
matching symbole -> société / CIK / exchange.

Mais pour une logique PIT et survivor-bias-aware, il faut historiser cette
référence, car un même ticker peut :

- apparaitre plus tard dans l'histoire
- disparaitre (delisting)
- changer d'exchange
- changer de société de rattachement
- être réutilisé dans le futur

Cette entité sert donc à construire `symbol_reference_history`, puis une vue
courante dérivée si nécessaire.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from datetime import date, datetime


@dataclass(frozen=True, slots=True)
class SymbolReferenceVersion:
    """
    Version historisée d'une référence symbole.

    aliases_json
        alias textuel sérialisé JSON.
        On le garde en string ici pour rester simple et compatible SQL.

    symbol_match_enabled
        autorise ou non les matches directs par ticker

    name_match_enabled
        autorise ou non les matches par nom société / alias
    """

    symbol_reference_id: str
    listing_id: str
    company_id: str | None
    symbol: str
    cik: str | None
    company_name: str
    company_name_clean: str
    aliases_json: str | None
    exchange: str | None
    symbol_match_enabled: bool
    name_match_enabled: bool
    effective_from: date
    effective_to: date | None
    is_active: bool
    created_at: datetime | None = None
    updated_at: datetime | None = None

    def normalized_symbol(self) -> str:
        """
        Retourne le ticker canonique.
        """
        return self.symbol.strip().upper()

    def normalized_cik(self) -> str | None:
        """
        Retourne le CIK zero-padded si présent.
        """
        if self.cik is None:
            return None

        value = self.cik.strip()
        if not value:
            return None

        return value.zfill(10)

    def is_effective_on(self, as_of_date: date) -> bool:
        """
        Vérifie si cette version de référence est valide à la date donnée.
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
    ) -> "SymbolReferenceVersion":
        """
        Ferme la version courante.
        """
        if effective_to < self.effective_from:
            raise ValueError("effective_to cannot be earlier than effective_from")

        return replace(
            self,
            effective_to=effective_to,
            is_active=False,
            updated_at=updated_at,
        )

    def matching_key(self) -> str:
        """
        Clé de matching stable pour les usages aval.

        Priorité :
        1. company_id si disponible
        2. CIK sinon
        3. fallback sur symbole + exchange

        Note :
        le fallback est moins fiable et ne doit pas être utilisé comme vérité
        ultime pour la recherche PIT.
        """
        if self.company_id and self.company_id.strip():
            return f"COMPANY:{self.company_id.strip()}"

        cik = self.normalized_cik()
        if cik:
            return f"CIK:{cik}"

        exchange = (self.exchange or "").strip().upper()
        return f"SYMBOL:{self.normalized_symbol()}|{exchange}"
