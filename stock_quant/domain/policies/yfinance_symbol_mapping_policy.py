from __future__ import annotations

"""
YfinanceSymbolMappingPolicy

Responsabilité :
- convertir un symbole canonique interne vers une forme Yahoo compatible
- décider si le symbole est éligible au fetch Yahoo standard

IMPORTANT :
- ne jamais modifier le symbole canonique persistant en base
- le mapping provider est temporaire et sert uniquement au fetch
"""

from dataclasses import dataclass
import re


@dataclass(frozen=True)
class YfinanceSymbolMappingResult:
    canonical_symbol: str
    provider_symbol: str | None
    is_fetchable: bool
    exclusion_reason: str | None
    mapping_applied: bool


class YfinanceSymbolMappingPolicy:
    """
    Politique prudente pour Yahoo Finance.

    Hypothèses opérationnelles :
    - common stocks et classes d'actions simples : généralement fetchables
    - rights / warrants / units / preferreds exotiques : exclus par défaut
    - certains symboles avec '.' doivent être convertis en '-'
    """

    _CLASS_SHARE_RE = re.compile(r"^[A-Z0-9]+\.[A-Z]$")
    _HAS_DOLLAR_RE = re.compile(r"\$")
    _RIGHTS_RE = re.compile(r"(^|[-\.])(R|RI|RT)$")
    _WARRANTS_RE = re.compile(r"(^|[-\.])(W|WT)$")
    _UNITS_RE = re.compile(r"(^|[-\.])UN$")
    _PREFERRED_SUFFIX_RE = re.compile(r"(^|[-\.])P[A-Z]?$")

    def map_symbol(self, canonical_symbol: str) -> YfinanceSymbolMappingResult:
        """
        Retourne la décision provider pour un symbole canonique.

        Règles :
        - trim + upper
        - conversion ciblée . -> - pour les classes d'actions simples
        - exclusion prudente des syntaxes Yahoo instables
        """

        raw = (canonical_symbol or "").strip().upper()

        if not raw:
            return YfinanceSymbolMappingResult(
                canonical_symbol=raw,
                provider_symbol=None,
                is_fetchable=False,
                exclusion_reason="empty_symbol",
                mapping_applied=False,
            )

        # ------------------------------------------------------------------
        # Exclusions prudentes : Yahoo est souvent instable sur ces suffixes.
        # ------------------------------------------------------------------
        if self._HAS_DOLLAR_RE.search(raw):
            return YfinanceSymbolMappingResult(
                canonical_symbol=raw,
                provider_symbol=None,
                is_fetchable=False,
                exclusion_reason="contains_dollar_syntax",
                mapping_applied=False,
            )

        if self._RIGHTS_RE.search(raw):
            return YfinanceSymbolMappingResult(
                canonical_symbol=raw,
                provider_symbol=None,
                is_fetchable=False,
                exclusion_reason="rights_not_supported",
                mapping_applied=False,
            )

        if self._WARRANTS_RE.search(raw):
            return YfinanceSymbolMappingResult(
                canonical_symbol=raw,
                provider_symbol=None,
                is_fetchable=False,
                exclusion_reason="warrants_not_supported",
                mapping_applied=False,
            )

        if self._UNITS_RE.search(raw):
            return YfinanceSymbolMappingResult(
                canonical_symbol=raw,
                provider_symbol=None,
                is_fetchable=False,
                exclusion_reason="units_not_supported",
                mapping_applied=False,
            )

        if self._PREFERRED_SUFFIX_RE.search(raw):
            return YfinanceSymbolMappingResult(
                canonical_symbol=raw,
                provider_symbol=None,
                is_fetchable=False,
                exclusion_reason="preferred_not_supported_by_default",
                mapping_applied=False,
            )

        # ------------------------------------------------------------------
        # Mapping ciblé des classes d'actions simples.
        # Exemples : BRK.B -> BRK-B, BF.A -> BF-A
        # ------------------------------------------------------------------
        if self._CLASS_SHARE_RE.match(raw):
            mapped = raw.replace(".", "-")
            return YfinanceSymbolMappingResult(
                canonical_symbol=raw,
                provider_symbol=mapped,
                is_fetchable=True,
                exclusion_reason=None,
                mapping_applied=(mapped != raw),
            )

        # ------------------------------------------------------------------
        # Cas par défaut : on garde le symbole tel quel.
        # ------------------------------------------------------------------
        return YfinanceSymbolMappingResult(
            canonical_symbol=raw,
            provider_symbol=raw,
            is_fetchable=True,
            exclusion_reason=None,
            mapping_applied=False,
        )
