from __future__ import annotations

import re


class NewsSymbolCandidatePolicy:
    _NON_ALNUM_RE = re.compile(r"[^A-Z0-9 ]+")

    def normalize_text(self, text: str) -> str:
        value = text.upper()
        value = self._NON_ALNUM_RE.sub(" ", value)
        value = " ".join(value.split())
        return value

    def can_use_symbol_match(self, symbol: str) -> bool:
        return len(symbol.strip()) >= 2

    def can_use_alias_match(self, alias: str) -> bool:
        alias = alias.strip()
        return len(alias) >= 4

    def alias_in_title(self, alias: str, normalized_title: str) -> bool:
        if not alias:
            return False
        pattern = f" {alias} "
        haystack = f" {normalized_title} "
        return pattern in haystack

    def symbol_in_title(self, symbol: str, normalized_title: str) -> bool:
        if not self.can_use_symbol_match(symbol):
            return False
        pattern = f" {symbol} "
        haystack = f" {normalized_title} "
        return pattern in haystack

    def score_match(self, *, match_type: str, matched_text: str) -> float:
        if match_type == "symbol":
            return 1.0
        if match_type == "alias":
            return 0.9 if len(matched_text) >= 8 else 0.75
        return 0.5
