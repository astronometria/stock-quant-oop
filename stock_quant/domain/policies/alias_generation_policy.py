from __future__ import annotations

import re


class AliasGenerationPolicy:
    _SPACE_RE = re.compile(r"\s+")
    _PUNCT_RE = re.compile(r"[^A-Z0-9 ]+")
    _SUFFIXES = [
        " INCORPORATED",
        " INC",
        " CORPORATION",
        " CORP",
        " COMPANY",
        " CO",
        " LIMITED",
        " LTD",
        " HOLDINGS",
        " HOLDING",
        " GROUP",
        " PLC",
        " SA",
        " NV",
        " AG",
        " LP",
        " LLC",
        " ADR",
        " ADS",
        " TRUST",
    ]

    def normalize_name(self, company_name: str) -> str:
        value = company_name.strip().upper()
        value = self._PUNCT_RE.sub(" ", value)
        value = self._SPACE_RE.sub(" ", value).strip()
        return value

    def strip_suffixes(self, normalized_name: str) -> str:
        value = normalized_name
        changed = True
        while changed and value:
            changed = False
            for suffix in self._SUFFIXES:
                if value.endswith(suffix):
                    value = value[: -len(suffix)].strip()
                    changed = True
        return value

    def generate_aliases(self, company_name: str) -> list[str]:
        normalized = self.normalize_name(company_name)
        stripped = self.strip_suffixes(normalized)

        aliases: list[str] = []
        for candidate in [normalized, stripped]:
            candidate = candidate.strip()
            if candidate and candidate not in aliases:
                aliases.append(candidate)

        if stripped:
            compact = stripped.replace(" ", "")
            if compact and compact not in aliases:
                aliases.append(compact)

        return aliases
