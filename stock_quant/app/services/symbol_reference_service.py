from __future__ import annotations

import json
from datetime import datetime
from typing import Iterable

from stock_quant.domain.entities.symbol_reference import SymbolReferenceEntry
from stock_quant.domain.entities.universe import UniverseEntry
from stock_quant.domain.policies.alias_generation_policy import AliasGenerationPolicy


class SymbolReferenceService:
    def __init__(self, alias_policy: AliasGenerationPolicy) -> None:
        self.alias_policy = alias_policy

    def build(self, universe_entries: Iterable[UniverseEntry]) -> tuple[list[SymbolReferenceEntry], dict[str, int]]:
        results: list[SymbolReferenceEntry] = []

        for entry in universe_entries:
            if not entry.include_in_universe:
                continue
            if not entry.company_name:
                continue

            aliases = self.alias_policy.generate_aliases(entry.company_name)
            company_name_clean = self.alias_policy.normalize_name(entry.company_name)

            symbol_match_enabled = bool(entry.symbol)
            name_match_enabled = len(aliases) > 0 and len(company_name_clean) >= 3

            results.append(
                SymbolReferenceEntry(
                    symbol=entry.symbol,
                    cik=entry.cik,
                    company_name=entry.company_name,
                    company_name_clean=company_name_clean,
                    aliases_json=json.dumps(aliases, ensure_ascii=False, sort_keys=False),
                    exchange=entry.exchange_normalized,
                    source_name=entry.source_name,
                    symbol_match_enabled=symbol_match_enabled,
                    name_match_enabled=name_match_enabled,
                    created_at=datetime.utcnow(),
                )
            )

        metrics = {
            "input_entries": len(list(universe_entries)) if not isinstance(universe_entries, list) else len(universe_entries),
            "output_entries": len(results),
            "name_match_enabled_count": sum(1 for x in results if x.name_match_enabled),
            "symbol_match_enabled_count": sum(1 for x in results if x.symbol_match_enabled),
        }
        return results, metrics
