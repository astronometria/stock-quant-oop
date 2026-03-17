from __future__ import annotations

"""
PriceProviderSymbolService

Responsabilité :
- prendre un scope canonique de symboles
- produire un scope provider-compatible pour Yahoo
- journaliser les exclusions et mappings

IMPORTANT :
- symbole canonique != symbole provider
- le service ne fetch rien
"""

from dataclasses import dataclass
from typing import Iterable

from stock_quant.domain.policies.yfinance_symbol_mapping_policy import (
    YfinanceSymbolMappingPolicy,
    YfinanceSymbolMappingResult,
)


@dataclass(frozen=True)
class ProviderSymbolRecord:
    canonical_symbol: str
    provider_name: str
    provider_symbol: str | None
    is_fetchable: bool
    exclusion_reason: str | None
    mapping_applied: bool


@dataclass(frozen=True)
class ProviderSymbolPlan:
    provider_name: str
    total_input_count: int
    eligible_count: int
    excluded_count: int
    mapping_applied_count: int
    records: list[ProviderSymbolRecord]

    @property
    def eligible_provider_symbols(self) -> list[str]:
        return [
            record.provider_symbol
            for record in self.records
            if record.is_fetchable and record.provider_symbol
        ]

    @property
    def canonical_to_provider(self) -> dict[str, str]:
        return {
            record.canonical_symbol: record.provider_symbol
            for record in self.records
            if record.is_fetchable and record.provider_symbol
        }

    @property
    def provider_to_canonical(self) -> dict[str, str]:
        return {
            record.provider_symbol: record.canonical_symbol
            for record in self.records
            if record.is_fetchable and record.provider_symbol
        }

    @property
    def exclusion_breakdown(self) -> dict[str, int]:
        counts: dict[str, int] = {}
        for record in self.records:
            if not record.is_fetchable:
                reason = record.exclusion_reason or "unknown"
                counts[reason] = counts.get(reason, 0) + 1
        return dict(sorted(counts.items(), key=lambda item: item[0]))


class PriceProviderSymbolService:
    def __init__(self, yfinance_policy: YfinanceSymbolMappingPolicy | None = None):
        self._yfinance_policy = yfinance_policy or YfinanceSymbolMappingPolicy()

    def build_yfinance_plan(self, canonical_symbols: Iterable[str]) -> ProviderSymbolPlan:
        """
        Transforme une liste de symboles canoniques en plan provider-compatible.

        Déduplication :
        - trim
        - upper
        - ordre trié stable
        """

        cleaned = sorted({
            str(symbol).strip().upper()
            for symbol in canonical_symbols
            if str(symbol).strip()
        })

        records: list[ProviderSymbolRecord] = []
        mapping_applied_count = 0
        eligible_count = 0
        excluded_count = 0

        for canonical_symbol in cleaned:
            mapping: YfinanceSymbolMappingResult = self._yfinance_policy.map_symbol(canonical_symbol)

            record = ProviderSymbolRecord(
                canonical_symbol=mapping.canonical_symbol,
                provider_name="yfinance",
                provider_symbol=mapping.provider_symbol,
                is_fetchable=mapping.is_fetchable,
                exclusion_reason=mapping.exclusion_reason,
                mapping_applied=mapping.mapping_applied,
            )
            records.append(record)

            if record.mapping_applied:
                mapping_applied_count += 1

            if record.is_fetchable and record.provider_symbol:
                eligible_count += 1
            else:
                excluded_count += 1

        return ProviderSymbolPlan(
            provider_name="yfinance",
            total_input_count=len(cleaned),
            eligible_count=eligible_count,
            excluded_count=excluded_count,
            mapping_applied_count=mapping_applied_count,
            records=records,
        )
