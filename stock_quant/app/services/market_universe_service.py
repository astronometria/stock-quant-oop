from __future__ import annotations

import json
from collections import defaultdict
from datetime import datetime
from typing import Iterable

from stock_quant.domain.entities.universe import RawUniverseCandidate, UniverseConflict, UniverseEntry
from stock_quant.domain.policies.exchange_normalization_policy import ExchangeNormalizationPolicy
from stock_quant.domain.policies.security_classification_policy import SecurityClassificationPolicy
from stock_quant.domain.policies.universe_conflict_resolution_policy import UniverseConflictResolutionPolicy
from stock_quant.domain.policies.universe_inclusion_policy import UniverseInclusionPolicy
from stock_quant.domain.value_objects.cik import Cik
from stock_quant.domain.value_objects.symbol import Symbol


class MarketUniverseService:
    def __init__(
        self,
        exchange_policy: ExchangeNormalizationPolicy,
        classification_policy: SecurityClassificationPolicy,
        inclusion_policy: UniverseInclusionPolicy,
        conflict_policy: UniverseConflictResolutionPolicy,
    ) -> None:
        self.exchange_policy = exchange_policy
        self.classification_policy = classification_policy
        self.inclusion_policy = inclusion_policy
        self.conflict_policy = conflict_policy

    def build(
        self,
        raw_candidates: Iterable[RawUniverseCandidate],
        *,
        allow_adr: bool = True,
    ) -> tuple[list[UniverseEntry], list[UniverseConflict], dict[str, int]]:
        prepared: list[UniverseEntry] = []
        skipped_invalid = 0

        for raw in raw_candidates:
            try:
                symbol = Symbol.from_raw(raw.symbol).value
            except Exception:
                skipped_invalid += 1
                continue

            cik = Cik.from_raw(raw.cik)
            exchange_normalized = self.exchange_policy.normalize(raw.exchange_raw)
            classification = self.classification_policy.classify(raw.company_name, raw.security_type_raw)

            include_in_universe, exclusion_reason = self.inclusion_policy.decide(
                symbol=symbol,
                exchange_normalized=exchange_normalized,
                security_type=str(classification["security_type"]),
                is_common_stock=bool(classification["is_common_stock"]),
                is_etf=bool(classification["is_etf"]),
                is_preferred=bool(classification["is_preferred"]),
                is_warrant=bool(classification["is_warrant"]),
                is_right=bool(classification["is_right"]),
                is_unit=bool(classification["is_unit"]),
                allow_adr=allow_adr,
                is_adr=bool(classification["is_adr"]),
            )

            prepared.append(
                UniverseEntry(
                    symbol=symbol,
                    company_name=(raw.company_name or "").strip() or None,
                    cik=cik.value if cik else None,
                    exchange_raw=(raw.exchange_raw or "").strip() or None,
                    exchange_normalized=exchange_normalized,
                    security_type=str(classification["security_type"]),
                    include_in_universe=include_in_universe,
                    exclusion_reason=exclusion_reason,
                    is_common_stock=bool(classification["is_common_stock"]),
                    is_adr=bool(classification["is_adr"]),
                    is_etf=bool(classification["is_etf"]),
                    is_preferred=bool(classification["is_preferred"]),
                    is_warrant=bool(classification["is_warrant"]),
                    is_right=bool(classification["is_right"]),
                    is_unit=bool(classification["is_unit"]),
                    source_name=raw.source_name,
                    as_of_date=raw.as_of_date,
                    created_at=datetime.utcnow(),
                )
            )

        grouped: dict[str, list[UniverseEntry]] = defaultdict(list)
        for entry in prepared:
            grouped[entry.symbol].append(entry)

        final_entries: list[UniverseEntry] = []
        conflicts: list[UniverseConflict] = []

        for symbol, entries in grouped.items():
            best = self.conflict_policy.choose_best(entries)
            final_entries.append(best)

            for other in entries:
                if other is best:
                    continue
                conflicts.append(
                    UniverseConflict(
                        symbol=symbol,
                        chosen_source=best.source_name,
                        rejected_source=other.source_name,
                        reason="duplicate_symbol_resolution",
                        payload_json=json.dumps(
                            {
                                "chosen_exchange": best.exchange_normalized,
                                "rejected_exchange": other.exchange_normalized,
                                "chosen_security_type": best.security_type,
                                "rejected_security_type": other.security_type,
                            },
                            sort_keys=True,
                        ),
                        created_at=datetime.utcnow(),
                    )
                )

        metrics = {
            "raw_candidates": len(prepared) + skipped_invalid,
            "prepared_entries": len(prepared),
            "skipped_invalid": skipped_invalid,
            "final_entries": len(final_entries),
            "conflicts": len(conflicts),
            "included_final_entries": sum(1 for x in final_entries if x.include_in_universe),
        }
        return final_entries, conflicts, metrics
