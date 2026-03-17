from __future__ import annotations

import json
from dataclasses import replace
from datetime import datetime
from typing import Iterable

from stock_quant.domain.entities.universe import (
    RawUniverseCandidate,
    UniverseConflict,
    UniverseEntry,
)
from stock_quant.domain.policies.exchange_normalization_policy import (
    ExchangeNormalizationPolicy,
)
from stock_quant.domain.policies.security_classification_policy import (
    SecurityClassificationPolicy,
)
from stock_quant.domain.policies.universe_conflict_resolution_policy import (
    UniverseConflictResolutionPolicy,
)
from stock_quant.domain.policies.universe_inclusion_policy import (
    UniverseInclusionPolicy,
)


class MarketUniverseService:
    """
    Backward-compatible market universe service.

    Supports the legacy unit tests:
    - constructor accepts policy objects
    - build(...) returns (final_entries, conflicts, metrics)

    This service is intentionally self-contained so tests stay stable even
    while the repo evolves toward repository-driven pipelines.
    """

    def __init__(
        self,
        exchange_policy: ExchangeNormalizationPolicy | None = None,
        classification_policy: SecurityClassificationPolicy | None = None,
        inclusion_policy: UniverseInclusionPolicy | None = None,
        conflict_policy: UniverseConflictResolutionPolicy | None = None,
        repository=None,
    ) -> None:
        self.exchange_policy = exchange_policy or ExchangeNormalizationPolicy()
        self.classification_policy = classification_policy or SecurityClassificationPolicy()
        self.inclusion_policy = inclusion_policy or UniverseInclusionPolicy()
        self.conflict_policy = conflict_policy or UniverseConflictResolutionPolicy()
        self.repository = repository

    def build(
        self,
        raw_candidates: Iterable[RawUniverseCandidate],
        allow_adr: bool = True,
    ) -> tuple[list[UniverseEntry], list[UniverseConflict], dict[str, int]]:
        raw_list = list(raw_candidates)

        metrics = {
            "raw_candidates": len(raw_list),
            "skipped_invalid": 0,
            "final_entries": 0,
            "included_final_entries": 0,
        }

        valid_rows: list[RawUniverseCandidate] = []
        for row in raw_list:
            symbol = (row.symbol or "").strip().upper() if row.symbol is not None else ""
            if not symbol:
                metrics["skipped_invalid"] += 1
                continue
            valid_rows.append(
                replace(
                    row,
                    symbol=symbol,
                )
            )

        grouped: dict[str, list[RawUniverseCandidate]] = {}
        for row in valid_rows:
            grouped.setdefault(row.symbol, []).append(row)

        final_entries: list[UniverseEntry] = []
        conflicts: list[UniverseConflict] = []

        for symbol, candidates in sorted(grouped.items()):
            candidate_payloads = [self._to_entry_payload(c, allow_adr=allow_adr) for c in candidates]
            candidate_payloads.sort(key=self._entry_sort_key)

            chosen = candidate_payloads[0]
            final_entries.append(self._payload_to_entry(chosen))

            if len(candidate_payloads) > 1:
                rejected = candidate_payloads[1]
                conflicts.append(
                    UniverseConflict(
                        symbol=symbol,
                        chosen_source=str(chosen["source_name"]),
                        rejected_source=str(rejected["source_name"]),
                        reason="preferred_candidate_selected",
                        payload_json=json.dumps(
                            {
                                "chosen": chosen,
                                "rejected": rejected,
                            },
                            default=str,
                            ensure_ascii=False,
                            sort_keys=True,
                        ),
                        created_at=datetime.utcnow(),
                    )
                )

        metrics["final_entries"] = len(final_entries)
        metrics["included_final_entries"] = sum(1 for row in final_entries if row.include_in_universe)

        return final_entries, conflicts, metrics

    def _to_entry_payload(
        self,
        row: RawUniverseCandidate,
        *,
        allow_adr: bool,
    ) -> dict:
        exchange_normalized = self.exchange_policy.normalize(row.exchange_raw)
        classification = self.classification_policy.classify(
            company_name=row.company_name,
            security_type_raw=row.security_type_raw,
        )
        include_in_universe, exclusion_reason = self.inclusion_policy.decide(
            symbol=row.symbol or "",
            exchange_normalized=exchange_normalized,
            security_type=str(classification["security_type"]),
            is_common_stock=bool(classification["is_common_stock"]),
            is_etf=bool(classification["is_etf"]),
            is_preferred=bool(classification["is_preferred"]),
            is_warrant=bool(classification["is_warrant"]),
            is_right=bool(classification["is_right"]),
            is_unit=bool(classification["is_unit"]),
            is_adr=bool(classification["is_adr"]),
            allow_adr=allow_adr,
        )

        return {
            "symbol": row.symbol,
            "company_name": row.company_name,
            "cik": row.cik,
            "exchange_raw": row.exchange_raw,
            "exchange_normalized": exchange_normalized,
            "security_type": str(classification["security_type"]),
            "include_in_universe": include_in_universe,
            "exclusion_reason": exclusion_reason,
            "is_common_stock": bool(classification["is_common_stock"]),
            "is_adr": bool(classification["is_adr"]),
            "is_etf": bool(classification["is_etf"]),
            "is_preferred": bool(classification["is_preferred"]),
            "is_warrant": bool(classification["is_warrant"]),
            "is_right": bool(classification["is_right"]),
            "is_unit": bool(classification["is_unit"]),
            "source_name": row.source_name,
            "as_of_date": row.as_of_date,
        }

    def _entry_sort_key(self, payload: dict) -> tuple:
        exchange_rank_map = {
            "NASDAQ": 0,
            "NYSE": 1,
            "NYSE_AMERICAN": 2,
            "NYSEARCA": 3,
            "NYSE_ARCA": 3,
            "BATS": 4,
            "IEX": 5,
            "OTC": 99,
        }
        exchange_rank = exchange_rank_map.get(str(payload.get("exchange_normalized") or "").upper(), 50)

        return (
            0 if payload["include_in_universe"] else 1,
            0 if payload["cik"] else 1,
            exchange_rank,
            str(payload["source_name"]),
        )

    def _payload_to_entry(self, payload: dict) -> UniverseEntry:
        return UniverseEntry(
            symbol=str(payload["symbol"]),
            company_name=payload["company_name"],
            cik=payload["cik"],
            exchange_raw=payload["exchange_raw"],
            exchange_normalized=payload["exchange_normalized"],
            security_type=str(payload["security_type"]),
            include_in_universe=bool(payload["include_in_universe"]),
            exclusion_reason=payload["exclusion_reason"],
            is_common_stock=bool(payload["is_common_stock"]),
            is_adr=bool(payload["is_adr"]),
            is_etf=bool(payload["is_etf"]),
            is_preferred=bool(payload["is_preferred"]),
            is_warrant=bool(payload["is_warrant"]),
            is_right=bool(payload["is_right"]),
            is_unit=bool(payload["is_unit"]),
            source_name=str(payload["source_name"]),
            as_of_date=payload["as_of_date"],
            created_at=datetime.utcnow(),
        )
