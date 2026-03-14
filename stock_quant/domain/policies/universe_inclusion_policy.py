from __future__ import annotations

from dataclasses import dataclass


ALLOWED_EXCHANGES = {"NASDAQ", "NYSE"}


@dataclass(frozen=True)
class UniverseInclusionDecision:
    include: bool
    reason: str | None = None


def decide_universe_inclusion(
    *,
    exchange_normalized: str | None,
    is_common_stock: bool,
    is_adr: bool,
    is_etf: bool,
    is_preferred: bool,
    is_warrant: bool,
    is_right: bool,
    is_unit: bool,
) -> UniverseInclusionDecision:

    if exchange_normalized not in ALLOWED_EXCHANGES:
        return UniverseInclusionDecision(False, "exchange_not_allowed")

    if not is_common_stock:
        return UniverseInclusionDecision(False, "not_common_stock")

    if is_etf:
        return UniverseInclusionDecision(False, "etf_excluded")

    if is_adr:
        return UniverseInclusionDecision(False, "adr_excluded")

    if is_preferred:
        return UniverseInclusionDecision(False, "preferred_excluded")

    if is_warrant:
        return UniverseInclusionDecision(False, "warrant_excluded")

    if is_right:
        return UniverseInclusionDecision(False, "right_excluded")

    if is_unit:
        return UniverseInclusionDecision(False, "unit_excluded")

    return UniverseInclusionDecision(True, None)
