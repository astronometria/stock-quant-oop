from __future__ import annotations

from dataclasses import dataclass


ALLOWED_EXCHANGES = {"NASDAQ", "NYSE", "NYSE_ARCA", "NYSEAMERICAN", "NYSE_AMERICAN"}
OTC_EXCHANGES = {"OTC", "OTCQX", "OTCQB", "OTCPINK", "PINK", "GREY", "GREY_MARKET"}


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
    allow_adr: bool = True,
) -> UniverseInclusionDecision:
    exchange = (exchange_normalized or "").strip().upper()

    if exchange in OTC_EXCHANGES:
        return UniverseInclusionDecision(False, "otc_security")

    if is_etf:
        return UniverseInclusionDecision(False, "etf_excluded")

    if is_preferred:
        return UniverseInclusionDecision(False, "preferred_excluded")

    if is_warrant:
        return UniverseInclusionDecision(False, "warrant_excluded")

    if is_right:
        return UniverseInclusionDecision(False, "right_excluded")

    if is_unit:
        return UniverseInclusionDecision(False, "unit_excluded")

    if is_adr:
        if not allow_adr:
            return UniverseInclusionDecision(False, "adr_excluded")
        if exchange and exchange not in OTC_EXCHANGES:
            return UniverseInclusionDecision(True, None)

    if exchange not in ALLOWED_EXCHANGES:
        return UniverseInclusionDecision(False, "exchange_not_allowed")

    if not is_common_stock:
        return UniverseInclusionDecision(False, "not_common_stock")

    return UniverseInclusionDecision(True, None)


class UniverseInclusionPolicy:
    def decide(
        self,
        *,
        symbol: str,
        exchange_normalized: str | None,
        security_type: str,
        is_common_stock: bool,
        is_etf: bool,
        is_preferred: bool,
        is_warrant: bool,
        is_right: bool,
        is_unit: bool,
        allow_adr: bool = True,
        is_adr: bool = False,
    ) -> tuple[bool, str | None]:
        decision = decide_universe_inclusion(
            exchange_normalized=exchange_normalized,
            is_common_stock=is_common_stock,
            is_adr=is_adr,
            is_etf=is_etf,
            is_preferred=is_preferred,
            is_warrant=is_warrant,
            is_right=is_right,
            is_unit=is_unit,
            allow_adr=allow_adr,
        )
        return decision.include, decision.reason
