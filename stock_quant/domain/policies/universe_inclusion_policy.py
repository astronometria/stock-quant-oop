from __future__ import annotations


class UniverseInclusionPolicy:
    REGULAR_EXCHANGES = {"NASDAQ", "NYSE", "NYSE_ARCA", "NYSE_AMERICAN", "BATS", "IEX"}

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
        if not symbol:
            return False, "missing_symbol"
        if exchange_normalized is None:
            return False, "missing_exchange"
        if exchange_normalized == "OTC":
            return False, "otc_security"
        if exchange_normalized not in self.REGULAR_EXCHANGES:
            return False, "non_regular_exchange"
        if is_etf:
            return False, "etf_excluded"
        if is_preferred:
            return False, "preferred_excluded"
        if is_warrant:
            return False, "warrant_excluded"
        if is_right:
            return False, "right_excluded"
        if is_unit:
            return False, "unit_excluded"
        if is_adr and not allow_adr:
            return False, "adr_excluded"
        if security_type == "UNKNOWN":
            return False, "unknown_security_type"
        if not is_common_stock and not is_adr:
            return False, "not_common_stock_like"
        return True, None
