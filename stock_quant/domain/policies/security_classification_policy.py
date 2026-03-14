from __future__ import annotations


class SecurityClassificationPolicy:
    def classify(self, company_name: str | None, security_type_raw: str | None) -> dict[str, bool | str]:
        name = (company_name or "").upper()
        raw_type = (security_type_raw or "").upper()
        blob = f"{name} {raw_type}".strip()

        is_etf = " ETF" in f" {blob} " or "EXCHANGE TRADED FUND" in blob or raw_type == "ETF"
        is_adr = " ADR" in f" {blob} " or " ADS" in f" {blob} " or "DEPOSITARY" in blob
        is_preferred = "PREFERRED" in blob or "PREF " in blob or raw_type in {"PREFERRED", "PREF"}
        is_warrant = "WARRANT" in blob or raw_type == "WARRANT"
        is_right = " RIGHT" in f" {blob} " or " RIGHTS" in f" {blob} " or raw_type == "RIGHT"
        is_unit = " UNIT" in f" {blob} " or " UNITS" in f" {blob} " or raw_type == "UNIT"

        is_common_stock = not any([is_etf, is_preferred, is_warrant, is_right, is_unit])

        if is_etf:
            security_type = "ETF"
        elif is_adr:
            security_type = "ADR"
        elif is_preferred:
            security_type = "PREFERRED"
        elif is_warrant:
            security_type = "WARRANT"
        elif is_right:
            security_type = "RIGHT"
        elif is_unit:
            security_type = "UNIT"
        elif is_common_stock:
            security_type = "COMMON_STOCK"
        else:
            security_type = "UNKNOWN"

        return {
            "security_type": security_type,
            "is_common_stock": is_common_stock,
            "is_adr": is_adr,
            "is_etf": is_etf,
            "is_preferred": is_preferred,
            "is_warrant": is_warrant,
            "is_right": is_right,
            "is_unit": is_unit,
        }
