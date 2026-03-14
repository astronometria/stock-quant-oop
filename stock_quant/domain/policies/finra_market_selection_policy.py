from __future__ import annotations

from stock_quant.shared.enums import SourceMarket


class FinraMarketSelectionPolicy:
    def include_source_market(self, configured_market: str, row_source_market: str) -> bool:
        configured = (configured_market or "").strip().lower()
        row_market = (row_source_market or "").strip().lower()

        if configured == SourceMarket.BOTH.value:
            return row_market in {SourceMarket.REGULAR.value, SourceMarket.OTC.value}
        if configured == SourceMarket.REGULAR.value:
            return row_market == SourceMarket.REGULAR.value
        if configured == SourceMarket.OTC.value:
            return row_market == SourceMarket.OTC.value
        return False
