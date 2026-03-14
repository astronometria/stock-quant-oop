from __future__ import annotations

from collections.abc import Iterable
from datetime import date
from typing import Any

from stock_quant.domain.ports.providers import PriceSourcePort


class YfinancePriceProvider(PriceSourcePort):
    def fetch_daily_prices(self, symbols: list[str], as_of: date | None = None) -> Iterable[Any]:
        raise NotImplementedError("Implémentation yfinance à brancher en passe 3.")

    def fetch_history(
        self,
        symbols: list[str],
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> Iterable[Any]:
        raise NotImplementedError("Implémentation yfinance à brancher en passe 3.")
