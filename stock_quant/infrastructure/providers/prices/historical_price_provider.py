from __future__ import annotations

from collections.abc import Iterable
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd

from stock_quant.domain.ports.providers import PriceSourcePort
from stock_quant.infrastructure.providers.prices.raw_price_loader import RawPriceLoader


class HistoricalPriceProvider(PriceSourcePort):
    def __init__(self, source_paths: list[str | Path], loader: RawPriceLoader | None = None) -> None:
        self._source_paths = [Path(path).expanduser().resolve() for path in source_paths]
        self._loader = loader or RawPriceLoader()

    def fetch_daily_prices(self, symbols: list[str] | None, as_of: date | None = None) -> Iterable[Any]:
        end_date = as_of
        start_date = as_of
        return self.fetch_history(symbols=symbols, start_date=start_date, end_date=end_date)

    def fetch_history(
        self,
        symbols: list[str] | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> Iterable[Any]:
        frame = self._loader.load_many(self._source_paths)
        frame = self._filter_frame(frame, symbols=symbols, start_date=start_date, end_date=end_date)
        return frame.to_dict(orient="records")

    def fetch_history_frame(
        self,
        symbols: list[str] | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> pd.DataFrame:
        frame = self._loader.load_many(self._source_paths)
        return self._filter_frame(frame, symbols=symbols, start_date=start_date, end_date=end_date)

    def _filter_frame(
        self,
        frame: pd.DataFrame,
        symbols: list[str] | None,
        start_date: date | None,
        end_date: date | None,
    ) -> pd.DataFrame:
        filtered = frame.copy()

        normalized_symbols = {
            str(symbol).strip().upper()
            for symbol in (symbols or [])
            if str(symbol).strip()
        }
        if normalized_symbols:
            filtered = filtered[filtered["symbol"].isin(normalized_symbols)]

        if start_date is not None:
            filtered = filtered[filtered["price_date"] >= start_date]

        if end_date is not None:
            filtered = filtered[filtered["price_date"] <= end_date]

        return filtered.sort_values(["symbol", "price_date"]).reset_index(drop=True)
