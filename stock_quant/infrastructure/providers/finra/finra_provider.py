from __future__ import annotations

from collections.abc import Iterable
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd

from stock_quant.domain.ports.providers import ShortInterestSourcePort
from stock_quant.infrastructure.providers.finra.finra_raw_loader import FinraRawLoader


class FinraProvider(ShortInterestSourcePort):
    def __init__(self, source_paths: list[str | Path], loader: FinraRawLoader | None = None) -> None:
        self._source_paths = [Path(path).expanduser().resolve() for path in source_paths]
        self._loader = loader or FinraRawLoader()

    def fetch_short_interest(
        self,
        symbols: list[str] | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> Iterable[Any]:
        frame = self.fetch_short_interest_frame(
            symbols=symbols,
            start_date=start_date,
            end_date=end_date,
        )
        return frame.to_dict(orient="records")

    def fetch_short_interest_frame(
        self,
        symbols: list[str] | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> pd.DataFrame:
        frame = self._loader.load_many(self._source_paths)
        if frame.empty:
            return frame

        filtered = frame.copy()

        normalized_symbols = {symbol.strip().upper() for symbol in (symbols or []) if str(symbol).strip()}
        if normalized_symbols and "symbol" in filtered.columns:
            filtered = filtered[filtered["symbol"].isin(normalized_symbols)]

        if start_date is not None and "settlement_date" in filtered.columns:
            filtered = filtered[filtered["settlement_date"] >= start_date]

        if end_date is not None and "settlement_date" in filtered.columns:
            filtered = filtered[filtered["settlement_date"] <= end_date]

        return filtered.reset_index(drop=True)
