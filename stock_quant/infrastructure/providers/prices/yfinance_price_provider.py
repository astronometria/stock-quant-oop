from __future__ import annotations

"""
YfinancePriceProvider

Ajouts :
- fetch_daily_prices_range_frame()

IMPORTANT :
- single-day = comportement existant
- range = vrai fetch multi-jours
- aucune logique business ici (juste accès données)
"""

from datetime import date, timedelta
from typing import List
import pandas as pd
import yfinance as yf


class YfinancePriceProvider:
    def fetch_daily_prices_frame(
        self,
        *,
        symbols: List[str],
        as_of: date,
    ) -> pd.DataFrame:
        """
        Fetch pour UNE journée (comportement historique)
        """
        start = as_of
        end = as_of + timedelta(days=1)

        df = yf.download(
            tickers=" ".join(symbols),
            start=start,
            end=end,
            interval="1d",
            progress=False,
            group_by="ticker",
            auto_adjust=False,
        )

        return self._normalize(df, symbols)

    def fetch_daily_prices_range_frame(
        self,
        *,
        symbols: List[str],
        start_date: date,
        end_date: date,
    ) -> pd.DataFrame:
        """
        Fetch multi-jours réel
        """

        # yfinance end is exclusive → +1 jour
        yf_end = end_date + timedelta(days=1)

        df = yf.download(
            tickers=" ".join(symbols),
            start=start_date,
            end=yf_end,
            interval="1d",
            progress=False,
            group_by="ticker",
            auto_adjust=False,
        )

        return self._normalize(df, symbols)

    def _normalize(self, df: pd.DataFrame, symbols: List[str]) -> pd.DataFrame:
        """
        Normalisation vers format long standardisé
        """

        if df.empty:
            return df

        records = []

        for symbol in symbols:
            try:
                sub = df[symbol]
            except Exception:
                continue

            for idx, row in sub.iterrows():
                records.append(
                    {
                        "symbol": symbol,
                        "price_date": idx.date(),
                        "open": row.get("Open"),
                        "high": row.get("High"),
                        "low": row.get("Low"),
                        "close": row.get("Close"),
                        "volume": row.get("Volume"),
                    }
                )

        return pd.DataFrame(records)
