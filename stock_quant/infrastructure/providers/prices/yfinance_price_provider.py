from __future__ import annotations

"""
YfinancePriceProvider

Responsabilité :
- fetch Yahoo Finance
- batching pour limiter le rate limit
- normalisation robuste mono / multi-symboles
"""

from datetime import timedelta
import time
from typing import Iterable

import pandas as pd
import yfinance as yf
from tqdm import tqdm


class YfinancePriceProvider:
    def __init__(
        self,
        batch_size: int = 100,
        sleep_seconds: float = 1.0,
        max_retries: int = 2,
    ):
        self.batch_size = batch_size
        self.sleep_seconds = sleep_seconds
        self.max_retries = max_retries

    def _chunked(self, symbols: list[str]) -> Iterable[list[str]]:
        for i in range(0, len(symbols), self.batch_size):
            yield symbols[i:i + self.batch_size]

    def fetch(
        self,
        symbols: list[str],
        start_date,
        end_date,
        requires_range_fetch: bool,
    ) -> pd.DataFrame:
        """
        Yahoo traite `end` comme exclusif.
        Donc pour inclure `end_date`, on passe `end_date + 1 jour`.
        """

        if not symbols:
            return pd.DataFrame()

        yahoo_end_date = end_date + timedelta(days=1)

        all_frames: list[pd.DataFrame] = []
        batches = list(self._chunked(symbols))

        for batch in tqdm(batches, desc="yfinance batches"):
            batch_df = self._fetch_batch_with_retry(
                batch=batch,
                start_date=start_date,
                end_date_exclusive=yahoo_end_date,
            )
            if batch_df is not None and not batch_df.empty:
                all_frames.append(batch_df)

            time.sleep(self.sleep_seconds)

        if not all_frames:
            return pd.DataFrame()

        combined = pd.concat(all_frames, ignore_index=True)

        # Déduplication défensive au niveau provider.
        combined = combined.drop_duplicates(subset=["symbol", "price_date"]).reset_index(drop=True)

        return combined

    def _fetch_batch_with_retry(
        self,
        batch: list[str],
        start_date,
        end_date_exclusive,
    ) -> pd.DataFrame | None:
        for attempt in range(self.max_retries + 1):
            try:
                return self._fetch_batch(
                    batch=batch,
                    start_date=start_date,
                    end_date_exclusive=end_date_exclusive,
                )
            except Exception as exc:
                if attempt >= self.max_retries:
                    print(f"[WARN] yfinance batch failed permanently size={len(batch)} error={exc}")
                    return None

                sleep_for = 2.0 * (attempt + 1)
                print(
                    f"[RETRY] yfinance batch retry={attempt + 1}/{self.max_retries} "
                    f"size={len(batch)} sleep={sleep_for}s error={exc}"
                )
                time.sleep(sleep_for)

        return None

    def _fetch_batch(
        self,
        batch: list[str],
        start_date,
        end_date_exclusive,
    ) -> pd.DataFrame | None:
        raw = yf.download(
            tickers=" ".join(batch),
            start=start_date,
            end=end_date_exclusive,
            interval="1d",
            auto_adjust=False,
            progress=False,
            group_by="ticker",
            threads=False,
        )

        if raw is None or raw.empty:
            return None

        return self._normalize_download_frame(raw=raw, batch=batch)

    def _normalize_download_frame(
        self,
        raw: pd.DataFrame,
        batch: list[str],
    ) -> pd.DataFrame | None:
        """
        Normalise les 2 formes principales de yfinance :

        1) batch multi-symboles :
           colonnes MultiIndex, ex. ('AAPL', 'Open')

        2) batch mono-symbole :
           colonnes simples ou parfois MultiIndex selon version/comportement
        """

        frames: list[pd.DataFrame] = []

        # --------------------------------------------------------------
        # Cas MultiIndex
        # --------------------------------------------------------------
        if isinstance(raw.columns, pd.MultiIndex):
            level0 = set(raw.columns.get_level_values(0))

            for provider_symbol in batch:
                if provider_symbol not in level0:
                    continue

                symbol_frame = raw[provider_symbol].copy()
                normalized = self._normalize_symbol_frame(
                    symbol_frame=symbol_frame,
                    provider_symbol=provider_symbol,
                )
                if normalized is not None and not normalized.empty:
                    frames.append(normalized)

        else:
            # ----------------------------------------------------------
            # Cas colonnes simples : on suppose batch mono-symbole.
            # ----------------------------------------------------------
            if len(batch) != 1:
                return None

            provider_symbol = batch[0]
            normalized = self._normalize_symbol_frame(
                symbol_frame=raw.copy(),
                provider_symbol=provider_symbol,
            )
            if normalized is not None and not normalized.empty:
                frames.append(normalized)

        if not frames:
            return None

        return pd.concat(frames, ignore_index=True)

    def _normalize_symbol_frame(
        self,
        symbol_frame: pd.DataFrame,
        provider_symbol: str,
    ) -> pd.DataFrame | None:
        if symbol_frame is None or symbol_frame.empty:
            return None

        frame = symbol_frame.copy()

        # Harmonisation défensive des noms de colonnes.
        rename_map = {
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Adj Close": "adj_close",
            "Volume": "volume",
        }
        frame = frame.rename(columns=rename_map)

        required = ["open", "high", "low", "close", "volume"]
        missing = [col for col in required if col not in frame.columns]
        if missing:
            return None

        frame = frame.reset_index()

        # yfinance retourne souvent une colonne Date ou Datetime en index reset.
        index_col = frame.columns[0]
        frame = frame.rename(columns={index_col: "price_date"})

        frame["price_date"] = pd.to_datetime(frame["price_date"]).dt.date
        frame["symbol"] = provider_symbol

        out = frame[["symbol", "price_date", "open", "high", "low", "close", "volume"]].copy()

        # Nettoyage minimal.
        out = out.dropna(subset=["price_date", "close"])
        return out.reset_index(drop=True)
