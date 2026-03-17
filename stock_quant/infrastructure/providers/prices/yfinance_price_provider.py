from __future__ import annotations

"""
YfinancePriceProvider (batch + retry)

Responsabilité :
- fetch Yahoo
- batching pour éviter rate limit
"""

import time
from typing import List
import pandas as pd
import yfinance as yf
from tqdm import tqdm


class YfinancePriceProvider:
    def __init__(
        self,
        batch_size: int = 200,
        sleep_seconds: float = 1.0,
        max_retries: int = 2,
    ):
        self.batch_size = batch_size
        self.sleep_seconds = sleep_seconds
        self.max_retries = max_retries

    def _chunks(self, lst: List[str]):
        for i in range(0, len(lst), self.batch_size):
            yield lst[i:i + self.batch_size]

    def fetch(
        self,
        symbols: List[str],
        start_date,
        end_date,
        requires_range_fetch: bool,
    ) -> pd.DataFrame:

        all_frames = []

        batches = list(self._chunks(symbols))

        for batch in tqdm(batches, desc="yfinance batches"):
            df = self._fetch_batch_with_retry(
                batch,
                start_date,
                end_date,
                requires_range_fetch,
            )
            if df is not None and not df.empty:
                all_frames.append(df)

            time.sleep(self.sleep_seconds)

        if not all_frames:
            return pd.DataFrame()

        return pd.concat(all_frames, ignore_index=True)

    def _fetch_batch_with_retry(
        self,
        batch,
        start_date,
        end_date,
        requires_range_fetch,
    ):

        for attempt in range(self.max_retries + 1):
            try:
                return self._fetch_batch(batch, start_date, end_date)
            except Exception as e:
                if attempt >= self.max_retries:
                    print(f"[WARN] batch failed permanently: {e}")
                    return None

                print(f"[RETRY] attempt={attempt+1} error={e}")
                time.sleep(2 * (attempt + 1))

        return None

    def _fetch_batch(self, batch, start_date, end_date):

        data = yf.download(
            tickers=" ".join(batch),
            start=start_date,
            end=end_date,
            group_by="ticker",
            progress=False,
        )

        if data is None or data.empty:
            return None

        frames = []

        for symbol in batch:
            try:
                df = data[symbol].copy()
                df["symbol"] = symbol
                df["price_date"] = df.index.date
                frames.append(df.reset_index(drop=True))
            except Exception:
                continue

        if not frames:
            return None

        return pd.concat(frames, ignore_index=True)
