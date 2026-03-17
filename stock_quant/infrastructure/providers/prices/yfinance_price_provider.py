from __future__ import annotations

"""
YfinancePriceProvider

Responsabilités :
- fetch Yahoo Finance
- batching pour limiter le rate limit
- normalisation robuste mono / multi-symboles
- capture des erreurs provider par symbole quand yfinance les expose

IMPORTANT :
- `end` est exclusif chez Yahoo => on ajoute +1 jour
- les symboles renvoyés par ce provider restent des symboles provider
- le remap vers symbole canonique se fait dans l'adapter
"""

from dataclasses import dataclass
from datetime import timedelta
import time
from typing import Iterable

import pandas as pd
import yfinance as yf
from tqdm import tqdm

try:
    # API interne yfinance, utile pour récupérer les erreurs par ticker.
    # Si cette API change, le provider continuera de fonctionner avec un fallback.
    import yfinance.shared as yf_shared
except Exception:  # pragma: no cover - dépend du runtime
    yf_shared = None


@dataclass(frozen=True)
class YfinanceFetchFailure:
    provider_symbol: str
    failure_reason: str
    is_transient: bool


TRANSIENT_ERROR_KEYWORDS = [
    "too many requests",
    "rate limited",
    "rate limit",
    "timeout",
    "timed out",
    "connection",
    "network",
    "temporarily unavailable",
    "temporarily down",
]

PERMANENT_ERROR_KEYWORDS = [
    "possibly delisted",
    "no timezone found",
    "no price data found",
    "quote not found",
    "symbol may be delisted",
]


def is_transient_error_message(message: str) -> bool:
    text = (message or "").strip().lower()
    return any(keyword in text for keyword in TRANSIENT_ERROR_KEYWORDS)


def is_permanent_error_message(message: str) -> bool:
    text = (message or "").strip().lower()
    return any(keyword in text for keyword in PERMANENT_ERROR_KEYWORDS)


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
        self._last_failures: list[YfinanceFetchFailure] = []

    def get_last_failures(self) -> list[YfinanceFetchFailure]:
        """
        Retourne les failures du dernier fetch global.
        """
        return list(self._last_failures)

    def _reset_failures(self) -> None:
        self._last_failures = []

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
        self._reset_failures()

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
        combined = combined.drop_duplicates(subset=["symbol", "price_date"]).reset_index(drop=True)

        return combined

    def _fetch_batch_with_retry(
        self,
        batch: list[str],
        start_date,
        end_date_exclusive,
    ) -> pd.DataFrame | None:
        for attempt in range(self.max_retries + 1):
            batch_result = self._fetch_batch(
                batch=batch,
                start_date=start_date,
                end_date_exclusive=end_date_exclusive,
            )

            # Si aucune erreur transitoire n'est présente, on accepte le résultat.
            transient_failures = [f for f in batch_result["failures"] if f.is_transient]
            if not transient_failures:
                self._last_failures.extend(batch_result["failures"])
                return batch_result["frame"]

            # Si on a des erreurs transitoires mais plus de retries disponibles,
            # on garde quand même les failures observées.
            if attempt >= self.max_retries:
                self._last_failures.extend(batch_result["failures"])
                return batch_result["frame"]

            sleep_for = 2.0 * (attempt + 1)
            print(
                f"[RETRY] yfinance transient retry={attempt + 1}/{self.max_retries} "
                f"batch_size={len(batch)} transient_failures={len(transient_failures)} "
                f"sleep={sleep_for}s"
            )
            time.sleep(sleep_for)

        return None

    def _fetch_batch(
        self,
        batch: list[str],
        start_date,
        end_date_exclusive,
    ) -> dict:
        self._clear_yfinance_shared_errors()

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

        frame = self._normalize_download_frame(raw=raw, batch=batch)
        returned_symbols = set(frame["symbol"].astype(str).unique()) if frame is not None and not frame.empty else set()

        captured_errors = self._collect_yfinance_shared_errors()

        failures: list[YfinanceFetchFailure] = []

        # 1) erreurs explicites remontées par yfinance
        for provider_symbol, message in captured_errors.items():
            failures.append(
                YfinanceFetchFailure(
                    provider_symbol=provider_symbol,
                    failure_reason=message,
                    is_transient=is_transient_error_message(message),
                )
            )

        # 2) fallback : symbole demandé mais absent, sans message explicite
        captured_symbols = set(captured_errors.keys())
        missing_without_explicit_error = [
            provider_symbol
            for provider_symbol in batch
            if provider_symbol not in returned_symbols and provider_symbol not in captured_symbols
        ]
        for provider_symbol in missing_without_explicit_error:
            failures.append(
                YfinanceFetchFailure(
                    provider_symbol=provider_symbol,
                    failure_reason="no_data_returned",
                    is_transient=False,
                )
            )

        return {
            "frame": frame,
            "failures": failures,
        }

    def _clear_yfinance_shared_errors(self) -> None:
        if yf_shared is None:
            return
        try:
            if hasattr(yf_shared, "_ERRORS") and isinstance(yf_shared._ERRORS, dict):
                yf_shared._ERRORS.clear()
            if hasattr(yf_shared, "_TRACEBACKS") and isinstance(yf_shared._TRACEBACKS, dict):
                yf_shared._TRACEBACKS.clear()
        except Exception:
            # Le provider doit rester robuste même si l'API interne change.
            return

    def _collect_yfinance_shared_errors(self) -> dict[str, str]:
        if yf_shared is None:
            return {}

        try:
            errors = getattr(yf_shared, "_ERRORS", {})
            if not isinstance(errors, dict):
                return {}

            normalized: dict[str, str] = {}
            for key, value in errors.items():
                provider_symbol = str(key).strip().upper()
                message = str(value).strip()
                if provider_symbol and message:
                    normalized[provider_symbol] = message
            return normalized
        except Exception:
            return {}

    def _normalize_download_frame(
        self,
        raw: pd.DataFrame,
        batch: list[str],
    ) -> pd.DataFrame:
        """
        Normalise les 2 formes principales de yfinance :

        1) batch multi-symboles :
           colonnes MultiIndex, ex. ('AAPL', 'Open')

        2) batch mono-symbole :
           colonnes simples ou parfois MultiIndex selon version/comportement
        """
        if raw is None or raw.empty:
            return pd.DataFrame()

        frames: list[pd.DataFrame] = []

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
            if len(batch) != 1:
                return pd.DataFrame()

            provider_symbol = batch[0]
            normalized = self._normalize_symbol_frame(
                symbol_frame=raw.copy(),
                provider_symbol=provider_symbol,
            )
            if normalized is not None and not normalized.empty:
                frames.append(normalized)

        if not frames:
            return pd.DataFrame()

        return pd.concat(frames, ignore_index=True)

    def _normalize_symbol_frame(
        self,
        symbol_frame: pd.DataFrame,
        provider_symbol: str,
    ) -> pd.DataFrame | None:
        if symbol_frame is None or symbol_frame.empty:
            return None

        frame = symbol_frame.copy()

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
        index_col = frame.columns[0]
        frame = frame.rename(columns={index_col: "price_date"})

        frame["price_date"] = pd.to_datetime(frame["price_date"]).dt.date
        frame["symbol"] = provider_symbol

        out = frame[["symbol", "price_date", "open", "high", "low", "close", "volume"]].copy()
        out = out.dropna(subset=["price_date", "close"])

        return out.reset_index(drop=True)
