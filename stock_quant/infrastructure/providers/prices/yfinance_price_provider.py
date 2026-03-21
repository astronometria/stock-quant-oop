from __future__ import annotations

"""
Safe Yahoo Finance provider.

Objectif
--------
- respecter le contrat attendu par ProviderFrameAdapter (`fetch`)
- exposer aussi `fetch_prices` pour compatibilité
- éviter les rate limits Yahoo avec petits batches + backoff
- garder les failures consultables via `get_last_failures()`

Contrat de sortie
-----------------
Retourne un DataFrame canonique avec les colonnes:
- symbol
- price_date
- open
- high
- low
- close
- volume
- source_name

Notes
-----
- Python mince pour l'orchestration batch/retry
- yfinance fait le fetch réseau
- beaucoup de commentaires pour aider les autres développeurs
"""

import random
import time
from dataclasses import dataclass
from typing import Any, Iterable

import pandas as pd
import yfinance as yf

try:
    from tqdm import tqdm
except Exception:
    def tqdm(iterable, **kwargs):
        return iterable


# -----------------------------------------------------------------------------
# Failure model expected by build_prices.py
# -----------------------------------------------------------------------------
@dataclass(frozen=True)
class YfinanceFetchFailure:
    """
    Failure normalisé pour le pipeline prix.

    Champs utilisés plus loin:
    - provider_symbol
    - failure_reason
    - is_transient
    """

    provider_symbol: str
    failure_reason: str
    is_transient: bool


# -----------------------------------------------------------------------------
# Provider
# -----------------------------------------------------------------------------
class YfinancePriceProvider:
    """
    Provider Yahoo Finance avec throttling défensif.

    Pourquoi cette implémentation
    -----------------------------
    - Yahoo peut rate-limit sans préavis
    - de gros batches agressifs échouent plus souvent
    - on préfère des petits batches plus lents mais stables
    """

    def __init__(
        self,
        *,
        batch_size: int = 25,
        sleep_seconds: float = 3.0,
        max_retries: int = 5,
        jitter_min_seconds: float = 0.50,
        jitter_max_seconds: float = 2.00,
    ) -> None:
        self.batch_size = max(1, int(batch_size))
        self.sleep_seconds = max(0.0, float(sleep_seconds))
        self.max_retries = max(1, int(max_retries))
        self.jitter_min_seconds = max(0.0, float(jitter_min_seconds))
        self.jitter_max_seconds = max(self.jitter_min_seconds, float(jitter_max_seconds))
        self._last_failures: list[YfinanceFetchFailure] = []

    # ------------------------------------------------------------------
    # Public API expected by ProviderFrameAdapter
    # ------------------------------------------------------------------
    def fetch(
        self,
        *,
        symbols: list[str],
        start_date: str | None,
        end_date: str | None,
        requires_range_fetch: bool | None = None,
    ) -> pd.DataFrame:
        """
        Main provider entrypoint expected by ProviderFrameAdapter.
        """
        self._last_failures = []

        normalized_symbols = self._normalize_symbols(symbols)
        if not normalized_symbols:
            return self._empty_price_frame()

        batches = [
            normalized_symbols[i:i + self.batch_size]
            for i in range(0, len(normalized_symbols), self.batch_size)
        ]

        print(
            f"[yfinance] total_symbols={len(normalized_symbols)} "
            f"batches={len(batches)} batch_size={self.batch_size}",
            flush=True,
        )

        frames: list[pd.DataFrame] = []

        for batch in tqdm(batches, desc="yfinance batches"):
            batch_frame = self._fetch_batch_with_retry(
                batch=batch,
                start_date=start_date,
                end_date=end_date,
            )

            if batch_frame is not None and not batch_frame.empty:
                frames.append(batch_frame)

            # Pause volontaire entre les batches pour réduire la pression
            # sur Yahoo Finance.
            time.sleep(self.sleep_seconds)

        if not frames:
            return self._empty_price_frame()

        combined = pd.concat(frames, ignore_index=True)
        if combined.empty:
            return self._empty_price_frame()

        # Dédup défensif au cas où un rerun batch renvoie des doublons.
        combined = combined.drop_duplicates(subset=["symbol", "price_date"]).reset_index(drop=True)
        return combined

    def fetch_prices(
        self,
        *,
        symbols: list[str],
        start_date: str | None,
        end_date: str | None,
        requires_range_fetch: bool | None = None,
    ) -> pd.DataFrame:
        """
        Alias conservé pour compatibilité avec d'autres appels éventuels.
        """
        return self.fetch(
            symbols=symbols,
            start_date=start_date,
            end_date=end_date,
            requires_range_fetch=requires_range_fetch,
        )

    def get_last_failures(self) -> list[YfinanceFetchFailure]:
        """
        Retourne les failures du dernier run.
        """
        return list(self._last_failures)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _normalize_symbols(self, symbols: Iterable[str]) -> list[str]:
        """
        Normalise et déduplique les symboles.
        """
        return sorted(
            {
                str(symbol).strip().upper()
                for symbol in symbols
                if str(symbol).strip()
            }
        )

    def _fetch_batch_with_retry(
        self,
        *,
        batch: list[str],
        start_date: str | None,
        end_date: str | None,
    ) -> pd.DataFrame | None:
        """
        Fetch un batch avec retry exponentiel + jitter.

        En cas de rate limit complet:
        - on enregistre chaque symbole comme failure transitoire
        - on retourne None pour ce batch
        """
        last_error_message = ""

        for attempt in range(1, self.max_retries + 1):
            try:
                # threads=False pour être plus conservateur côté Yahoo.
                raw = yf.download(
                    tickers=" ".join(batch),
                    start=start_date,
                    end=end_date,
                    group_by="ticker",
                    auto_adjust=False,
                    progress=False,
                    threads=False,
                )

                if raw is None or raw.empty:
                    # Vide ne veut pas forcément dire erreur; on considère
                    # que le batch a répondu, puis on laisse la normalisation
                    # décider si des lignes exploitables existent.
                    return self._normalize_yfinance_output(raw=raw, batch=batch)

                return self._normalize_yfinance_output(raw=raw, batch=batch)

            except Exception as exc:
                last_error_message = str(exc)
                is_transient = self._is_transient_error(last_error_message)

                sleep_time = (2 ** attempt) + random.uniform(
                    self.jitter_min_seconds,
                    self.jitter_max_seconds,
                )

                print(
                    f"[yfinance] retry {attempt}/{self.max_retries} "
                    f"batch_size={len(batch)} "
                    f"transient={is_transient} "
                    f"sleep={sleep_time:.2f}s "
                    f"error={last_error_message}",
                    flush=True,
                )

                # Si l'erreur paraît permanente, inutile d'insister trop.
                if not is_transient:
                    break

                time.sleep(sleep_time)

        # Si on arrive ici, le batch a échoué définitivement pour ce run.
        is_transient = self._is_transient_error(last_error_message)
        for symbol in batch:
            self._last_failures.append(
                YfinanceFetchFailure(
                    provider_symbol=symbol,
                    failure_reason=last_error_message or "unknown_yfinance_error",
                    is_transient=is_transient,
                )
            )

        print(
            f"[yfinance] FAILED batch permanently size={len(batch)} error={last_error_message}",
            flush=True,
        )
        return None

    def _normalize_yfinance_output(
        self,
        *,
        raw: Any,
        batch: list[str],
    ) -> pd.DataFrame:
        """
        Convertit la sortie yfinance en DataFrame canonique.

        Gère:
        - batch multi-symboles (columns MultiIndex)
        - batch mono-symbole
        """
        if raw is None:
            return self._empty_price_frame()

        if not isinstance(raw, pd.DataFrame) or raw.empty:
            return self._empty_price_frame()

        frames: list[pd.DataFrame] = []

        # ------------------------------------------------------------------
        # Cas multi-symboles
        # ------------------------------------------------------------------
        if isinstance(raw.columns, pd.MultiIndex):
            top_level = set(raw.columns.get_level_values(0))

            for symbol in batch:
                if symbol not in top_level:
                    # Symbole absent du retour: souvent pas de data ou response
                    # partielle. On ne marque pas automatiquement comme failure
                    # permanente ici.
                    continue

                symbol_frame = raw[symbol].copy()
                canonical = self._normalize_single_symbol_frame(
                    frame=symbol_frame,
                    symbol=symbol,
                )
                if canonical is not None and not canonical.empty:
                    frames.append(canonical)

        # ------------------------------------------------------------------
        # Cas mono-symbole
        # ------------------------------------------------------------------
        else:
            symbol = batch[0]
            canonical = self._normalize_single_symbol_frame(
                frame=raw.copy(),
                symbol=symbol,
            )
            if canonical is not None and not canonical.empty:
                frames.append(canonical)

        if not frames:
            return self._empty_price_frame()

        return pd.concat(frames, ignore_index=True)

    def _normalize_single_symbol_frame(
        self,
        *,
        frame: pd.DataFrame,
        symbol: str,
    ) -> pd.DataFrame | None:
        """
        Normalise un DataFrame OHLCV pour un seul symbole.
        """
        if frame is None or frame.empty:
            return None

        working = frame.copy().reset_index()

        # yfinance renvoie normalement "Date". On garde un fallback souple.
        column_map = {str(col).strip().lower(): col for col in working.columns}

        date_col = None
        for candidate in ["date", "datetime"]:
            if candidate in column_map:
                date_col = column_map[candidate]
                break
        if date_col is None:
            return None

        required_candidates = {
            "open": "open",
            "high": "high",
            "low": "low",
            "close": "close",
            "volume": "volume",
        }

        missing_required = [src for src in required_candidates if src not in column_map]
        if missing_required:
            return None

        canonical = pd.DataFrame(
            {
                "symbol": symbol,
                "price_date": pd.to_datetime(working[date_col], errors="coerce").dt.date,
                "open": pd.to_numeric(working[column_map["open"]], errors="coerce"),
                "high": pd.to_numeric(working[column_map["high"]], errors="coerce"),
                "low": pd.to_numeric(working[column_map["low"]], errors="coerce"),
                "close": pd.to_numeric(working[column_map["close"]], errors="coerce"),
                "volume": pd.to_numeric(working[column_map["volume"]], errors="coerce").fillna(0).astype("int64"),
                "source_name": "yfinance",
            }
        )

        canonical = canonical.dropna(
            subset=["symbol", "price_date", "open", "high", "low", "close"]
        )
        if canonical.empty:
            return None

        return canonical

    def _empty_price_frame(self) -> pd.DataFrame:
        return pd.DataFrame(
            columns=[
                "symbol",
                "price_date",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "source_name",
            ]
        )

    def _is_transient_error(self, message: str) -> bool:
        """
        Détecte les erreurs réseau / throttling qui méritent un retry.
        """
        lowered = str(message or "").lower()
        transient_markers = [
            "rate limit",
            "too many requests",
            "yfratelimiterror",
            "timed out",
            "timeout",
            "temporarily unavailable",
            "connection reset",
            "remote end closed connection",
            "server error",
            "bad gateway",
            "service unavailable",
        ]
        return any(marker in lowered for marker in transient_markers)
