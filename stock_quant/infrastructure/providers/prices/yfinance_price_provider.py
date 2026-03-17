from __future__ import annotations

"""
Yahoo Finance provider robuste pour le refresh daily.

Objectifs principaux
--------------------
- Garder un design mince côté provider.
- Filtrer les symboles manifestement incompatibles avec Yahoo avant appel réseau.
- Exécuter les requêtes en petits batches pour réduire les rate limits.
- Continuer malgré les erreurs non fatales, tout en les journalisant.
- Retourner un DataFrame propre et stable pour le pipeline build_prices.

Notes importantes
-----------------
- Ce provider est volontairement "best effort" :
  on privilégie l'ingestion partielle utile plutôt qu'un échec global.
- La table canonique de sortie du pipeline demeure `price_history`.
- Ce provider ne décide pas des règles de recherche; il ne fait que récupérer
  et normaliser des prix Yahoo dans un format compatible avec le service.
"""

from dataclasses import dataclass
from datetime import date, datetime
from time import sleep
from typing import Iterable

import pandas as pd
import yfinance as yf
from tqdm import tqdm

from stock_quant.shared.exceptions import ServiceError


@dataclass(slots=True)
class YahooBatchStats:
    """
    Petit conteneur de métriques pour faciliter le debug futur.
    """
    requested_batches: int = 0
    completed_batches: int = 0
    requested_symbols: int = 0
    accepted_symbols: int = 0
    skipped_symbols: int = 0
    rows_returned: int = 0


class YfinancePriceProvider:
    """
    Provider Yahoo Finance orienté DataFrame.

    Stratégie retenue
    -----------------
    Option A choisie :
    - batch petit
    - retry souple
    - sleep entre batches
    - skip des erreurs individuelles avec logs
    """

    def __init__(
        self,
        *,
        batch_size: int = 20,
        max_retries: int = 3,
        sleep_seconds: float = 1.5,
        timeout_seconds: int = 20,
        progress_desc: str = "yfinance daily batches",
    ) -> None:
        # Taille de lot volontairement petite pour éviter de surcharger Yahoo.
        self.batch_size = max(1, int(batch_size))

        # Nombre maximal de tentatives par batch.
        self.max_retries = max(1, int(max_retries))

        # Petite pause entre batches / retries pour réduire la pression.
        self.sleep_seconds = max(0.0, float(sleep_seconds))

        # Timeout transmis à yfinance.
        self.timeout_seconds = max(1, int(timeout_seconds))

        # Libellé tqdm.
        self.progress_desc = str(progress_desc)

    # ------------------------------------------------------------------
    # API publique attendue par ProviderFrameAdapter / build_prices
    # ------------------------------------------------------------------

    def fetch_daily_prices_frame(
        self,
        *,
        symbols: list[str] | None,
        as_of: date | None = None,
    ) -> pd.DataFrame:
        """
        Retourne les prix journaliers Yahoo pour une date cible.

        Convention :
        - si `as_of` est fourni, on récupère la fenêtre [as_of, as_of + 1 jour)
        - cela correspond au comportement usuel de yfinance en daily
        """
        normalized_symbols = self._prepare_symbols(symbols)

        if not normalized_symbols:
            return self._empty_frame()

        target_date = as_of or date.today()
        start_str = target_date.isoformat()
        end_str = (pd.Timestamp(target_date) + pd.Timedelta(days=1)).date().isoformat()

        return self._fetch_batches(
            symbols=normalized_symbols,
            start_date=start_str,
            end_date=end_str,
        )

    def fetch_history_frame(
        self,
        *,
        symbols: list[str] | None,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> pd.DataFrame:
        """
        Support secondaire pour compat future backfill ciblé via Yahoo.
        """
        normalized_symbols = self._prepare_symbols(symbols)

        if not normalized_symbols:
            return self._empty_frame()

        start_str = start_date.isoformat() if start_date is not None else None
        end_str = end_date.isoformat() if end_date is not None else None

        return self._fetch_batches(
            symbols=normalized_symbols,
            start_date=start_str,
            end_date=end_str,
        )

    # ------------------------------------------------------------------
    # Coeur de récupération
    # ------------------------------------------------------------------

    def _fetch_batches(
        self,
        *,
        symbols: list[str],
        start_date: str | None,
        end_date: str | None,
    ) -> pd.DataFrame:
        stats = YahooBatchStats(
            requested_batches=len(self._chunk(symbols, self.batch_size)),
            requested_symbols=len(symbols),
            accepted_symbols=len(symbols),
            skipped_symbols=0,
        )

        batch_frames: list[pd.DataFrame] = []
        batches = self._chunk(symbols, self.batch_size)

        for batch in tqdm(
            batches,
            desc=self.progress_desc,
            unit="batch",
            dynamic_ncols=True,
            leave=False,
        ):
            frame = self._fetch_single_batch_with_retry(
                batch=batch,
                start_date=start_date,
                end_date=end_date,
            )
            stats.completed_batches += 1

            if frame is not None and not frame.empty:
                stats.rows_returned += int(len(frame))
                batch_frames.append(frame)

            # Pause légère entre lots pour limiter les erreurs 429 / rate limit.
            if self.sleep_seconds > 0:
                sleep(self.sleep_seconds)

        if not batch_frames:
            return self._empty_frame()

        merged = pd.concat(batch_frames, ignore_index=True)
        merged = self._normalize_output_frame(merged)

        return merged

    def _fetch_single_batch_with_retry(
        self,
        *,
        batch: list[str],
        start_date: str | None,
        end_date: str | None,
    ) -> pd.DataFrame | None:
        """
        Exécute un batch Yahoo avec retries.

        En mode Option A :
        - on log l'erreur
        - on réessaie quelques fois
        - si ça échoue encore, on skip le batch
        """
        last_error: Exception | None = None

        for attempt in range(1, self.max_retries + 1):
            try:
                frame = self._download_batch(
                    batch=batch,
                    start_date=start_date,
                    end_date=end_date,
                )
                return frame
            except Exception as exc:  # noqa: BLE001
                last_error = exc
                print(
                    f"[yfinance_price_provider] batch_failed "
                    f"attempt={attempt}/{self.max_retries} "
                    f"batch_size={len(batch)} "
                    f"symbols={','.join(batch[:5])}{'...' if len(batch) > 5 else ''} "
                    f"error={exc}"
                )

                if attempt < self.max_retries and self.sleep_seconds > 0:
                    sleep(self.sleep_seconds)

        print(
            f"[yfinance_price_provider] batch_skipped "
            f"batch_size={len(batch)} "
            f"symbols={','.join(batch[:5])}{'...' if len(batch) > 5 else ''} "
            f"error={last_error}"
        )
        return None

    def _download_batch(
        self,
        *,
        batch: list[str],
        start_date: str | None,
        end_date: str | None,
    ) -> pd.DataFrame:
        """
        Télécharge un batch via yfinance et retourne un DataFrame long :
        symbol, price_date, open, high, low, close, volume, source_name, ingested_at

        On utilise group_by='ticker' pour un parsing plus prévisible.
        """
        if not batch:
            return self._empty_frame()

        joined = " ".join(batch)

        raw = yf.download(
            tickers=joined,
            start=start_date,
            end=end_date,
            interval="1d",
            auto_adjust=False,
            actions=False,
            threads=False,
            group_by="ticker",
            progress=False,
            timeout=self.timeout_seconds,
        )

        if raw is None or raw.empty:
            return self._empty_frame()

        # Cas 1 : un seul ticker -> colonnes simples
        if len(batch) == 1 and not isinstance(raw.columns, pd.MultiIndex):
            return self._normalize_single_symbol_download(
                frame=raw,
                symbol=batch[0],
            )

        # Cas 2 : plusieurs tickers -> colonnes MultiIndex attendues
        if isinstance(raw.columns, pd.MultiIndex):
            long_frames: list[pd.DataFrame] = []

            # Premier niveau ou second niveau selon la structure renvoyée par yfinance.
            level0 = list(raw.columns.get_level_values(0))
            level1 = list(raw.columns.get_level_values(1))

            for symbol in batch:
                if symbol in level0:
                    symbol_frame = raw[symbol].copy()
                    normalized = self._normalize_single_symbol_download(
                        frame=symbol_frame,
                        symbol=symbol,
                    )
                    if not normalized.empty:
                        long_frames.append(normalized)
                    continue

                if symbol in level1:
                    symbol_frame = raw.xs(symbol, axis=1, level=1).copy()
                    normalized = self._normalize_single_symbol_download(
                        frame=symbol_frame,
                        symbol=symbol,
                    )
                    if not normalized.empty:
                        long_frames.append(normalized)

            if not long_frames:
                return self._empty_frame()

            return pd.concat(long_frames, ignore_index=True)

        # Fallback très défensif.
        raise ServiceError(
            f"unexpected yfinance response structure for batch of size {len(batch)}"
        )

    # ------------------------------------------------------------------
    # Préparation / filtrage des symboles
    # ------------------------------------------------------------------

    def _prepare_symbols(self, symbols: Iterable[str] | None) -> list[str]:
        """
        Normalise et filtre les symboles avant appel réseau.

        Règles retenues pour Yahoo-safe v1 :
        - trim + upper
        - exclure '$'
        - exclure '.'
        - exclure longueur > 5
        - garder le tiret '-' car plusieurs symboles US Yahoo l'utilisent
          (ex: preferred / warrants transformés côté univers en forme Yahoo-compatible)
        """
        normalized: list[str] = []
        seen: set[str] = set()

        for raw_symbol in symbols or []:
            symbol = self._normalize_symbol(raw_symbol)
            if not symbol:
                continue
            if symbol in seen:
                continue

            if not self._is_yahoo_safe_symbol(symbol):
                print(f"[yfinance_price_provider] skip_symbol symbol={symbol} reason=not_yahoo_safe")
                continue

            seen.add(symbol)
            normalized.append(symbol)

        return normalized

    def _is_yahoo_safe_symbol(self, symbol: str) -> bool:
        if not symbol:
            return False
        if "$" in symbol:
            return False
        if "." in symbol:
            return False
        if len(symbol) > 5:
            return False
        return True

    def _normalize_symbol(self, value: object) -> str:
        if value is None:
            return ""
        symbol = str(value).strip().upper()

        # Harmonisation minimale vers le style Yahoo :
        # certains systèmes internes utilisent suffixes préférentiels avec underscore.
        symbol = symbol.replace("_", "-")

        return symbol

    # ------------------------------------------------------------------
    # Normalisation DataFrame
    # ------------------------------------------------------------------

    def _normalize_single_symbol_download(
        self,
        *,
        frame: pd.DataFrame,
        symbol: str,
    ) -> pd.DataFrame:
        if frame is None or frame.empty:
            return self._empty_frame()

        normalized = frame.copy()

        # Le nom de l'index peut varier; on le remet en colonne.
        normalized = normalized.reset_index()

        rename_map = {}
        lower_map = {str(col).strip().lower(): col for col in normalized.columns}

        for src, dst in {
            "date": "price_date",
            "datetime": "price_date",
            "open": "open",
            "high": "high",
            "low": "low",
            "close": "close",
            "adj close": "adj_close",
            "adj close*": "adj_close",
            "volume": "volume",
        }.items():
            if src in lower_map:
                rename_map[lower_map[src]] = dst

        normalized = normalized.rename(columns=rename_map)

        # Si Close absent mais Adj Close présent, on reprend Adj Close.
        if "close" not in normalized.columns and "adj_close" in normalized.columns:
            normalized["close"] = normalized["adj_close"]

        required = ["price_date", "open", "high", "low", "close", "volume"]
        missing = [col for col in required if col not in normalized.columns]
        if missing:
            raise ServiceError(
                f"yfinance single-symbol frame missing required columns for {symbol}: {missing}"
            )

        normalized["symbol"] = symbol
        normalized["source_name"] = "yfinance"
        normalized["ingested_at"] = pd.Timestamp.utcnow()

        normalized = normalized[
            [
                "symbol",
                "price_date",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "source_name",
                "ingested_at",
            ]
        ]

        return normalized

    def _normalize_output_frame(self, frame: pd.DataFrame) -> pd.DataFrame:
        if frame is None or frame.empty:
            return self._empty_frame()

        normalized = frame.copy()

        normalized["symbol"] = normalized["symbol"].astype(str).str.strip().str.upper()
        normalized["price_date"] = pd.to_datetime(normalized["price_date"], errors="coerce").dt.date
        normalized["open"] = pd.to_numeric(normalized["open"], errors="coerce")
        normalized["high"] = pd.to_numeric(normalized["high"], errors="coerce")
        normalized["low"] = pd.to_numeric(normalized["low"], errors="coerce")
        normalized["close"] = pd.to_numeric(normalized["close"], errors="coerce")
        normalized["volume"] = pd.to_numeric(normalized["volume"], errors="coerce").fillna(0).astype("int64")
        normalized["source_name"] = normalized["source_name"].astype(str).str.strip().replace("", "yfinance")
        normalized["ingested_at"] = pd.to_datetime(
            normalized["ingested_at"],
            errors="coerce",
        ).fillna(pd.Timestamp.utcnow())

        # Écarter les lignes inexploitables.
        normalized = normalized.dropna(
            subset=["symbol", "price_date", "open", "high", "low", "close"]
        )

        if normalized.empty:
            return self._empty_frame()

        # Déduplication stable : on garde la dernière écriture.
        normalized = normalized.sort_values(
            by=["symbol", "price_date", "ingested_at"],
            ascending=[True, True, True],
        ).drop_duplicates(
            subset=["symbol", "price_date"],
            keep="last",
        )

        return normalized.reset_index(drop=True)

    def _empty_frame(self) -> pd.DataFrame:
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
                "ingested_at",
            ]
        )

    # ------------------------------------------------------------------
    # Utilitaires
    # ------------------------------------------------------------------

    def _chunk(self, values: list[str], chunk_size: int) -> list[list[str]]:
        if not values:
            return []
        return [values[i : i + chunk_size] for i in range(0, len(values), chunk_size)]
