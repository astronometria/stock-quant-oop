from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Iterable

import pandas as pd

from stock_quant.domain.entities.prices import PriceBar, RawPriceBar
from stock_quant.shared.exceptions import ServiceError


@dataclass(slots=True)
class PriceIngestionResult:
    """
    Résultat canonique de l'ingestion incrémentale des prix.

    Notes:
    - start_date / end_date sont conservés dans le contrat de retour pour
      permettre au pipeline / CLI de journaliser précisément la fenêtre demandée.
    - price_history reste la table canonique.
    - price_latest reste serving only.
    """

    requested_symbols: int
    fetched_symbols: int
    written_price_history_rows: int
    price_latest_rows_after_refresh: int
    start_date: str | None = None
    end_date: str | None = None


class PriceIngestionService:
    """
    Dual-surface price service.

    Supported modes:
    - legacy in-memory build(raw_bars, allowed_symbols=...)
    - incremental ingest_incremental(...) backed by repository/provider
    """

    def __init__(self, price_repository=None, historical_price_provider=None) -> None:
        self.price_repository = price_repository
        self.historical_price_provider = historical_price_provider

    # ------------------------------------------------------------------
    # Legacy test-compatible surface
    # ------------------------------------------------------------------

    def build(
        self,
        raw_bars: Iterable[RawPriceBar],
        *,
        allowed_symbols: set[str],
    ) -> tuple[list[PriceBar], dict[str, int]]:
        normalized_allowed = {
            self._norm_symbol(s)
            for s in allowed_symbols
            if self._norm_symbol(s)
        }

        accepted: list[PriceBar] = []
        metrics = {
            "raw_bars": 0,
            "accepted_bars": 0,
            "skipped_not_in_universe": 0,
            "skipped_invalid": 0,
        }

        for raw in raw_bars:
            metrics["raw_bars"] += 1

            symbol = self._norm_symbol(getattr(raw, "symbol", None))
            if not symbol or symbol not in normalized_allowed:
                metrics["skipped_not_in_universe"] += 1
                continue

            normalized = self._normalize_raw_bar(raw, symbol=symbol)
            if normalized is None:
                metrics["skipped_invalid"] += 1
                continue

            accepted.append(normalized)
            metrics["accepted_bars"] += 1

        return accepted, metrics

    def _normalize_raw_bar(self, raw: RawPriceBar, *, symbol: str) -> PriceBar | None:
        """
        Normalise une barre brute en appliquant des garde-fous minimaux.

        Règles:
        - price_date obligatoire
        - OHLC cohérent
        - volume non négatif
        - si close manque mais qu'une autre borne existe, on reconstruit au minimum
        """
        price_date = getattr(raw, "price_date", None)
        if price_date is None:
            return None

        close = self._to_float(getattr(raw, "close", None))
        open_ = self._to_float(getattr(raw, "open", None))
        high = self._to_float(getattr(raw, "high", None))
        low = self._to_float(getattr(raw, "low", None))
        volume = self._to_int(getattr(raw, "volume", None), default=0)
        source_name = str(getattr(raw, "source_name", "unknown") or "unknown").strip() or "unknown"

        if close is None and any(v is None for v in (open_, high, low)):
            return None

        if close is None:
            close = open_ if open_ is not None else high if high is not None else low
        if close is None:
            return None

        if open_ is None:
            open_ = close
        if high is None:
            high = close
        if low is None:
            low = close

        if high < low:
            return None
        if open_ < 0 or high < 0 or low < 0 or close < 0 or volume < 0:
            return None
        if high < max(open_, close) or low > min(open_, close):
            return None

        return PriceBar(
            symbol=symbol,
            price_date=price_date,
            open=open_,
            high=high,
            low=low,
            close=close,
            volume=volume,
            source_name=source_name,
            ingested_at=datetime.utcnow(),
        )

    # ------------------------------------------------------------------
    # Incremental repository/provider surface
    # ------------------------------------------------------------------

    def ingest_incremental(
        self,
        symbols: Iterable[str] | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> PriceIngestionResult:
        """
        Ingestion incrémentale via repository/provider.

        Important:
        - start_date / end_date sont propagés jusqu'au résultat final.
        - si aucun symbole demandé n'est admissible, on retourne un résultat vide
          mais cohérent, sans casser le contrat du pipeline.
        """
        if self.price_repository is None or self.historical_price_provider is None:
            raise ServiceError(
                "price_repository and historical_price_provider are required for incremental ingestion"
            )

        try:
            allowed_symbols = self._normalize_symbols(
                self.price_repository.list_allowed_symbols()
            )
            if not allowed_symbols:
                raise ServiceError("no allowed symbols available for price ingestion")

            requested_symbols = self._resolve_requested_symbols(
                allowed_symbols=allowed_symbols,
                requested_symbols=symbols,
            )

            if not requested_symbols:
                return PriceIngestionResult(
                    requested_symbols=0,
                    fetched_symbols=0,
                    written_price_history_rows=0,
                    price_latest_rows_after_refresh=self.price_repository.refresh_price_latest([]),
                    start_date=start_date,
                    end_date=end_date,
                )

            raw_frame = self._fetch_prices(
                symbols=requested_symbols,
                start_date=start_date,
                end_date=end_date,
            )
            normalized_frame = self._normalize_price_frame(raw_frame)

            touched_symbols: list[str] = []
            if not normalized_frame.empty:
                touched_symbols = sorted(
                    {
                        str(symbol).strip().upper()
                        for symbol in normalized_frame["symbol"].dropna().tolist()
                        if str(symbol).strip()
                    }
                )

            written_rows = self.price_repository.upsert_price_history(normalized_frame)
            latest_rows_after_refresh = self.price_repository.refresh_price_latest(
                touched_symbols
            )

            return PriceIngestionResult(
                requested_symbols=len(requested_symbols),
                fetched_symbols=len(touched_symbols),
                written_price_history_rows=written_rows,
                price_latest_rows_after_refresh=latest_rows_after_refresh,
                start_date=start_date,
                end_date=end_date,
            )

        except Exception as exc:
            if isinstance(exc, ServiceError):
                raise
            raise ServiceError(f"failed to ingest incremental prices: {exc}") from exc

    def _resolve_requested_symbols(
        self,
        allowed_symbols: list[str],
        requested_symbols: Iterable[str] | None,
    ) -> list[str]:
        """
        Filtre les symboles demandés par l'utilisateur contre l'univers admissible.
        """
        allowed_set = set(allowed_symbols)
        if requested_symbols is None:
            return allowed_symbols

        requested = self._normalize_symbols(requested_symbols)
        return [symbol for symbol in requested if symbol in allowed_set]

    def _fetch_prices(
        self,
        symbols: list[str],
        start_date: str | None,
        end_date: str | None,
    ):
        """
        Appelle le provider avec le contrat supporté.

        Le ProviderFrameAdapter utilisé par build_prices.py expose fetch_prices(...),
        donc ce chemin doit rester le premier.
        """
        fetch_method = getattr(self.historical_price_provider, "fetch_prices", None)
        if callable(fetch_method):
            return fetch_method(
                symbols=symbols,
                start_date=start_date,
                end_date=end_date,
            )

        fallback_method = getattr(self.historical_price_provider, "fetch", None)
        if callable(fallback_method):
            return fallback_method(
                symbols=symbols,
                start_date=start_date,
                end_date=end_date,
            )

        raise ServiceError("historical price provider has no supported fetch method")

    def _normalize_price_frame(self, frame: Any) -> pd.DataFrame:
        """
        Normalise les sorties providers vers le schéma canonique price_history.
        """
        if frame is None:
            return self._empty_price_frame()

        if isinstance(frame, list):
            if not frame:
                return self._empty_price_frame()
            frame = pd.DataFrame(frame)

        if not isinstance(frame, pd.DataFrame):
            raise ServiceError(f"unsupported provider frame type: {type(frame)!r}")

        if frame.empty:
            return self._empty_price_frame()

        normalized = frame.copy()
        normalized.columns = [str(col).strip() for col in normalized.columns]

        rename_map: dict[str, str] = {}
        lower_to_actual = {str(col).strip().lower(): col for col in normalized.columns}
        for src, dst in {
            "date": "price_date",
            "datetime": "price_date",
            "timestamp": "price_date",
            "adj close": "close",
            "adj_close": "close",
        }.items():
            if src in lower_to_actual and dst not in normalized.columns:
                rename_map[lower_to_actual[src]] = dst

        if rename_map:
            normalized = normalized.rename(columns=rename_map)

        required = ["symbol", "price_date", "open", "high", "low", "close", "volume"]
        missing = [name for name in required if name not in normalized.columns]
        if missing:
            raise ServiceError(f"provider frame missing required columns: {missing}")

        if "source_name" not in normalized.columns:
            normalized["source_name"] = "yfinance"
        if "ingested_at" not in normalized.columns:
            normalized["ingested_at"] = pd.Timestamp.utcnow()

        normalized["symbol"] = normalized["symbol"].astype(str).str.strip().str.upper()
        normalized["price_date"] = pd.to_datetime(
            normalized["price_date"],
            errors="coerce",
        ).dt.date
        normalized["open"] = pd.to_numeric(normalized["open"], errors="coerce")
        normalized["high"] = pd.to_numeric(normalized["high"], errors="coerce")
        normalized["low"] = pd.to_numeric(normalized["low"], errors="coerce")
        normalized["close"] = pd.to_numeric(normalized["close"], errors="coerce")
        normalized["volume"] = (
            pd.to_numeric(normalized["volume"], errors="coerce")
            .fillna(0)
            .astype("int64")
        )
        normalized["source_name"] = (
            normalized["source_name"]
            .astype(str)
            .str.strip()
            .replace("", "yfinance")
        )
        normalized["ingested_at"] = pd.to_datetime(
            normalized["ingested_at"],
            errors="coerce",
        ).fillna(pd.Timestamp.utcnow())

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
        ].dropna(subset=["symbol", "price_date", "open", "high", "low", "close"])

        if normalized.empty:
            return self._empty_price_frame()

        normalized = normalized.sort_values(
            by=["symbol", "price_date", "ingested_at"],
            ascending=[True, True, True],
        ).drop_duplicates(
            subset=["symbol", "price_date"],
            keep="last",
        )

        return normalized.reset_index(drop=True)

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
                "ingested_at",
            ]
        )

    def _normalize_symbols(self, symbols: Iterable[str]) -> list[str]:
        return sorted(
            {
                self._norm_symbol(symbol)
                for symbol in symbols
                if self._norm_symbol(symbol)
            }
        )

    def _norm_symbol(self, value: Any) -> str:
        if value is None:
            return ""
        return str(value).strip().upper()

    def _to_float(self, value: Any) -> float | None:
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    def _to_int(self, value: Any, *, default: int = 0) -> int:
        if value is None:
            return default
        try:
            return int(value)
        except (TypeError, ValueError):
            return default
