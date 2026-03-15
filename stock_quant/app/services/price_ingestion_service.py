from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable

import pandas as pd

from stock_quant.shared.exceptions import ServiceError


@dataclass(slots=True)
class PriceIngestionResult:
    requested_symbols: int
    fetched_symbols: int
    written_price_history_rows: int
    price_latest_rows_after_refresh: int


class PriceIngestionService:
    """
    Application service for incremental canonical price ingestion.

    Responsibilities:
    - determine allowed symbols from repository
    - normalize provider output into canonical dataframe
    - upsert incremental rows into price_history
    - refresh price_latest only for touched symbols
    """

    def __init__(self, price_repository, historical_price_provider) -> None:
        self.price_repository = price_repository
        self.historical_price_provider = historical_price_provider

    def ingest_incremental(
        self,
        symbols: Iterable[str] | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> PriceIngestionResult:
        try:
            allowed_symbols = self._normalize_symbols(self.price_repository.list_allowed_symbols())
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
                )

            raw_frame = self._fetch_prices(
                symbols=requested_symbols,
                start_date=start_date,
                end_date=end_date,
            )
            normalized_frame = self._normalize_price_frame(raw_frame)

            touched_symbols = []
            if not normalized_frame.empty:
                touched_symbols = sorted(
                    {
                        str(symbol).strip().upper()
                        for symbol in normalized_frame["symbol"].dropna().tolist()
                        if str(symbol).strip()
                    }
                )

            written_rows = self.price_repository.upsert_price_history(normalized_frame)
            latest_rows_after_refresh = self.price_repository.refresh_price_latest(touched_symbols)

            return PriceIngestionResult(
                requested_symbols=len(requested_symbols),
                fetched_symbols=len(touched_symbols),
                written_price_history_rows=written_rows,
                price_latest_rows_after_refresh=latest_rows_after_refresh,
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
        fetch_method = getattr(self.historical_price_provider, "fetch_prices", None)
        if callable(fetch_method):
            return fetch_method(symbols=symbols, start_date=start_date, end_date=end_date)

        fallback_method = getattr(self.historical_price_provider, "fetch", None)
        if callable(fallback_method):
            return fallback_method(symbols=symbols, start_date=start_date, end_date=end_date)

        raise ServiceError("historical price provider has no supported fetch method")

    def _normalize_price_frame(self, frame: Any) -> pd.DataFrame:
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

        rename_map = {}
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
        normalized["price_date"] = pd.to_datetime(normalized["price_date"], errors="coerce").dt.date
        normalized["open"] = pd.to_numeric(normalized["open"], errors="coerce")
        normalized["high"] = pd.to_numeric(normalized["high"], errors="coerce")
        normalized["low"] = pd.to_numeric(normalized["low"], errors="coerce")
        normalized["close"] = pd.to_numeric(normalized["close"], errors="coerce")
        normalized["volume"] = pd.to_numeric(normalized["volume"], errors="coerce").fillna(0).astype("int64")
        normalized["source_name"] = normalized["source_name"].astype(str).str.strip().replace("", "yfinance")
        normalized["ingested_at"] = pd.to_datetime(normalized["ingested_at"], errors="coerce").fillna(pd.Timestamp.utcnow())

        normalized = normalized[
            ["symbol", "price_date", "open", "high", "low", "close", "volume", "source_name", "ingested_at"]
        ].dropna(subset=["symbol", "price_date", "open", "high", "low", "close"])

        if normalized.empty:
            return self._empty_price_frame()

        normalized = normalized.sort_values(
            by=["symbol", "price_date", "ingested_at"],
            ascending=[True, True, True],
        ).drop_duplicates(subset=["symbol", "price_date"], keep="last")

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
                str(symbol).strip().upper()
                for symbol in symbols
                if symbol is not None and str(symbol).strip()
            }
        )
