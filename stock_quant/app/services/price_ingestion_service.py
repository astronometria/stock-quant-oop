from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import date
from typing import Any

import pandas as pd

from stock_quant.domain.entities.prices import PriceBar


@dataclass(slots=True)
class PriceIngestionSummary:
    rows_read: int
    rows_valid: int
    rows_invalid: int
    symbols_count: int
    min_price_date: date | None
    max_price_date: date | None


class PriceIngestionService:
    def normalize_records(self, records: Iterable[Mapping[str, Any] | PriceBar]) -> list[dict[str, Any]]:
        normalized: list[dict[str, Any]] = []

        for record in records:
            if isinstance(record, PriceBar):
                normalized.append(
                    {
                        "symbol": str(record.symbol).strip().upper(),
                        "price_date": record.price_date,
                        "open": float(record.open),
                        "high": float(record.high),
                        "low": float(record.low),
                        "close": float(record.close),
                        "volume": int(record.volume),
                        "source_name": str(record.source_name),
                        "ingested_at": record.ingested_at,
                    }
                )
                continue

            normalized.append(
                {
                    "symbol": str(record.get("symbol", "")).strip().upper(),
                    "price_date": record.get("price_date"),
                    "open": record.get("open"),
                    "high": record.get("high"),
                    "low": record.get("low"),
                    "close": record.get("close"),
                    "volume": record.get("volume", 0),
                    "source_name": str(record.get("source_name", "")) or "unknown",
                    "ingested_at": record.get("ingested_at"),
                }
            )

        return normalized

    def build_price_frame(self, records: Iterable[Mapping[str, Any] | PriceBar]) -> pd.DataFrame:
        normalized = self.normalize_records(records)
        if not normalized:
            return self._empty_frame()

        frame = pd.DataFrame(normalized)

        frame["symbol"] = frame["symbol"].astype(str).str.strip().str.upper()
        frame["price_date"] = pd.to_datetime(frame["price_date"], errors="coerce").dt.date
        frame["open"] = pd.to_numeric(frame["open"], errors="coerce")
        frame["high"] = pd.to_numeric(frame["high"], errors="coerce")
        frame["low"] = pd.to_numeric(frame["low"], errors="coerce")
        frame["close"] = pd.to_numeric(frame["close"], errors="coerce")
        frame["volume"] = pd.to_numeric(frame["volume"], errors="coerce").fillna(0).astype("int64")
        frame["source_name"] = frame["source_name"].fillna("unknown").astype(str)

        if "ingested_at" not in frame.columns:
            frame["ingested_at"] = pd.Timestamp.utcnow()
        else:
            frame["ingested_at"] = frame["ingested_at"].fillna(pd.Timestamp.utcnow())

        frame = frame.dropna(subset=["symbol", "price_date", "open", "high", "low", "close"])
        frame = frame[(frame["symbol"] != "") & (frame["high"] >= frame["low"])]
        frame = frame.sort_values(["symbol", "price_date"]).drop_duplicates(
            subset=["symbol", "price_date"],
            keep="last",
        )

        return frame.loc[
            :,
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
            ],
        ].reset_index(drop=True)

    def summarize_frame(self, frame: pd.DataFrame, rows_read: int | None = None) -> PriceIngestionSummary:
        if frame.empty:
            return PriceIngestionSummary(
                rows_read=rows_read or 0,
                rows_valid=0,
                rows_invalid=max((rows_read or 0), 0),
                symbols_count=0,
                min_price_date=None,
                max_price_date=None,
            )

        valid_rows = int(len(frame))
        effective_rows_read = rows_read if rows_read is not None else valid_rows
        invalid_rows = max(int(effective_rows_read) - valid_rows, 0)

        return PriceIngestionSummary(
            rows_read=int(effective_rows_read),
            rows_valid=valid_rows,
            rows_invalid=invalid_rows,
            symbols_count=int(frame["symbol"].nunique()),
            min_price_date=frame["price_date"].min(),
            max_price_date=frame["price_date"].max(),
        )

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
