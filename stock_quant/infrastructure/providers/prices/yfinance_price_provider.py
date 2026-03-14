from __future__ import annotations

from collections.abc import Iterable
from datetime import date
from typing import Any

import pandas as pd

from stock_quant.domain.ports.providers import PriceSourcePort


class YfinancePriceProvider(PriceSourcePort):
    def __init__(self, auto_adjust: bool = False, threads: bool = True) -> None:
        self._auto_adjust = auto_adjust
        self._threads = threads

    def fetch_daily_prices(self, symbols: list[str], as_of: date | None = None) -> Iterable[Any]:
        frame = self.fetch_daily_prices_frame(symbols=symbols, as_of=as_of)
        return frame.to_dict(orient="records")

    def fetch_history(
        self,
        symbols: list[str],
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> Iterable[Any]:
        frame = self.fetch_history_frame(symbols=symbols, start_date=start_date, end_date=end_date)
        return frame.to_dict(orient="records")

    def fetch_daily_prices_frame(self, symbols: list[str], as_of: date | None = None) -> pd.DataFrame:
        normalized_symbols = [symbol.strip().upper() for symbol in symbols if str(symbol).strip()]
        if not normalized_symbols:
            return self._empty_frame()

        try:
            import yfinance as yf
        except Exception as exc:
            raise RuntimeError("yfinance is not installed or could not be imported") from exc

        target_date = as_of or date.today()
        start = target_date.isoformat()
        end = (pd.Timestamp(target_date) + pd.Timedelta(days=1)).date().isoformat()

        data = yf.download(
            tickers=normalized_symbols,
            start=start,
            end=end,
            auto_adjust=self._auto_adjust,
            group_by="ticker",
            progress=False,
            threads=self._threads,
        )

        return self._normalize_downloaded_data(
            data=data,
            normalized_symbols=normalized_symbols,
            default_source_name="yfinance",
        )

    def fetch_history_frame(
        self,
        symbols: list[str],
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> pd.DataFrame:
        normalized_symbols = [symbol.strip().upper() for symbol in symbols if str(symbol).strip()]
        if not normalized_symbols:
            return self._empty_frame()

        try:
            import yfinance as yf
        except Exception as exc:
            raise RuntimeError("yfinance is not installed or could not be imported") from exc

        download_start = start_date.isoformat() if start_date is not None else None
        download_end = None
        if end_date is not None:
            download_end = (pd.Timestamp(end_date) + pd.Timedelta(days=1)).date().isoformat()

        data = yf.download(
            tickers=normalized_symbols,
            start=download_start,
            end=download_end,
            auto_adjust=self._auto_adjust,
            group_by="ticker",
            progress=False,
            threads=self._threads,
        )

        return self._normalize_downloaded_data(
            data=data,
            normalized_symbols=normalized_symbols,
            default_source_name="yfinance",
        )

    def _normalize_downloaded_data(
        self,
        data: pd.DataFrame,
        normalized_symbols: list[str],
        default_source_name: str,
    ) -> pd.DataFrame:
        if data is None or data.empty:
            return self._empty_frame()

        frames: list[pd.DataFrame] = []

        if isinstance(data.columns, pd.MultiIndex):
            level0 = [str(v).strip().upper() for v in data.columns.get_level_values(0)]
            level1 = [str(v).strip().upper() for v in data.columns.get_level_values(1)]

            symbols_in_level0 = set(level0)
            symbols_in_level1 = set(level1)

            if any(symbol in symbols_in_level0 for symbol in normalized_symbols):
                for symbol in normalized_symbols:
                    if symbol not in symbols_in_level0:
                        continue
                    symbol_frame = data[symbol].copy()
                    if symbol_frame.empty:
                        continue
                    frames.append(self._normalize_symbol_frame(symbol, symbol_frame, default_source_name))
            elif any(symbol in symbols_in_level1 for symbol in normalized_symbols):
                for symbol in normalized_symbols:
                    if symbol not in symbols_in_level1:
                        continue
                    symbol_frame = data.xs(symbol, axis=1, level=1).copy()
                    if symbol_frame.empty:
                        continue
                    frames.append(self._normalize_symbol_frame(symbol, symbol_frame, default_source_name))
            else:
                flattened = data.copy()
                flattened.columns = [
                    "_".join(str(part).strip() for part in col if str(part).strip())
                    if isinstance(col, tuple)
                    else str(col).strip()
                    for col in flattened.columns
                ]
                if len(normalized_symbols) == 1:
                    frames.append(self._normalize_symbol_frame(normalized_symbols[0], flattened, default_source_name))
                else:
                    return self._empty_frame()
        else:
            if len(normalized_symbols) != 1:
                raise RuntimeError("unexpected yfinance output shape for multi-symbol download")
            frames.append(self._normalize_symbol_frame(normalized_symbols[0], data.copy(), default_source_name))

        if not frames:
            return self._empty_frame()

        combined = pd.concat(frames, ignore_index=True)
        combined = combined.sort_values(["symbol", "price_date"]).reset_index(drop=True)
        return combined

    def _normalize_symbol_frame(self, symbol: str, frame: pd.DataFrame, default_source_name: str) -> pd.DataFrame:
        working = frame.reset_index().copy()

        if isinstance(working.columns, pd.MultiIndex):
            working.columns = [
                "_".join(str(part).strip() for part in col if str(part).strip())
                if isinstance(col, tuple)
                else str(col).strip()
                for col in working.columns
            ]
        else:
            working.columns = [str(col).strip() for col in working.columns]

        # Normaliser d'abord les noms de colonnes en majuscules pour inspection
        original_columns = list(working.columns)
        normalized_lookup = {col: str(col).strip().upper() for col in original_columns}

        # Date / index
        date_col = None
        for col, col_upper in normalized_lookup.items():
            if col_upper in {"DATE", "DATETIME", "INDEX"}:
                date_col = col
                break
        if date_col is not None and date_col != "price_date":
            working = working.rename(columns={date_col: "price_date"})

        # Choix explicite de la colonne close :
        # on préfère Adj Close si présent, sinon Close.
        adj_close_candidates = [col for col, col_upper in normalized_lookup.items() if "ADJ CLOSE" in col_upper]
        close_candidates = [
            col for col, col_upper in normalized_lookup.items()
            if col_upper == "CLOSE" or col_upper.endswith("_CLOSE")
        ]
        chosen_close = adj_close_candidates[0] if adj_close_candidates else (close_candidates[0] if close_candidates else None)

        rename_map: dict[str, str] = {}
        for col in original_columns:
            col_upper = normalized_lookup[col]

            if col == date_col:
                continue
            if chosen_close is not None and col == chosen_close:
                rename_map[col] = "close"
                continue

            if "OPEN" in col_upper and col_upper != "OPENINTEREST":
                rename_map[col] = "open"
            elif "HIGH" in col_upper:
                rename_map[col] = "high"
            elif col_upper == "LOW" or col_upper.endswith("_LOW"):
                rename_map[col] = "low"
            elif "VOLUME" in col_upper:
                rename_map[col] = "volume"

        working = working.rename(columns=rename_map)

        # Si Adj Close et Close coexistent, on élimine la colonne non retenue.
        candidate_drop_cols: list[str] = []
        for col in working.columns:
            col_upper = str(col).strip().upper()
            if col == "close":
                continue
            if "ADJ CLOSE" in col_upper or col_upper == "CLOSE" or col_upper.endswith("_CLOSE"):
                candidate_drop_cols.append(col)
        if candidate_drop_cols:
            working = working.drop(columns=candidate_drop_cols, errors="ignore")

        # Protection supplémentaire : si des colonnes dupliquées subsistent, on garde la première.
        if working.columns.duplicated().any():
            working = working.loc[:, ~working.columns.duplicated()]

        required = ["price_date", "open", "high", "low", "close", "volume"]
        missing = [col for col in required if col not in working.columns]
        if missing:
            raise ValueError(f"yfinance frame missing required columns for {symbol}: {missing}")

        working["symbol"] = symbol
        working["price_date"] = pd.to_datetime(working["price_date"], errors="coerce").dt.date
        working["open"] = pd.to_numeric(working["open"], errors="coerce")
        working["high"] = pd.to_numeric(working["high"], errors="coerce")
        working["low"] = pd.to_numeric(working["low"], errors="coerce")
        working["close"] = pd.to_numeric(working["close"], errors="coerce")
        working["volume"] = pd.to_numeric(working["volume"], errors="coerce").fillna(0).astype("int64")
        working["source_name"] = default_source_name
        working["ingested_at"] = pd.Timestamp.utcnow()

        working = working.dropna(subset=["price_date", "open", "high", "low", "close"])
        return working.loc[:, ["symbol", "price_date", "open", "high", "low", "close", "volume", "source_name", "ingested_at"]]

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
