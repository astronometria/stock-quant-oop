from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path
from typing import Any

import pandas as pd

from stock_quant.domain.ports.providers import SymbolSourcePort


class SymbolSourceLoader(SymbolSourcePort):
    def __init__(self, source_paths: list[str | Path] | None = None) -> None:
        self._source_paths = [Path(path).expanduser().resolve() for path in (source_paths or [])]

    def fetch_symbols(self) -> Iterable[Any]:
        frame = self.fetch_symbols_frame()
        return frame.to_dict(orient="records")

    def fetch_symbols_frame(self) -> pd.DataFrame:
        frames: list[pd.DataFrame] = []

        for path in self._source_paths:
            frame = pd.read_csv(path)
            frame.columns = [str(col).strip() for col in frame.columns]

            rename_map = {
                "Symbol": "symbol",
                "Ticker": "symbol",
                "Company Name": "company_name",
                "CIK": "cik",
                "Exchange": "exchange",
            }
            frame = frame.rename(columns=rename_map)

            if "symbol" in frame.columns:
                frame["symbol"] = frame["symbol"].astype(str).str.strip().str.upper()

            frame["source_file"] = path.name
            frames.append(frame)

        if not frames:
            return pd.DataFrame()

        merged = pd.concat(frames, ignore_index=True)
        return merged.reset_index(drop=True)
