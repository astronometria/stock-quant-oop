from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path
from zipfile import ZipFile

import pandas as pd

try:
    from tqdm import tqdm
except Exception:
    def tqdm(iterable, **kwargs):
        return iterable


REQUIRED_CANONICAL_COLUMNS = [
    "symbol",
    "price_date",
    "open",
    "high",
    "low",
    "close",
    "volume",
]


class RawPriceLoader:
    def load_from_csv(self, path: Path) -> pd.DataFrame:
        csv_path = Path(path).expanduser().resolve()
        df = pd.read_csv(csv_path)
        return self._normalize_frame(df, source_name=f"csv:{csv_path.name}")

    def load_from_zip(self, path: Path) -> pd.DataFrame:
        zip_path = Path(path).expanduser().resolve()
        frames: list[pd.DataFrame] = []

        with ZipFile(zip_path) as zf:
            members = [name for name in zf.namelist() if name.lower().endswith(".csv")]
            for member in tqdm(members, desc="zip-csv", unit="file"):
                with zf.open(member) as handle:
                    df = pd.read_csv(handle)
                frames.append(self._normalize_frame(df, source_name=f"zip:{zip_path.name}:{Path(member).name}"))

        if not frames:
            return self._empty_frame()

        return pd.concat(frames, ignore_index=True)

    def load_from_directory(self, path: Path) -> pd.DataFrame:
        directory = Path(path).expanduser().resolve()
        frames: list[pd.DataFrame] = []

        csv_files = sorted(directory.rglob("*.csv"))
        for csv_file in tqdm(csv_files, desc="dir-csv", unit="file"):
            df = pd.read_csv(csv_file)
            frames.append(self._normalize_frame(df, source_name=f"dir:{csv_file.name}"))

        if not frames:
            return self._empty_frame()

        return pd.concat(frames, ignore_index=True)

    def load_many(self, paths: Iterable[Path]) -> pd.DataFrame:
        frames: list[pd.DataFrame] = []

        for path in paths:
            resolved = Path(path).expanduser().resolve()
            if resolved.is_dir():
                frames.append(self.load_from_directory(resolved))
            elif resolved.suffix.lower() == ".zip":
                frames.append(self.load_from_zip(resolved))
            elif resolved.suffix.lower() == ".csv":
                frames.append(self.load_from_csv(resolved))
            else:
                raise ValueError(f"unsupported raw price source path: {resolved}")

        if not frames:
            return self._empty_frame()

        non_empty = [frame for frame in frames if not frame.empty]
        if not non_empty:
            return self._empty_frame()

        return pd.concat(non_empty, ignore_index=True)

    def _normalize_frame(self, df: pd.DataFrame, source_name: str) -> pd.DataFrame:
        frame = df.copy()
        frame.columns = [str(col).strip() for col in frame.columns]

        rename_map = {
            "Date": "price_date",
            "date": "price_date",
            "datetime": "price_date",
            "Symbol": "symbol",
            "ticker": "symbol",
            "Ticker": "symbol",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Adj Close": "close",
            "adj_close": "close",
            "Volume": "volume",
            "source": "source_name",
        }
        frame = frame.rename(columns=rename_map)

        missing = [col for col in REQUIRED_CANONICAL_COLUMNS if col not in frame.columns]
        if missing:
            raise ValueError(
                "raw price frame missing required columns: "
                + ", ".join(missing)
            )

        frame["symbol"] = frame["symbol"].astype(str).str.strip().str.upper()
        frame["price_date"] = pd.to_datetime(frame["price_date"], errors="coerce").dt.date
        frame["open"] = pd.to_numeric(frame["open"], errors="coerce")
        frame["high"] = pd.to_numeric(frame["high"], errors="coerce")
        frame["low"] = pd.to_numeric(frame["low"], errors="coerce")
        frame["close"] = pd.to_numeric(frame["close"], errors="coerce")
        frame["volume"] = pd.to_numeric(frame["volume"], errors="coerce").fillna(0).astype("int64")

        if "source_name" not in frame.columns:
            frame["source_name"] = source_name
        else:
            frame["source_name"] = frame["source_name"].fillna(source_name).astype(str)

        frame["ingested_at"] = pd.Timestamp.utcnow()

        frame = frame.dropna(subset=["symbol", "price_date", "open", "high", "low", "close"])
        frame = frame.loc[:, ["symbol", "price_date", "open", "high", "low", "close", "volume", "source_name", "ingested_at"]]
        frame = frame.sort_values(["symbol", "price_date"]).reset_index(drop=True)
        return frame

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
