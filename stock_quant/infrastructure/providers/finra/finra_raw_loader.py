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


class FinraRawLoader:
    def load_from_csv(self, path: Path) -> pd.DataFrame:
        csv_path = Path(path).expanduser().resolve()
        frame = pd.read_csv(csv_path)
        return self._normalize_frame(frame, source_file=csv_path.name)

    def load_from_zip(self, path: Path) -> pd.DataFrame:
        zip_path = Path(path).expanduser().resolve()
        frames: list[pd.DataFrame] = []

        with ZipFile(zip_path) as zf:
            members = [name for name in zf.namelist() if name.lower().endswith(".csv")]
            for member in tqdm(members, desc="finra-zip", unit="file"):
                with zf.open(member) as handle:
                    frame = pd.read_csv(handle)
                frames.append(self._normalize_frame(frame, source_file=Path(member).name))

        if not frames:
            return self._empty_frame()

        return pd.concat(frames, ignore_index=True)

    def load_many(self, paths: Iterable[Path]) -> pd.DataFrame:
        frames: list[pd.DataFrame] = []

        for path in paths:
            resolved = Path(path).expanduser().resolve()
            if resolved.suffix.lower() == ".zip":
                frames.append(self.load_from_zip(resolved))
            elif resolved.suffix.lower() == ".csv":
                frames.append(self.load_from_csv(resolved))
            else:
                raise ValueError(f"unsupported FINRA source path: {resolved}")

        if not frames:
            return self._empty_frame()

        non_empty = [frame for frame in frames if not frame.empty]
        if not non_empty:
            return self._empty_frame()

        return pd.concat(non_empty, ignore_index=True)

    def _normalize_frame(self, frame: pd.DataFrame, source_file: str) -> pd.DataFrame:
        working = frame.copy()
        working.columns = [str(col).strip() for col in working.columns]

        rename_map = {
            "Symbol": "symbol",
            "symbolCode": "symbol",
            "Settlement Date": "settlement_date",
            "Current Short Position Quantity": "short_interest",
            "Previous Short Position Quantity": "previous_short_interest",
            "Average Daily Volume Quantity": "avg_daily_volume",
            "Days To Cover Quantity": "days_to_cover",
            "Revision Flag": "revision_flag",
        }
        working = working.rename(columns=rename_map)

        if "symbol" in working.columns:
            working["symbol"] = working["symbol"].astype(str).str.strip().str.upper()

        if "settlement_date" in working.columns:
            working["settlement_date"] = pd.to_datetime(working["settlement_date"], errors="coerce").dt.date

        for col in ["short_interest", "previous_short_interest", "avg_daily_volume", "days_to_cover"]:
            if col in working.columns:
                working[col] = pd.to_numeric(working[col], errors="coerce")

        working["source_file"] = source_file
        working["loaded_at"] = pd.Timestamp.utcnow()
        return working.reset_index(drop=True)

    def _empty_frame(self) -> pd.DataFrame:
        return pd.DataFrame(
            columns=[
                "symbol",
                "settlement_date",
                "short_interest",
                "previous_short_interest",
                "avg_daily_volume",
                "days_to_cover",
                "revision_flag",
                "source_file",
                "loaded_at",
            ]
        )
