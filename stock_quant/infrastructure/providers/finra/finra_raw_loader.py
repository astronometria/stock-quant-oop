from __future__ import annotations

import re
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

        if self._looks_like_finra_short_interest(csv_path.name):
            frame = self._read_pipe_delimited(csv_path)
        else:
            frame = pd.read_csv(csv_path)

        return self._normalize_frame(frame, source_file=csv_path.name)

    def load_from_txt(self, path: Path) -> pd.DataFrame:
        txt_path = Path(path).expanduser().resolve()
        frame = self._read_pipe_delimited(txt_path)
        return self._normalize_frame(frame, source_file=txt_path.name)

    def load_from_zip(self, path: Path) -> pd.DataFrame:
        zip_path = Path(path).expanduser().resolve()
        frames: list[pd.DataFrame] = []

        with ZipFile(zip_path) as zf:
            members = [
                name
                for name in zf.namelist()
                if name.lower().endswith(".csv") or name.lower().endswith(".txt")
            ]
            for member in tqdm(
                members,
                desc="finra-zip",
                unit="file",
                dynamic_ncols=True,
                mininterval=0.5,
            ):
                with zf.open(member) as handle:
                    if member.lower().endswith(".txt") or self._looks_like_finra_short_interest(member):
                        frame = pd.read_csv(
                            handle,
                            sep="|",
                            engine="python",
                            on_bad_lines="skip",
                            dtype=str,
                        )
                    else:
                        frame = pd.read_csv(handle)
                frames.append(self._normalize_frame(frame, source_file=Path(member).name))

        return self._concat_or_empty(frames)

    def load_from_directory(self, path: Path) -> pd.DataFrame:
        root = Path(path).expanduser().resolve()
        files = sorted(
            p
            for p in root.rglob("*")
            if p.is_file() and p.suffix.lower() in {".csv", ".txt", ".zip"}
        )
        frames: list[pd.DataFrame] = []

        for file_path in tqdm(
            files,
            desc="finra-dir",
            unit="file",
            dynamic_ncols=True,
            mininterval=0.5,
        ):
            suffix = file_path.suffix.lower()
            if suffix == ".csv":
                frames.append(self.load_from_csv(file_path))
            elif suffix == ".txt":
                frames.append(self.load_from_txt(file_path))
            elif suffix == ".zip":
                frames.append(self.load_from_zip(file_path))

        return self._concat_or_empty(frames)

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
            elif resolved.suffix.lower() == ".txt":
                frames.append(self.load_from_txt(resolved))
            else:
                raise ValueError(f"unsupported FINRA source path: {resolved}")

        out = self._concat_or_empty(frames)
        if out.empty:
            return out

        return (
            out.drop_duplicates(
                subset=["symbol", "settlement_date", "source_file"],
                keep="last",
            )
            .reset_index(drop=True)
        )

    def _read_pipe_delimited(self, path: Path) -> pd.DataFrame:
        return pd.read_csv(
            path,
            sep="|",
            engine="python",
            on_bad_lines="skip",
            dtype=str,
        )

    def _looks_like_finra_short_interest(self, name: str) -> bool:
        lowered = str(name).lower()
        return bool(re.search(r"shrt20\d{6}\.csv$", lowered))

    def _concat_or_empty(self, frames: list[pd.DataFrame]) -> pd.DataFrame:
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
            "symbol": "symbol",
            "symbolCode": "symbol",
            "Symbol Code": "symbol",
            "Settlement Date": "settlement_date",
            "SettlementDate": "settlement_date",
            "settlementDate": "settlement_date",
            "settlement_date": "settlement_date",
            "Current Short": "short_interest",
            "Current Short Position Quantity": "short_interest",
            "CurrentShortPositionQuantity": "short_interest",
            "currentShortPositionQuantity": "short_interest",
            "short_interest": "short_interest",
            "Previous Short": "previous_short_interest",
            "Previous Short Position Quantity": "previous_short_interest",
            "PreviousShortPositionQuantity": "previous_short_interest",
            "previousShortPositionQuantity": "previous_short_interest",
            "previous_short_interest": "previous_short_interest",
            "Avg Daily Volume": "avg_daily_volume",
            "Average Daily Volume Quantity": "avg_daily_volume",
            "AverageDailyVolumeQuantity": "avg_daily_volume",
            "averageDailyVolumeQuantity": "avg_daily_volume",
            "avg_daily_volume": "avg_daily_volume",
            "Days to Cover": "days_to_cover",
            "Days To Cover": "days_to_cover",
            "Days To Cover Quantity": "days_to_cover",
            "DaysToCoverQuantity": "days_to_cover",
            "daysToCoverQuantity": "days_to_cover",
            "days_to_cover": "days_to_cover",
            "Shares Float": "shares_float",
            "Shares Float Quantity": "shares_float",
            "SharesFloatQuantity": "shares_float",
            "shares_float": "shares_float",
            "Revision Flag": "revision_flag",
            "revisionFlag": "revision_flag",
            "revision_flag": "revision_flag",
            "Market": "source_market",
            "market": "source_market",
            "marketClassCode": "source_market",
            "Source Market": "source_market",
            "source_market": "source_market",
        }
        working = working.rename(columns=rename_map)

        if "symbol" in working.columns:
            working["symbol"] = working["symbol"].astype(str).str.strip().str.upper()

        if "settlement_date" in working.columns:
            working["settlement_date"] = pd.to_datetime(
                working["settlement_date"],
                errors="coerce",
            ).dt.date

        for col in [
            "short_interest",
            "previous_short_interest",
            "avg_daily_volume",
            "days_to_cover",
            "shares_float",
        ]:
            if col in working.columns:
                working[col] = pd.to_numeric(working[col], errors="coerce")

        if "revision_flag" not in working.columns:
            working["revision_flag"] = None
        else:
            working["revision_flag"] = working["revision_flag"].astype(str).str.strip()

        if "source_market" not in working.columns:
            working["source_market"] = self._infer_source_market(source_file)
        else:
            working["source_market"] = (
                working["source_market"]
                .fillna(self._infer_source_market(source_file))
                .astype(str)
                .str.strip()
                .str.lower()
            )

        if "source_date" not in working.columns:
            working["source_date"] = self._infer_source_date(source_file)
        else:
            working["source_date"] = pd.to_datetime(
                working["source_date"],
                errors="coerce",
            ).dt.date

        working["source_file"] = source_file
        working["ingested_at"] = pd.Timestamp.utcnow()

        required_defaults = {
            "symbol": None,
            "settlement_date": None,
            "short_interest": None,
            "previous_short_interest": None,
            "avg_daily_volume": None,
            "days_to_cover": None,
            "shares_float": None,
            "revision_flag": None,
            "source_market": self._infer_source_market(source_file),
            "source_file": source_file,
            "source_date": self._infer_source_date(source_file),
            "ingested_at": pd.Timestamp.utcnow(),
        }
        for col, default_value in required_defaults.items():
            if col not in working.columns:
                working[col] = default_value

        working = working.dropna(subset=["symbol", "settlement_date", "short_interest"])
        working = working[working["symbol"].astype(str).str.strip() != ""]

        return (
            working.loc[
                :,
                [
                    "symbol",
                    "settlement_date",
                    "short_interest",
                    "previous_short_interest",
                    "avg_daily_volume",
                    "days_to_cover",
                    "shares_float",
                    "revision_flag",
                    "source_market",
                    "source_file",
                    "source_date",
                    "ingested_at",
                ],
            ]
            .reset_index(drop=True)
        )

    def _infer_source_market(self, source_file: str) -> str:
        lowered = str(source_file).lower()

        market_map = {
            "fnsq": "regular",
            "fnqc": "regular",
            "fnyx": "regular",
            "fnra": "regular",
            "cnms": "regular",
            "shrt": "otc",
            "otc": "otc",
        }
        for token, market in market_map.items():
            if token in lowered:
                return market
        return "unknown"

    def _infer_source_date(self, source_file: str):
        match = re.search(r"(20\d{6})", str(source_file))
        if not match:
            return None
        try:
            return pd.to_datetime(match.group(1), format="%Y%m%d", errors="coerce").date()
        except Exception:
            return None

    def _empty_frame(self) -> pd.DataFrame:
        return pd.DataFrame(
            columns=[
                "symbol",
                "settlement_date",
                "short_interest",
                "previous_short_interest",
                "avg_daily_volume",
                "days_to_cover",
                "shares_float",
                "revision_flag",
                "source_market",
                "source_file",
                "source_date",
                "ingested_at",
            ]
        )
