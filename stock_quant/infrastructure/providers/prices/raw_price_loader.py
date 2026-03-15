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
    def __init__(self) -> None:
        self._skipped_files: list[tuple[str, str]] = []

    def load_from_csv(self, path: Path) -> pd.DataFrame:
        csv_path = Path(path).expanduser().resolve()
        try:
            df = pd.read_csv(csv_path)
        except pd.errors.EmptyDataError:
            self._remember_skip(csv_path, "empty_csv")
            return self._empty_frame()
        return self._normalize_frame(df, source_name=f"csv:{csv_path.name}")

    def load_from_stooq_txt(self, path: Path) -> pd.DataFrame:
        txt_path = Path(path).expanduser().resolve()
        try:
            df = pd.read_csv(txt_path)
        except pd.errors.EmptyDataError:
            self._remember_skip(txt_path, "empty_txt")
            return self._empty_frame()
        return self._normalize_frame(df, source_name=f"stooq:{txt_path.name}")

    def load_from_zip(self, path: Path) -> pd.DataFrame:
        zip_path = Path(path).expanduser().resolve()
        frames: list[pd.DataFrame] = []

        with ZipFile(zip_path) as zf:
            csv_members = [name for name in zf.namelist() if name.lower().endswith(".csv")]
            txt_members = [name for name in zf.namelist() if name.lower().endswith(".txt")]

            for member in tqdm(
                csv_members,
                desc="zip-csv",
                unit="file",
                dynamic_ncols=True,
                mininterval=1.0,
                leave=False,
            ):
                try:
                    with zf.open(member) as handle:
                        df = pd.read_csv(handle)
                    frames.append(
                        self._normalize_frame(
                            df,
                            source_name=f"zip:{zip_path.name}:{Path(member).name}",
                        )
                    )
                except pd.errors.EmptyDataError:
                    self._remember_skip(Path(f"{zip_path}!{member}"), "empty_zip_csv")
                except Exception as exc:
                    self._remember_skip(Path(f"{zip_path}!{member}"), f"zip_csv_error:{type(exc).__name__}")

            for member in tqdm(
                txt_members,
                desc="zip-txt",
                unit="file",
                dynamic_ncols=True,
                mininterval=1.0,
                leave=False,
            ):
                try:
                    with zf.open(member) as handle:
                        df = pd.read_csv(handle)
                    frames.append(
                        self._normalize_frame(
                            df,
                            source_name=f"zip-stooq:{zip_path.name}:{Path(member).name}",
                        )
                    )
                except pd.errors.EmptyDataError:
                    self._remember_skip(Path(f"{zip_path}!{member}"), "empty_zip_txt")
                except Exception as exc:
                    self._remember_skip(Path(f"{zip_path}!{member}"), f"zip_txt_error:{type(exc).__name__}")

        return self._concat_frames(frames)

    def load_from_directory(self, path: Path) -> pd.DataFrame:
        directory = Path(path).expanduser().resolve()
        frames: list[pd.DataFrame] = []

        csv_files = sorted(directory.rglob("*.csv"))
        for csv_file in tqdm(
            csv_files,
            desc="dir-csv",
            unit="file",
            dynamic_ncols=True,
            mininterval=1.0,
            leave=False,
        ):
            try:
                df = pd.read_csv(csv_file)
                frames.append(self._normalize_frame(df, source_name=f"dir:{csv_file.name}"))
            except pd.errors.EmptyDataError:
                self._remember_skip(csv_file, "empty_dir_csv")
            except Exception as exc:
                self._remember_skip(csv_file, f"dir_csv_error:{type(exc).__name__}")

        txt_files = sorted(directory.rglob("*.txt"))
        for txt_file in tqdm(
            txt_files,
            desc="dir-txt",
            unit="file",
            dynamic_ncols=True,
            mininterval=1.0,
            leave=False,
        ):
            try:
                df = pd.read_csv(txt_file)
                frames.append(self._normalize_frame(df, source_name=f"dir-stooq:{txt_file.name}"))
            except pd.errors.EmptyDataError:
                self._remember_skip(txt_file, "empty_dir_txt")
            except Exception as exc:
                self._remember_skip(txt_file, f"dir_txt_error:{type(exc).__name__}")

        self._print_skip_summary()
        return self._concat_frames(frames)

    def load_many(self, paths: Iterable[Path]) -> pd.DataFrame:
        self._skipped_files = []
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
                frames.append(self.load_from_stooq_txt(resolved))
            else:
                raise ValueError(f"unsupported raw price source path: {resolved}")

        return self._concat_frames(frames)

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
            "<TICKER>": "symbol",
            "<DATE>": "price_date",
            "<OPEN>": "open",
            "<HIGH>": "high",
            "<LOW>": "low",
            "<CLOSE>": "close",
            "<VOL>": "volume",
            "<PER>": "period",
            "<TIME>": "time",
            "<OPENINT>": "open_interest",
        }
        frame = frame.rename(columns=rename_map)

        if "period" in frame.columns:
            frame["period"] = frame["period"].astype(str).str.strip().str.upper()
            frame = frame[frame["period"] == "D"]

        missing = [col for col in REQUIRED_CANONICAL_COLUMNS if col not in frame.columns]
        if missing:
            raise ValueError(
                "raw price frame missing required columns: " + ", ".join(missing)
            )

        frame["symbol"] = (
            frame["symbol"]
            .astype(str)
            .str.strip()
            .str.upper()
            .str.replace(".US", "", regex=False)
        )

        price_date_as_str = frame["price_date"].astype(str)
        if price_date_as_str.str.fullmatch(r"\d{8}").fillna(False).all():
            frame["price_date"] = pd.to_datetime(
                price_date_as_str,
                format="%Y%m%d",
                errors="coerce",
            ).dt.date
        else:
            frame["price_date"] = pd.to_datetime(
                frame["price_date"],
                errors="coerce",
            ).dt.date

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
        frame = frame[(frame["symbol"] != "") & (frame["high"] >= frame["low"])]

        frame = frame.loc[
            :,
            ["symbol", "price_date", "open", "high", "low", "close", "volume", "source_name", "ingested_at"],
        ]

        frame = frame.sort_values(["symbol", "price_date"]).reset_index(drop=True)
        return frame

    def _concat_frames(self, frames: list[pd.DataFrame]) -> pd.DataFrame:
        if not frames:
            return self._empty_frame()

        non_empty = [frame for frame in frames if not frame.empty]
        if not non_empty:
            return self._empty_frame()

        return pd.concat(non_empty, ignore_index=True)

    def _remember_skip(self, path: Path, reason: str) -> None:
        self._skipped_files.append((str(path), reason))

    def _print_skip_summary(self) -> None:
        if not self._skipped_files:
            return

        print(f"[raw_price_loader] skipped_files={len(self._skipped_files)}")
        sample = self._skipped_files[:20]
        for path, reason in sample:
            print(f"[raw_price_loader] skipped reason={reason} path={path}")
        remaining = len(self._skipped_files) - len(sample)
        if remaining > 0:
            print(f"[raw_price_loader] skipped_more={remaining}")

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
