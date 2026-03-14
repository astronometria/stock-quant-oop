from __future__ import annotations

from collections.abc import Iterable
from pathlib import Path

import pandas as pd


class SecSourceLoader:
    def __init__(self, source_paths: list[str | Path] | None = None) -> None:
        self._source_paths = [Path(path).expanduser().resolve() for path in (source_paths or [])]

    def load_index_frame(self) -> pd.DataFrame:
        frames: list[pd.DataFrame] = []

        for path in self._source_paths:
            frame = pd.read_csv(path)
            frame.columns = [str(col).strip() for col in frame.columns]

            rename_map = {
                "cik_str": "cik",
                "cik": "cik",
                "company": "company_name",
                "company_name": "company_name",
                "form": "form_type",
                "form_type": "form_type",
                "filed": "filing_date",
                "filing_date": "filing_date",
                "accepted": "accepted_at",
                "accessionNumber": "accession_number",
                "accession_number": "accession_number",
                "primaryDocument": "primary_document",
                "primary_document": "primary_document",
                "filingUrl": "filing_url",
                "filing_url": "filing_url",
            }
            frame = frame.rename(columns=rename_map)

            if "cik" in frame.columns:
                frame["cik"] = frame["cik"].astype(str).str.strip().str.zfill(10)

            if "filing_date" in frame.columns:
                frame["filing_date"] = pd.to_datetime(frame["filing_date"], errors="coerce").dt.date

            if "accepted_at" in frame.columns:
                frame["accepted_at"] = pd.to_datetime(frame["accepted_at"], errors="coerce")

            frame["source_name"] = "sec"
            frames.append(frame)

        if not frames:
            return pd.DataFrame()

        return pd.concat(frames, ignore_index=True).reset_index(drop=True)
