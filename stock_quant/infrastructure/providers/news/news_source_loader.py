from __future__ import annotations

from collections.abc import Iterable
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd

from stock_quant.domain.ports.providers import NewsSourcePort


class NewsSourceLoader(NewsSourcePort):
    def __init__(self, source_paths: list[str | Path] | None = None) -> None:
        self._source_paths = [Path(path).expanduser().resolve() for path in (source_paths or [])]

    def fetch_news(
        self,
        start_date: date | None = None,
        end_date: date | None = None,
        limit: int | None = None,
    ) -> Iterable[Any]:
        frame = self.fetch_news_frame(start_date=start_date, end_date=end_date, limit=limit)
        return frame.to_dict(orient="records")

    def fetch_news_frame(
        self,
        start_date: date | None = None,
        end_date: date | None = None,
        limit: int | None = None,
    ) -> pd.DataFrame:
        frames: list[pd.DataFrame] = []

        for path in self._source_paths:
            frame = pd.read_csv(path)
            frame.columns = [str(col).strip() for col in frame.columns]

            rename_map = {
                "publishedAt": "published_at",
                "Published At": "published_at",
                "url": "article_url",
                "source": "source_name",
            }
            frame = frame.rename(columns=rename_map)

            if "published_at" in frame.columns:
                frame["published_at"] = pd.to_datetime(frame["published_at"], errors="coerce")

            frame["source_file"] = path.name
            frames.append(frame)

        if not frames:
            return pd.DataFrame()

        merged = pd.concat(frames, ignore_index=True)

        if start_date is not None and "published_at" in merged.columns:
            merged = merged[merged["published_at"].dt.date >= start_date]

        if end_date is not None and "published_at" in merged.columns:
            merged = merged[merged["published_at"].dt.date <= end_date]

        if limit is not None:
            merged = merged.head(limit)

        return merged.reset_index(drop=True)
