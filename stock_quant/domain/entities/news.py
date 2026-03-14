from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(slots=True)
class RawNewsSourceRecord:
    raw_id: int
    published_at: datetime
    source_name: str
    title: str
    article_url: str
    domain: str | None
    raw_payload_json: str | None


@dataclass(slots=True)
class NewsArticleRaw:
    raw_id: int
    published_at: datetime
    source_name: str
    title: str
    article_url: str
    domain: str | None
    raw_payload_json: str | None
    ingested_at: datetime


@dataclass(slots=True)
class NewsSymbolCandidate:
    raw_id: int
    symbol: str
    match_type: str
    match_score: float
    matched_text: str
    created_at: datetime
