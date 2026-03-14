from __future__ import annotations

from dataclasses import asdict, dataclass
from datetime import date, datetime
from typing import Any


@dataclass(slots=True)
class NewsArticleNormalized:
    article_id: str
    published_at: datetime | None
    available_at: datetime | None
    source_name: str | None
    title: str | None
    article_url: str | None
    article_hash: str
    cluster_id: str
    created_at: datetime | None = None

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class NewsSymbolLink:
    article_id: str
    instrument_id: str
    company_id: str | None
    symbol: str
    link_confidence: float
    linking_method: str
    created_at: datetime | None = None

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class NewsEventWindow:
    article_id: str
    instrument_id: str
    symbol: str
    event_date: date
    window_start_date: date
    window_end_date: date
    created_at: datetime | None = None

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class NewsLlmEnrichment:
    article_id: str
    model_name: str
    prompt_version: str
    event_type: str | None
    sentiment_label: str | None
    causality_score: float | None
    novelty_score: float | None
    summary_text: str | None
    created_at: datetime | None = None

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(slots=True)
class NewsFeatureDaily:
    instrument_id: str
    company_id: str | None
    symbol: str
    as_of_date: date
    article_count_1d: int
    unique_cluster_count_1d: int
    avg_link_confidence: float | None
    source_name: str | None = "news"
    created_at: datetime | None = None

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)
