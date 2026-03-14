from __future__ import annotations

from datetime import datetime
from typing import Iterable

from stock_quant.domain.entities.news import NewsArticleRaw, RawNewsSourceRecord


class NewsRawIngestionService:
    def build(self, raw_records: Iterable[RawNewsSourceRecord]) -> tuple[list[NewsArticleRaw], dict[str, int]]:
        results: list[NewsArticleRaw] = []
        skipped_invalid = 0

        for raw in raw_records:
            if raw.raw_id is None or raw.published_at is None or not raw.title or not raw.article_url:
                skipped_invalid += 1
                continue

            results.append(
                NewsArticleRaw(
                    raw_id=int(raw.raw_id),
                    published_at=raw.published_at,
                    source_name=raw.source_name,
                    title=raw.title,
                    article_url=raw.article_url,
                    domain=raw.domain,
                    raw_payload_json=raw.raw_payload_json,
                    ingested_at=datetime.utcnow(),
                )
            )

        metrics = {
            "raw_records": len(raw_records) if isinstance(raw_records, list) else len(list(raw_records)),
            "accepted_records": len(results),
            "skipped_invalid": skipped_invalid,
        }
        return results, metrics
