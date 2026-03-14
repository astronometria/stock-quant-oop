from __future__ import annotations

import hashlib
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any

from stock_quant.domain.entities.news_intelligence import (
    NewsArticleNormalized,
    NewsEventWindow,
    NewsFeatureDaily,
    NewsLlmEnrichment,
    NewsSymbolLink,
)


class NewsIntelligenceService:
    def build(
        self,
        raw_articles: list[dict[str, Any]],
        symbol_candidates: list[dict[str, Any]],
        symbol_map: dict[str, dict[str, str | None]],
    ) -> tuple[
        list[NewsArticleNormalized],
        list[NewsSymbolLink],
        list[NewsEventWindow],
        list[NewsLlmEnrichment],
        list[NewsFeatureDaily],
        dict[str, int],
    ]:
        normalized_articles: list[NewsArticleNormalized] = []
        links: list[NewsSymbolLink] = []
        event_windows: list[NewsEventWindow] = []
        llm_rows: list[NewsLlmEnrichment] = []

        article_by_raw_id: dict[int, NewsArticleNormalized] = {}

        for row in raw_articles:
            raw_id = row.get("raw_id")
            published_at = row.get("published_at")
            title = row.get("title")
            article_url = row.get("article_url")
            source_name = row.get("source_name")

            base_text = f"{title or ''}|{article_url or ''}|{published_at or ''}"
            article_hash = hashlib.sha1(base_text.encode("utf-8")).hexdigest()
            article_id = f"ARTICLE:{article_hash[:20]}"
            cluster_id = f"CLUSTER:{article_hash[:16]}"

            article = NewsArticleNormalized(
                article_id=article_id,
                published_at=published_at,
                available_at=published_at,
                source_name=source_name,
                title=title,
                article_url=article_url,
                article_hash=article_hash,
                cluster_id=cluster_id,
                created_at=datetime.utcnow(),
            )
            normalized_articles.append(article)

            if raw_id is not None:
                article_by_raw_id[int(raw_id)] = article

            llm_rows.append(
                NewsLlmEnrichment(
                    article_id=article_id,
                    model_name="placeholder",
                    prompt_version="v1",
                    event_type="unknown",
                    sentiment_label="neutral",
                    causality_score=0.0,
                    novelty_score=0.0,
                    summary_text=title,
                    created_at=datetime.utcnow(),
                )
            )

        seen_links: set[tuple[str, str]] = set()

        for row in symbol_candidates:
            raw_id = row.get("raw_id")
            symbol = str(row.get("symbol", "")).strip().upper()
            article = article_by_raw_id.get(int(raw_id)) if raw_id is not None else None
            mapping = symbol_map.get(symbol)

            if article is None or mapping is None:
                continue

            key = (article.article_id, symbol)
            if key in seen_links:
                continue
            seen_links.add(key)

            link = NewsSymbolLink(
                article_id=article.article_id,
                instrument_id=str(mapping.get("instrument_id") or ""),
                company_id=mapping.get("company_id"),
                symbol=symbol,
                link_confidence=1.0,
                linking_method="symbol_candidate",
                created_at=datetime.utcnow(),
            )
            if not link.instrument_id:
                continue

            links.append(link)

            event_date = article.published_at.date() if article.published_at is not None else datetime.utcnow().date()
            event_windows.append(
                NewsEventWindow(
                    article_id=article.article_id,
                    instrument_id=link.instrument_id,
                    symbol=symbol,
                    event_date=event_date,
                    window_start_date=event_date,
                    window_end_date=event_date + timedelta(days=1),
                    created_at=datetime.utcnow(),
                )
            )

        features = self._build_features(normalized_articles, links)

        metrics = {
            "raw_articles": len(raw_articles),
            "symbol_candidates": len(symbol_candidates),
            "normalized_articles": len(normalized_articles),
            "news_symbol_links": len(links),
            "news_event_windows": len(event_windows),
            "news_llm_enrichment_rows": len(llm_rows),
            "news_feature_rows": len(features),
        }

        return normalized_articles, links, event_windows, llm_rows, features, metrics

    def _build_features(
        self,
        normalized_articles: list[NewsArticleNormalized],
        links: list[NewsSymbolLink],
    ) -> list[NewsFeatureDaily]:
        article_map = {row.article_id: row for row in normalized_articles}
        grouped: dict[tuple[str, object], list[NewsSymbolLink]] = defaultdict(list)

        for link in links:
            article = article_map.get(link.article_id)
            if article is None or article.published_at is None:
                continue
            grouped[(link.instrument_id, article.published_at.date())].append(link)

        out: list[NewsFeatureDaily] = []

        for (instrument_id, as_of_date), rows in grouped.items():
            symbols = {row.symbol for row in rows}
            company_ids = {row.company_id for row in rows if row.company_id}
            confidences = [row.link_confidence for row in rows]
            article_ids = {row.article_id for row in rows}
            cluster_ids = {
                article_map[row.article_id].cluster_id
                for row in rows
                if row.article_id in article_map
            }

            out.append(
                NewsFeatureDaily(
                    instrument_id=instrument_id,
                    company_id=sorted(company_ids)[0] if company_ids else None,
                    symbol=sorted(symbols)[0] if symbols else "",
                    as_of_date=as_of_date,
                    article_count_1d=len(article_ids),
                    unique_cluster_count_1d=len(cluster_ids),
                    avg_link_confidence=(sum(confidences) / len(confidences)) if confidences else None,
                    source_name="news",
                    created_at=datetime.utcnow(),
                )
            )

        return sorted(out, key=lambda x: (x.symbol, x.as_of_date))
