from __future__ import annotations

import json
from datetime import datetime
from typing import Iterable

from stock_quant.domain.entities.news import NewsArticleRaw, NewsSymbolCandidate
from stock_quant.domain.entities.symbol_reference import SymbolReferenceEntry
from stock_quant.domain.policies.news_symbol_candidate_policy import NewsSymbolCandidatePolicy


class NewsSymbolCandidateService:
    def __init__(self, policy: NewsSymbolCandidatePolicy) -> None:
        self.policy = policy

    def build(
        self,
        news_articles: Iterable[NewsArticleRaw],
        symbol_references: Iterable[SymbolReferenceEntry],
    ) -> tuple[list[NewsSymbolCandidate], dict[str, int]]:
        articles = list(news_articles)
        references = list(symbol_references)

        candidates: list[NewsSymbolCandidate] = []
        matched_article_ids: set[int] = set()

        for article in articles:
            normalized_title = self.policy.normalize_text(article.title)
            article_matches: list[NewsSymbolCandidate] = []

            for ref in references:
                if ref.symbol_match_enabled and self.policy.symbol_in_title(ref.symbol, normalized_title):
                    article_matches.append(
                        NewsSymbolCandidate(
                            raw_id=article.raw_id,
                            symbol=ref.symbol,
                            match_type="symbol",
                            match_score=self.policy.score_match(match_type="symbol", matched_text=ref.symbol),
                            matched_text=ref.symbol,
                            created_at=datetime.utcnow(),
                        )
                    )

                if ref.name_match_enabled:
                    aliases = []
                    try:
                        aliases = json.loads(ref.aliases_json)
                    except Exception:
                        aliases = []

                    for alias in aliases:
                        alias_norm = self.policy.normalize_text(str(alias))
                        if not self.policy.can_use_alias_match(alias_norm):
                            continue
                        if self.policy.alias_in_title(alias_norm, normalized_title):
                            article_matches.append(
                                NewsSymbolCandidate(
                                    raw_id=article.raw_id,
                                    symbol=ref.symbol,
                                    match_type="alias",
                                    match_score=self.policy.score_match(match_type="alias", matched_text=alias_norm),
                                    matched_text=alias_norm,
                                    created_at=datetime.utcnow(),
                                )
                            )

            dedup: dict[tuple[int, str], NewsSymbolCandidate] = {}
            for match in article_matches:
                key = (match.raw_id, match.symbol)
                existing = dedup.get(key)
                if existing is None or match.match_score > existing.match_score:
                    dedup[key] = match

            final_matches = list(dedup.values())
            if final_matches:
                matched_article_ids.add(article.raw_id)
            candidates.extend(final_matches)

        metrics = {
            "news_articles": len(articles),
            "symbol_references": len(references),
            "matched_articles": len(matched_article_ids),
            "candidate_rows": len(candidates),
        }
        return candidates, metrics
