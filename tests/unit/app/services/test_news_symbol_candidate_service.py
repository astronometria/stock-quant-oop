from datetime import datetime

from stock_quant.app.services.news_symbol_candidate_service import NewsSymbolCandidateService
from stock_quant.domain.entities.news import NewsArticleRaw
from stock_quant.domain.entities.symbol_reference import SymbolReferenceEntry
from stock_quant.domain.policies.news_symbol_candidate_policy import NewsSymbolCandidatePolicy


def build_service() -> NewsSymbolCandidateService:
    return NewsSymbolCandidateService(policy=NewsSymbolCandidatePolicy())


def build_ref(symbol: str, company_name: str, aliases_json: str) -> SymbolReferenceEntry:
    return SymbolReferenceEntry(
        symbol=symbol,
        cik=None,
        company_name=company_name,
        company_name_clean=company_name.upper(),
        aliases_json=aliases_json,
        exchange="NASDAQ",
        source_name="fixture",
        symbol_match_enabled=True,
        name_match_enabled=True,
        created_at=datetime.utcnow(),
    )


def test_build_news_symbol_candidates_matches_aliases() -> None:
    service = build_service()
    news = [
        NewsArticleRaw(
            raw_id=1001,
            published_at=datetime(2026, 3, 14, 8, 30, 0),
            source_name="finance.yahoo.com",
            title="Apple launches new enterprise AI tooling for developers",
            article_url="https://example.com/apple",
            domain="finance.yahoo.com",
            raw_payload_json='{"provider":"fixture"}',
            ingested_at=datetime.utcnow(),
        ),
        NewsArticleRaw(
            raw_id=1002,
            published_at=datetime(2026, 3, 14, 9, 0, 0),
            source_name="reuters.com",
            title="Microsoft expands cloud agreements with major banking clients",
            article_url="https://example.com/msft",
            domain="reuters.com",
            raw_payload_json='{"provider":"fixture"}',
            ingested_at=datetime.utcnow(),
        ),
    ]
    refs = [
        build_ref("AAPL", "Apple Inc.", '["APPLE INC", "APPLE"]'),
        build_ref("MSFT", "Microsoft Corporation", '["MICROSOFT CORPORATION", "MICROSOFT"]'),
    ]

    candidates, metrics = service.build(news, refs)

    assert len(candidates) == 2
    assert metrics["news_articles"] == 2
    assert metrics["symbol_references"] == 2
    assert metrics["matched_articles"] == 2
    assert metrics["candidate_rows"] == 2

    pairs = {(c.raw_id, c.symbol) for c in candidates}
    assert (1001, "AAPL") in pairs
    assert (1002, "MSFT") in pairs


def test_build_news_symbol_candidates_ignores_unmatched_articles() -> None:
    service = build_service()
    news = [
        NewsArticleRaw(
            raw_id=1004,
            published_at=datetime(2026, 3, 14, 9, 45, 0),
            source_name="marketwatch.com",
            title="ETF flows accelerate as broad market rally continues",
            article_url="https://example.com/etf",
            domain="marketwatch.com",
            raw_payload_json='{"provider":"fixture"}',
            ingested_at=datetime.utcnow(),
        ),
    ]
    refs = [
        build_ref("AAPL", "Apple Inc.", '["APPLE INC", "APPLE"]'),
    ]

    candidates, metrics = service.build(news, refs)

    assert len(candidates) == 0
    assert metrics["matched_articles"] == 0
    assert metrics["candidate_rows"] == 0
