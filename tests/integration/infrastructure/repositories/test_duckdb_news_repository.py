from __future__ import annotations

from datetime import datetime
from pathlib import Path

from stock_quant.domain.entities.news import NewsArticleRaw, NewsSymbolCandidate
from stock_quant.domain.entities.symbol_reference import SymbolReferenceEntry
from stock_quant.infrastructure.db.duckdb_session_factory import DuckDbSessionFactory
from stock_quant.infrastructure.db.schema_manager import SchemaManager
from stock_quant.infrastructure.db.table_names import NEWS_SOURCE_RAW
from stock_quant.infrastructure.db.unit_of_work import DuckDbUnitOfWork
from stock_quant.infrastructure.repositories.duckdb_news_repository import DuckDbNewsRepository


def seed_symbol_reference(uow) -> None:
    uow.connection.executemany(
        """
        INSERT INTO symbol_reference (
            symbol, cik, company_name, company_name_clean, aliases_json, exchange,
            source_name, symbol_match_enabled, name_match_enabled, created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            ("AAPL", "0000320193", "Apple Inc.", "APPLE INC", '["APPLE INC","APPLE"]', "NASDAQ", "fixture", True, True, datetime.utcnow()),
            ("MSFT", "0000789019", "Microsoft Corporation", "MICROSOFT CORPORATION", '["MICROSOFT CORPORATION","MICROSOFT"]', "NASDAQ", "fixture", True, True, datetime.utcnow()),
        ],
    )


def test_news_repository_roundtrip(tmp_path: Path) -> None:
    db_path = tmp_path / "test_news.duckdb"
    session_factory = DuckDbSessionFactory(db_path)

    with DuckDbUnitOfWork(session_factory) as uow:
        SchemaManager(uow).initialize(drop_existing=True)
        seed_symbol_reference(uow)

        repo = DuckDbNewsRepository(uow.connection)

        uow.connection.executemany(
            f"""
            INSERT INTO {NEWS_SOURCE_RAW} (
                raw_id, published_at, source_name, title, article_url, domain, raw_payload_json, ingested_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (1001, "2026-03-14 08:30:00", "finance.yahoo.com", "Apple launches new enterprise AI tooling", "https://example.com/apple", "finance.yahoo.com", '{"provider":"fixture"}', datetime.utcnow()),
                (1002, "2026-03-14 09:00:00", "reuters.com", "Microsoft expands cloud agreements", "https://example.com/msft", "reuters.com", '{"provider":"fixture"}', datetime.utcnow()),
            ],
        )

        source_rows = repo.load_raw_news_source_records()
        assert len(source_rows) == 2

        raw_entries = [
            NewsArticleRaw(
                raw_id=1001,
                published_at=source_rows[0].published_at,
                source_name=source_rows[0].source_name,
                title=source_rows[0].title,
                article_url=source_rows[0].article_url,
                domain=source_rows[0].domain,
                raw_payload_json=source_rows[0].raw_payload_json,
                ingested_at=datetime.utcnow(),
            ),
            NewsArticleRaw(
                raw_id=1002,
                published_at=source_rows[1].published_at,
                source_name=source_rows[1].source_name,
                title=source_rows[1].title,
                article_url=source_rows[1].article_url,
                domain=source_rows[1].domain,
                raw_payload_json=source_rows[1].raw_payload_json,
                ingested_at=datetime.utcnow(),
            ),
        ]

        assert repo.replace_news_articles_raw(raw_entries) == 2
        fetched_articles = repo.load_news_articles_raw()
        assert len(fetched_articles) == 2

        refs = repo.load_symbol_reference_entries()
        assert len(refs) == 2
        assert isinstance(refs[0], SymbolReferenceEntry)

        candidates = [
            NewsSymbolCandidate(1001, "AAPL", "alias", 0.75, "APPLE", datetime.utcnow()),
            NewsSymbolCandidate(1002, "MSFT", "alias", 0.9, "MICROSOFT", datetime.utcnow()),
        ]
        assert repo.replace_news_symbol_candidates(candidates) == 2

        count = uow.connection.execute("SELECT COUNT(*) FROM news_symbol_candidates").fetchone()[0]
        assert count == 2
