from datetime import datetime

from stock_quant.app.services.news_raw_ingestion_service import NewsRawIngestionService
from stock_quant.domain.entities.news import RawNewsSourceRecord


def test_build_news_raw_accepts_valid_rows() -> None:
    service = NewsRawIngestionService()
    raw = [
        RawNewsSourceRecord(
            raw_id=1001,
            published_at=datetime(2026, 3, 14, 8, 30, 0),
            source_name="finance.yahoo.com",
            title="Apple launches new enterprise AI tooling for developers",
            article_url="https://example.com/apple",
            domain="finance.yahoo.com",
            raw_payload_json='{"provider":"fixture"}',
        )
    ]

    result, metrics = service.build(raw)

    assert len(result) == 1
    assert result[0].raw_id == 1001
    assert metrics["raw_records"] == 1
    assert metrics["accepted_records"] == 1
    assert metrics["skipped_invalid"] == 0


def test_build_news_raw_rejects_invalid_rows() -> None:
    service = NewsRawIngestionService()
    raw = [
        RawNewsSourceRecord(
            raw_id=1001,
            published_at=datetime(2026, 3, 14, 8, 30, 0),
            source_name="finance.yahoo.com",
            title="",
            article_url="https://example.com/apple",
            domain="finance.yahoo.com",
            raw_payload_json='{"provider":"fixture"}',
        ),
        RawNewsSourceRecord(
            raw_id=1002,
            published_at=None,  # type: ignore[arg-type]
            source_name="finance.yahoo.com",
            title="Valid title but missing timestamp",
            article_url="https://example.com/x",
            domain="finance.yahoo.com",
            raw_payload_json='{"provider":"fixture"}',
        ),
    ]

    result, metrics = service.build(raw)

    assert len(result) == 0
    assert metrics["skipped_invalid"] == 2
