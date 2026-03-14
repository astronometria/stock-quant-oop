from datetime import date

from stock_quant.app.services.price_ingestion_service import PriceIngestionService
from stock_quant.domain.entities.prices import RawPriceBar


def test_build_prices_filters_to_allowed_symbols() -> None:
    service = PriceIngestionService()
    raw = [
        RawPriceBar("AAPL", date(2026, 3, 12), 210.0, 213.0, 209.0, 212.0, 100, "fixture"),
        RawPriceBar("OTCM", date(2026, 3, 12), 10.0, 11.0, 9.0, 10.5, 50, "fixture"),
    ]

    result, metrics = service.build(raw, allowed_symbols={"AAPL"})

    assert len(result) == 1
    assert result[0].symbol == "AAPL"
    assert metrics["raw_bars"] == 2
    assert metrics["accepted_bars"] == 1
    assert metrics["skipped_not_in_universe"] == 1


def test_build_prices_rejects_invalid_bars() -> None:
    service = PriceIngestionService()
    raw = [
        RawPriceBar("AAPL", date(2026, 3, 12), 10.0, 9.0, 11.0, 10.5, 100, "fixture"),
        RawPriceBar("MSFT", date(2026, 3, 12), None, None, None, None, 100, "fixture"),
    ]

    result, metrics = service.build(raw, allowed_symbols={"AAPL", "MSFT"})

    assert len(result) == 0
    assert metrics["skipped_invalid"] == 2


def test_build_prices_fills_missing_ohl_with_close() -> None:
    service = PriceIngestionService()
    raw = [
        RawPriceBar("AAPL", date(2026, 3, 12), None, None, None, 212.4, None, "fixture"),
    ]

    result, metrics = service.build(raw, allowed_symbols={"AAPL"})

    assert len(result) == 1
    bar = result[0]
    assert bar.open == 212.4
    assert bar.high == 212.4
    assert bar.low == 212.4
    assert bar.close == 212.4
    assert bar.volume == 0
    assert metrics["accepted_bars"] == 1
