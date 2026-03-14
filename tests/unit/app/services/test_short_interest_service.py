from datetime import date

from stock_quant.app.services.short_interest_service import ShortInterestService
from stock_quant.domain.entities.short_interest import RawShortInterestRecord
from stock_quant.domain.policies.finra_market_selection_policy import FinraMarketSelectionPolicy


def build_service() -> ShortInterestService:
    return ShortInterestService(market_policy=FinraMarketSelectionPolicy())


def test_build_short_interest_filters_market_and_universe() -> None:
    service = build_service()
    raw = [
        RawShortInterestRecord(
            symbol="AAPL",
            settlement_date=date(2026, 2, 28),
            short_interest=12000000,
            previous_short_interest=11000000,
            avg_daily_volume=48000000.0,
            shares_float=15000000000,
            revision_flag=None,
            source_market="regular",
            source_file="reg.csv",
            source_date=date(2026, 2, 28),
        ),
        RawShortInterestRecord(
            symbol="OTCM",
            settlement_date=date(2026, 2, 28),
            short_interest=10000,
            previous_short_interest=9000,
            avg_daily_volume=5000.0,
            shares_float=2000000,
            revision_flag=None,
            source_market="otc",
            source_file="otc.csv",
            source_date=date(2026, 2, 28),
        ),
        RawShortInterestRecord(
            symbol="SPY",
            settlement_date=date(2026, 2, 28),
            short_interest=50000,
            previous_short_interest=45000,
            avg_daily_volume=120000.0,
            shares_float=None,
            revision_flag=None,
            source_market="regular",
            source_file="reg.csv",
            source_date=date(2026, 2, 28),
        ),
    ]

    history, sources, metrics = service.build(
        raw,
        allowed_symbols={"AAPL"},
        source_market="regular",
    )

    assert len(history) == 1
    assert len(sources) == 1
    assert history[0].symbol == "AAPL"
    assert history[0].days_to_cover == 12000000 / 48000000.0
    assert metrics["accepted_records"] == 1
    assert metrics["skipped_market_mismatch"] == 1
    assert metrics["skipped_not_in_universe"] == 1


def test_build_short_interest_computes_ratios() -> None:
    service = build_service()
    raw = [
        RawShortInterestRecord(
            symbol="BABA",
            settlement_date=date(2026, 3, 15),
            short_interest=14800000,
            previous_short_interest=15000000,
            avg_daily_volume=17500000.0,
            shares_float=2600000000,
            revision_flag=None,
            source_market="regular",
            source_file="reg_20260315.csv",
            source_date=date(2026, 3, 15),
        ),
    ]

    history, sources, metrics = service.build(
        raw,
        allowed_symbols={"BABA"},
        source_market="regular",
    )

    assert len(history) == 1
    rec = history[0]
    assert round(rec.days_to_cover or 0.0, 4) == round(14800000 / 17500000.0, 4)
    assert round(rec.short_interest_pct_float or 0.0, 6) == round(14800000 / 2600000000, 6)
    assert len(sources) == 1
    assert metrics["accepted_source_files"] == 1
