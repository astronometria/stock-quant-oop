from datetime import date

from stock_quant.app.services.market_universe_service import MarketUniverseService
from stock_quant.domain.entities.universe import RawUniverseCandidate
from stock_quant.domain.policies.exchange_normalization_policy import ExchangeNormalizationPolicy
from stock_quant.domain.policies.security_classification_policy import SecurityClassificationPolicy
from stock_quant.domain.policies.universe_conflict_resolution_policy import UniverseConflictResolutionPolicy
from stock_quant.domain.policies.universe_inclusion_policy import UniverseInclusionPolicy


def build_service() -> MarketUniverseService:
    return MarketUniverseService(
        exchange_policy=ExchangeNormalizationPolicy(),
        classification_policy=SecurityClassificationPolicy(),
        inclusion_policy=UniverseInclusionPolicy(),
        conflict_policy=UniverseConflictResolutionPolicy(),
    )


def test_build_universe_filters_and_resolves_duplicates() -> None:
    service = build_service()
    raw = [
        RawUniverseCandidate(
            symbol="AAPL",
            company_name="Apple Inc.",
            cik="0000320193",
            exchange_raw="NASDAQGS",
            security_type_raw="Common Stock",
            source_name="sec_fixture",
            as_of_date=date(2026, 3, 14),
        ),
        RawUniverseCandidate(
            symbol="AAPL",
            company_name="Apple Inc. duplicate worse row",
            cik=None,
            exchange_raw="OTCQX",
            security_type_raw="Common Stock",
            source_name="otc_fixture",
            as_of_date=date(2026, 3, 14),
        ),
        RawUniverseCandidate(
            symbol="SPY",
            company_name="SPDR S&P 500 ETF Trust",
            cik=None,
            exchange_raw="NYSEARCA",
            security_type_raw="ETF",
            source_name="etf_fixture",
            as_of_date=date(2026, 3, 14),
        ),
    ]

    final_entries, conflicts, metrics = service.build(raw, allow_adr=True)

    assert len(final_entries) == 2
    assert len(conflicts) == 1
    assert metrics["raw_candidates"] == 3
    assert metrics["final_entries"] == 2
    assert metrics["included_final_entries"] == 1

    aapl = next(x for x in final_entries if x.symbol == "AAPL")
    spy = next(x for x in final_entries if x.symbol == "SPY")

    assert aapl.include_in_universe is True
    assert aapl.exchange_normalized == "NASDAQ"
    assert aapl.source_name == "sec_fixture"

    assert spy.include_in_universe is False
    assert spy.exclusion_reason == "etf_excluded"


def test_build_universe_can_reject_invalid_symbols() -> None:
    service = build_service()
    raw = [
        RawUniverseCandidate(
            symbol=None,
            company_name="Broken",
            cik=None,
            exchange_raw="NASDAQ",
            security_type_raw="Common Stock",
            source_name="bad_fixture",
            as_of_date=date(2026, 3, 14),
        ),
        RawUniverseCandidate(
            symbol="MSFT",
            company_name="Microsoft Corporation",
            cik="0000789019",
            exchange_raw="NASDAQ",
            security_type_raw="Common Stock",
            source_name="sec_fixture",
            as_of_date=date(2026, 3, 14),
        ),
    ]

    final_entries, conflicts, metrics = service.build(raw, allow_adr=True)

    assert len(final_entries) == 1
    assert len(conflicts) == 0
    assert metrics["skipped_invalid"] == 1
    assert final_entries[0].symbol == "MSFT"
