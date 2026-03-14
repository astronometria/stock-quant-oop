import json
from datetime import date, datetime

from stock_quant.app.services.symbol_reference_service import SymbolReferenceService
from stock_quant.domain.entities.universe import UniverseEntry
from stock_quant.domain.policies.alias_generation_policy import AliasGenerationPolicy


def build_entry(symbol: str, company_name: str, include: bool = True, exchange: str = "NASDAQ") -> UniverseEntry:
    return UniverseEntry(
        symbol=symbol,
        company_name=company_name,
        cik=None,
        exchange_raw=exchange,
        exchange_normalized=exchange,
        security_type="COMMON_STOCK",
        include_in_universe=include,
        exclusion_reason=None if include else "excluded",
        is_common_stock=True,
        is_adr=False,
        is_etf=False,
        is_preferred=False,
        is_warrant=False,
        is_right=False,
        is_unit=False,
        source_name="fixture",
        as_of_date=date(2026, 3, 14),
        created_at=datetime.utcnow(),
    )


def test_build_symbol_reference_keeps_only_included_entries() -> None:
    service = SymbolReferenceService(alias_policy=AliasGenerationPolicy())
    entries = [
        build_entry("AAPL", "Apple Inc.", include=True),
        build_entry("SPY", "SPDR S&P 500 ETF Trust", include=False, exchange="NYSE_ARCA"),
    ]

    result, metrics = service.build(entries)

    assert len(result) == 1
    assert metrics["input_entries"] == 2
    assert metrics["output_entries"] == 1

    ref = result[0]
    assert ref.symbol == "AAPL"
    assert ref.company_name_clean == "APPLE INC"
    assert ref.symbol_match_enabled is True
    assert ref.name_match_enabled is True

    aliases = json.loads(ref.aliases_json)
    assert "APPLE INC" in aliases
    assert "APPLE" in aliases


def test_build_symbol_reference_handles_adr_aliases() -> None:
    service = SymbolReferenceService(alias_policy=AliasGenerationPolicy())
    entry = UniverseEntry(
        symbol="BABA",
        company_name="Alibaba Group Holding Ltd ADR",
        cik=None,
        exchange_raw="NYSE",
        exchange_normalized="NYSE",
        security_type="ADR",
        include_in_universe=True,
        exclusion_reason=None,
        is_common_stock=False,
        is_adr=True,
        is_etf=False,
        is_preferred=False,
        is_warrant=False,
        is_right=False,
        is_unit=False,
        source_name="fixture",
        as_of_date=date(2026, 3, 14),
        created_at=datetime.utcnow(),
    )

    result, metrics = service.build([entry])

    assert len(result) == 1
    assert metrics["output_entries"] == 1
    aliases = json.loads(result[0].aliases_json)
    assert "ALIBABA" in aliases
