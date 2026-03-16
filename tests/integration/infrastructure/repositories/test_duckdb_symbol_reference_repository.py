from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

from stock_quant.domain.entities.symbol_reference import SymbolReferenceEntry
from stock_quant.infrastructure.db.duckdb_session_factory import DuckDbSessionFactory
from stock_quant.infrastructure.db.schema_manager import SchemaManager
from stock_quant.infrastructure.db.unit_of_work import DuckDbUnitOfWork
from stock_quant.infrastructure.repositories.duckdb_symbol_reference_repository import DuckDbSymbolReferenceRepository


def seed_market_universe(uow) -> None:
    uow.connection.executemany(
        """
        INSERT INTO market_universe (
            symbol, company_name, cik, exchange_raw, exchange_normalized, security_type,
            include_in_universe, exclusion_reason, is_common_stock, is_adr, is_etf,
            is_preferred, is_warrant, is_right, is_unit, source_name, as_of_date, created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            ("AAPL", "Apple Inc.", "0000320193", "NASDAQGS", "NASDAQ", "COMMON_STOCK", True, None, True, False, False, False, False, False, False, "fixture", "2026-03-14", datetime.utcnow()),
            ("SPY", "SPDR S&P 500 ETF Trust", None, "NYSEARCA", "NYSE_ARCA", "ETF", False, "etf_excluded", False, False, True, False, False, False, False, "fixture", "2026-03-14", datetime.utcnow()),
        ],
    )


def test_symbol_reference_repository_roundtrip(tmp_path: Path) -> None:
    db_path = tmp_path / "test_symbol_reference.duckdb"
    session_factory = DuckDbSessionFactory(db_path)

    with DuckDbUnitOfWork(session_factory) as uow:
        SchemaManager(uow).initialize(drop_existing=True)
        seed_market_universe(uow)

        repo = DuckDbSymbolReferenceRepository(uow.connection)

        included = repo.load_included_universe_entries()
        assert len(included) == 1
        assert included[0].symbol == "AAPL"

        entries = [
            SymbolReferenceEntry(
                symbol="AAPL",
                cik="0000320193",
                company_name="Apple Inc.",
                company_name_clean="APPLE INC",
                aliases_json='["APPLE INC","APPLE"]',
                exchange="NASDAQ",
                source_name="fixture",
                symbol_match_enabled=True,
                name_match_enabled=True,
                created_at=datetime.utcnow(),
            )
        ]
        assert repo.replace_symbol_reference(entries) == 1

        count = uow.connection.execute("SELECT COUNT(*) FROM symbol_reference").fetchone()[0]
        assert count == 1
