from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

from stock_quant.domain.entities.universe import RawUniverseCandidate, UniverseConflict, UniverseEntry
from stock_quant.infrastructure.db.duckdb_session_factory import DuckDbSessionFactory
from stock_quant.infrastructure.db.schema_manager import SchemaManager
from stock_quant.infrastructure.db.table_names import SYMBOL_REFERENCE_SOURCE_RAW
from stock_quant.infrastructure.db.unit_of_work import DuckDbUnitOfWork
from stock_quant.infrastructure.repositories.duckdb_universe_repository import DuckDbUniverseRepository


def test_universe_repository_roundtrip(tmp_path: Path) -> None:
    db_path = tmp_path / "test_universe.duckdb"
    session_factory = DuckDbSessionFactory(db_path)

    with DuckDbUnitOfWork(session_factory) as uow:
        SchemaManager(uow).initialize(drop_existing=True)
        repo = DuckDbUniverseRepository(uow)

        entries = [
            UniverseEntry(
                symbol="AAPL",
                company_name="Apple Inc.",
                cik="0000320193",
                exchange_raw="NASDAQGS",
                exchange_normalized="NASDAQ",
                security_type="COMMON_STOCK",
                include_in_universe=True,
                exclusion_reason=None,
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
            ),
            UniverseEntry(
                symbol="SPY",
                company_name="SPDR S&P 500 ETF Trust",
                cik=None,
                exchange_raw="NYSEARCA",
                exchange_normalized="NYSE_ARCA",
                security_type="ETF",
                include_in_universe=False,
                exclusion_reason="etf_excluded",
                is_common_stock=False,
                is_adr=False,
                is_etf=True,
                is_preferred=False,
                is_warrant=False,
                is_right=False,
                is_unit=False,
                source_name="fixture",
                as_of_date=date(2026, 3, 14),
                created_at=datetime.utcnow(),
            ),
        ]
        conflicts = [
            UniverseConflict(
                symbol="AAPL",
                chosen_source="sec_fixture",
                rejected_source="otc_fixture",
                reason="duplicate_symbol_resolution",
                payload_json='{"chosen_exchange":"NASDAQ"}',
                created_at=datetime.utcnow(),
            )
        ]

        assert repo.replace_universe(entries) == 2
        assert repo.replace_conflicts(conflicts) == 1

        fetched = repo.fetch_all_universe_entries()
        assert len(fetched) == 2
        assert {x.symbol for x in fetched} == {"AAPL", "SPY"}

        conflict_count = uow.connection.execute(
            "SELECT COUNT(*) FROM market_universe_conflicts"
        ).fetchone()[0]
        assert conflict_count == 1


def test_universe_repository_load_raw_candidates(tmp_path: Path) -> None:
    db_path = tmp_path / "test_universe_raw.duckdb"
    session_factory = DuckDbSessionFactory(db_path)

    with DuckDbUnitOfWork(session_factory) as uow:
        SchemaManager(uow).initialize(drop_existing=True)
        uow.connection.executemany(
            f"""
            INSERT INTO {SYMBOL_REFERENCE_SOURCE_RAW} (
                symbol, company_name, cik, exchange_raw, security_type_raw, source_name, as_of_date, ingested_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                ("AAPL", "Apple Inc.", "0000320193", "NASDAQGS", "Common Stock", "fixture", "2026-03-14", datetime.utcnow()),
                ("MSFT", "Microsoft Corporation", "0000789019", "NASDAQ", "Common Stock", "fixture", "2026-03-14", datetime.utcnow()),
            ],
        )

        repo = DuckDbUniverseRepository(uow)
        rows = repo.load_raw_candidates()

        assert len(rows) == 2
        assert isinstance(rows[0], RawUniverseCandidate)
        assert {x.symbol for x in rows} == {"AAPL", "MSFT"}
