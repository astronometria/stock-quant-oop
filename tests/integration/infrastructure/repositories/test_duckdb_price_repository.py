from __future__ import annotations

from datetime import datetime
from pathlib import Path

from stock_quant.domain.entities.prices import PriceBar
from stock_quant.infrastructure.db.duckdb_session_factory import DuckDbSessionFactory
from stock_quant.infrastructure.db.schema_manager import SchemaManager
from stock_quant.infrastructure.db.table_names import PRICE_SOURCE_DAILY_RAW
from stock_quant.infrastructure.db.unit_of_work import DuckDbUnitOfWork
from stock_quant.infrastructure.repositories.duckdb_price_repository import DuckDbPriceRepository


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
            ("MSFT", "Microsoft Corporation", "0000789019", "NASDAQ", "NASDAQ", "COMMON_STOCK", True, None, True, False, False, False, False, False, False, "fixture", "2026-03-14", datetime.utcnow()),
        ],
    )


def test_price_repository_roundtrip_and_latest(tmp_path: Path) -> None:
    db_path = tmp_path / "test_prices.duckdb"
    session_factory = DuckDbSessionFactory(db_path)

    with DuckDbUnitOfWork(session_factory) as uow:
        SchemaManager(uow).initialize(drop_existing=True)
        seed_market_universe(uow)

        repo = DuckDbPriceRepository(uow.connection)

        uow.connection.executemany(
            f"""
            INSERT INTO {PRICE_SOURCE_DAILY_RAW} (
                symbol, price_date, open, high, low, close, volume, source_name, ingested_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                ("AAPL", "2026-03-12", 210.0, 213.0, 209.0, 212.0, 100, "fixture", datetime.utcnow()),
                ("MSFT", "2026-03-12", 400.0, 405.0, 399.0, 404.0, 200, "fixture", datetime.utcnow()),
            ],
        )

        raw_bars = repo.load_raw_price_bars()
        assert len(raw_bars) == 2
        assert repo.load_included_symbols() == {"AAPL", "MSFT"}

        bars = [
            PriceBar("AAPL", raw_bars[0].price_date, 210.0, 213.0, 209.0, 212.0, 100, "fixture", datetime.utcnow()),
            PriceBar("MSFT", raw_bars[1].price_date, 400.0, 405.0, 399.0, 404.0, 200, "fixture", datetime.utcnow()),
        ]

        assert repo.replace_price_history(bars) == 2
        assert repo.rebuild_price_latest() == 2

        latest = uow.connection.execute(
            "SELECT symbol, close FROM price_latest ORDER BY symbol"
        ).fetchall()
        assert latest == [("AAPL", 212.0), ("MSFT", 404.0)]
