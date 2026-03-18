from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

import pandas as pd

from stock_quant.infrastructure.db.duckdb_session_factory import DuckDbSessionFactory
from stock_quant.infrastructure.db.schema_manager import SchemaManager
from stock_quant.infrastructure.db.unit_of_work import DuckDbUnitOfWork
from stock_quant.infrastructure.repositories.duckdb_price_repository import DuckDbPriceRepository


def seed_market_universe(uow) -> None:
    """
    Seed minimal du market_universe pour tester la résolution de scope
    des symboles via l'API moderne get_refresh_symbols().
    """
    uow.connection.executemany(
        """
        INSERT INTO market_universe (
            symbol,
            company_name,
            cik,
            exchange_raw,
            exchange_normalized,
            security_type,
            include_in_universe,
            exclusion_reason,
            is_common_stock,
            is_adr,
            is_etf,
            is_preferred,
            is_warrant,
            is_right,
            is_unit,
            source_name,
            as_of_date,
            created_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                "AAPL",
                "Apple Inc.",
                "0000320193",
                "NASDAQGS",
                "NASDAQ",
                "COMMON_STOCK",
                True,
                None,
                True,
                False,
                False,
                False,
                False,
                False,
                False,
                "fixture",
                "2026-03-14",
                datetime.utcnow(),
            ),
            (
                "MSFT",
                "Microsoft Corporation",
                "0000789019",
                "NASDAQ",
                "NASDAQ",
                "COMMON_STOCK",
                True,
                None,
                True,
                False,
                False,
                False,
                False,
                False,
                False,
                "fixture",
                "2026-03-14",
                datetime.utcnow(),
            ),
        ],
    )


def test_price_repository_upsert_and_refresh_latest_modern_api(tmp_path: Path) -> None:
    """
    Test d'intégration research-grade du repository prix.

    Ce test valide uniquement l'API moderne :
    - get_refresh_symbols()
    - upsert_price_history()
    - refresh_price_latest()

    On évite volontairement toute dépendance à une API legacy.
    """
    db_path = tmp_path / "test_prices.duckdb"
    session_factory = DuckDbSessionFactory(db_path)

    with DuckDbUnitOfWork(session_factory) as uow:
        SchemaManager(uow).initialize(drop_existing=True)
        seed_market_universe(uow)

    repo = DuckDbPriceRepository(str(db_path))

    symbols, scope_source = repo.get_refresh_symbols()
    assert symbols == ["AAPL", "MSFT"]
    assert scope_source == "market_universe_latest_as_of"

    frame = pd.DataFrame(
        [
            {
                "symbol": "AAPL",
                "price_date": "2026-03-12",
                "open": 210.0,
                "high": 213.0,
                "low": 209.0,
                "close": 212.0,
                "volume": 100,
            },
            {
                "symbol": "MSFT",
                "price_date": "2026-03-12",
                "open": 400.0,
                "high": 405.0,
                "low": 399.0,
                "close": 404.0,
                "volume": 200,
            },
        ]
    )

    write_result = repo.upsert_price_history(
        frame,
        source_name="fixture",
        ingested_at=datetime.utcnow(),
    )

    assert write_result["input_rows"] == 2
    assert write_result["staged_rows"] == 2
    assert write_result["inserted_rows"] == 2

    latest_result = repo.refresh_price_latest()
    assert latest_result["inserted_rows"] == 2

    with DuckDbUnitOfWork(session_factory) as uow:
        price_history_rows = uow.connection.execute(
            """
            SELECT symbol, price_date, close
            FROM price_history
            ORDER BY symbol, price_date
            """
        ).fetchall()

        assert price_history_rows == [
            ("AAPL", date(2026, 3, 12), 212.0),
            ("MSFT", date(2026, 3, 12), 404.0),
        ]

        latest_rows = uow.connection.execute(
            """
            SELECT symbol, latest_price_date, close
            FROM price_latest
            ORDER BY symbol
            """
        ).fetchall()

        assert latest_rows == [
            ("AAPL", date(2026, 3, 12), 212.0),
            ("MSFT", date(2026, 3, 12), 404.0),
        ]
