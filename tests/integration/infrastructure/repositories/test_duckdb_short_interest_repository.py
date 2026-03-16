from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

from stock_quant.domain.entities.short_interest import ShortInterestRecord, ShortInterestSourceFile
from stock_quant.infrastructure.db.duckdb_session_factory import DuckDbSessionFactory
from stock_quant.infrastructure.db.schema_manager import SchemaManager
from stock_quant.infrastructure.db.table_names import FINRA_SHORT_INTEREST_SOURCE_RAW
from stock_quant.infrastructure.db.unit_of_work import DuckDbUnitOfWork
from stock_quant.infrastructure.repositories.duckdb_short_interest_repository import DuckDbShortInterestRepository


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
            ("BABA", "Alibaba Group Holding Ltd ADR", None, "NYSE", "NYSE", "ADR", True, None, False, True, False, False, False, False, False, "fixture", "2026-03-14", datetime.utcnow()),
        ],
    )


def test_short_interest_repository_roundtrip_and_latest(tmp_path: Path) -> None:
    db_path = tmp_path / "test_short_interest.duckdb"
    session_factory = DuckDbSessionFactory(db_path)

    with DuckDbUnitOfWork(session_factory) as uow:
        SchemaManager(uow).initialize(drop_existing=True)
        seed_market_universe(uow)
        repo = DuckDbShortInterestRepository(uow.connection)

        uow.connection.executemany(
            f"""
            INSERT INTO {FINRA_SHORT_INTEREST_SOURCE_RAW} (
                symbol, settlement_date, short_interest, previous_short_interest, avg_daily_volume,
                shares_float, revision_flag, source_market, source_file, source_date, ingested_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                ("AAPL", "2026-02-28", 12000000, 11000000, 48000000.0, 15000000000, None, "regular", "reg_20260228.csv", "2026-02-28", datetime.utcnow()),
                ("BABA", "2026-03-15", 14800000, 15000000, 17500000.0, 2600000000, None, "regular", "reg_20260315.csv", "2026-03-15", datetime.utcnow()),
            ],
        )

        raw_rows = repo.load_raw_short_interest_records()
        assert len(raw_rows) == 2
        assert repo.load_included_symbols() == {"AAPL", "BABA"}

        history = [
            ShortInterestRecord(
                symbol="AAPL",
                settlement_date=date(2026, 2, 28),
                short_interest=12000000,
                previous_short_interest=11000000,
                avg_daily_volume=48000000.0,
                days_to_cover=0.25,
                shares_float=15000000000,
                short_interest_pct_float=0.0008,
                revision_flag=None,
                source_market="regular",
                source_file="reg_20260228.csv",
                ingested_at=datetime.utcnow(),
            ),
            ShortInterestRecord(
                symbol="BABA",
                settlement_date=date(2026, 3, 15),
                short_interest=14800000,
                previous_short_interest=15000000,
                avg_daily_volume=17500000.0,
                days_to_cover=14800000 / 17500000.0,
                shares_float=2600000000,
                short_interest_pct_float=14800000 / 2600000000,
                revision_flag=None,
                source_market="regular",
                source_file="reg_20260315.csv",
                ingested_at=datetime.utcnow(),
            ),
        ]
        sources = [
            ShortInterestSourceFile("reg_20260228.csv", "regular", date(2026, 2, 28), 1, datetime.utcnow()),
            ShortInterestSourceFile("reg_20260315.csv", "regular", date(2026, 3, 15), 1, datetime.utcnow()),
        ]

        assert repo.replace_short_interest_history(history) == 2
        assert repo.rebuild_short_interest_latest() == 2
        assert repo.replace_short_interest_sources(sources) == 2

        latest = uow.connection.execute(
            "SELECT symbol, settlement_date FROM finra_short_interest_latest ORDER BY symbol"
        ).fetchall()
        assert latest == [("AAPL", date(2026, 2, 28)), ("BABA", date(2026, 3, 15))]
