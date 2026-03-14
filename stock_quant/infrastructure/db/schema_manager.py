from __future__ import annotations

from stock_quant.infrastructure.db.table_names import (
    FINRA_SHORT_INTEREST_HISTORY,
    FINRA_SHORT_INTEREST_LATEST,
    FINRA_SHORT_INTEREST_SOURCE_RAW,
    FINRA_SHORT_INTEREST_SOURCES,
    MARKET_UNIVERSE,
    MARKET_UNIVERSE_CONFLICTS,
    NEWS_ARTICLES_RAW,
    NEWS_SOURCE_RAW,
    NEWS_SYMBOL_CANDIDATES,
    PRICE_HISTORY,
    PRICE_LATEST,
    PRICE_SOURCE_DAILY_RAW,
    PRICE_SOURCE_DAILY_RAW_ALL,
    PRICE_SOURCE_DAILY_RAW_YAHOO,
    SYMBOL_REFERENCE,
    SYMBOL_REFERENCE_SOURCE_RAW,
)
from stock_quant.infrastructure.db.unit_of_work import DuckDbUnitOfWork
from stock_quant.shared.exceptions import SchemaError


class SchemaManager:
    def __init__(self, uow: DuckDbUnitOfWork) -> None:
        self.uow = uow

    @property
    def con(self):
        if self.uow.connection is None:
            raise SchemaError("SchemaManager requires an active UnitOfWork connection")
        return self.uow.connection

    def initialize(self, drop_existing: bool = False) -> None:
        try:
            self._drop_tables_if_requested(drop_existing=drop_existing)
            self._create_symbol_reference_source_raw()
            self._create_price_source_daily_raw_all()
            self._create_price_source_daily_raw()
            self._create_price_source_daily_raw_yahoo()
            self._create_finra_short_interest_source_raw()
            self._create_news_source_raw()
            self._create_market_universe()
            self._create_market_universe_conflicts()
            self._create_symbol_reference()
            self._create_price_history()
            self._create_price_latest()
            self._create_finra_short_interest_history()
            self._create_finra_short_interest_latest()
            self._create_finra_short_interest_sources()
            self._create_news_articles_raw()
            self._create_news_symbol_candidates()
        except Exception as exc:
            raise SchemaError(f"failed to initialize schema: {exc}") from exc

    def validate(self) -> None:
        required_tables = [
            SYMBOL_REFERENCE_SOURCE_RAW,
            PRICE_SOURCE_DAILY_RAW_ALL,
            PRICE_SOURCE_DAILY_RAW,
            PRICE_SOURCE_DAILY_RAW_YAHOO,
            FINRA_SHORT_INTEREST_SOURCE_RAW,
            NEWS_SOURCE_RAW,
            MARKET_UNIVERSE,
            MARKET_UNIVERSE_CONFLICTS,
            SYMBOL_REFERENCE,
            PRICE_HISTORY,
            PRICE_LATEST,
            FINRA_SHORT_INTEREST_HISTORY,
            FINRA_SHORT_INTEREST_LATEST,
            FINRA_SHORT_INTEREST_SOURCES,
            NEWS_ARTICLES_RAW,
            NEWS_SYMBOL_CANDIDATES,
        ]
        existing = {
            row[0]
            for row in self.con.execute(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
            ).fetchall()
        }
        missing = [name for name in required_tables if name not in existing]
        if missing:
            raise SchemaError(f"missing required tables: {', '.join(missing)}")

    def _drop_tables_if_requested(self, drop_existing: bool) -> None:
        if not drop_existing:
            return
        for table_name in [
            NEWS_SYMBOL_CANDIDATES,
            NEWS_ARTICLES_RAW,
            NEWS_SOURCE_RAW,
            FINRA_SHORT_INTEREST_SOURCES,
            FINRA_SHORT_INTEREST_LATEST,
            FINRA_SHORT_INTEREST_HISTORY,
            SYMBOL_REFERENCE,
            PRICE_LATEST,
            PRICE_HISTORY,
            MARKET_UNIVERSE_CONFLICTS,
            MARKET_UNIVERSE,
            FINRA_SHORT_INTEREST_SOURCE_RAW,
            PRICE_SOURCE_DAILY_RAW_YAHOO,
            PRICE_SOURCE_DAILY_RAW,
            PRICE_SOURCE_DAILY_RAW_ALL,
            SYMBOL_REFERENCE_SOURCE_RAW,
        ]:
            self.con.execute(f"DROP TABLE IF EXISTS {table_name}")

    def _create_symbol_reference_source_raw(self) -> None:
        self.con.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {SYMBOL_REFERENCE_SOURCE_RAW} (
                symbol VARCHAR,
                company_name VARCHAR,
                cik VARCHAR,
                exchange_raw VARCHAR,
                security_type_raw VARCHAR,
                source_name VARCHAR,
                as_of_date DATE,
                ingested_at TIMESTAMP
            )
            """
        )

    def _create_price_source_daily_raw_all(self) -> None:
        self.con.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {PRICE_SOURCE_DAILY_RAW_ALL} (
                symbol VARCHAR,
                price_date DATE,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                volume BIGINT,
                source_name VARCHAR,
                source_path VARCHAR,
                asset_class VARCHAR,
                venue_group VARCHAR,
                ingested_at TIMESTAMP
            )
            """
        )

    def _create_price_source_daily_raw(self) -> None:
        self.con.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {PRICE_SOURCE_DAILY_RAW} (
                symbol VARCHAR,
                price_date DATE,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                volume BIGINT,
                source_name VARCHAR,
                ingested_at TIMESTAMP
            )
            """
        )

    def _create_price_source_daily_raw_yahoo(self) -> None:
        self.con.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {PRICE_SOURCE_DAILY_RAW_YAHOO} (
                symbol VARCHAR,
                price_date DATE,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                volume BIGINT,
                source_name VARCHAR,
                fetched_at TIMESTAMP
            )
            """
        )

    def _create_finra_short_interest_source_raw(self) -> None:
        self.con.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {FINRA_SHORT_INTEREST_SOURCE_RAW} (
                symbol VARCHAR,
                settlement_date DATE,
                short_interest BIGINT,
                previous_short_interest BIGINT,
                avg_daily_volume DOUBLE,
                shares_float BIGINT,
                revision_flag VARCHAR,
                source_market VARCHAR,
                source_file VARCHAR,
                source_date DATE,
                ingested_at TIMESTAMP
            )
            """
        )

    def _create_news_source_raw(self) -> None:
        self.con.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {NEWS_SOURCE_RAW} (
                raw_id BIGINT,
                published_at TIMESTAMP,
                source_name VARCHAR,
                title VARCHAR,
                article_url VARCHAR,
                domain VARCHAR,
                raw_payload_json VARCHAR,
                ingested_at TIMESTAMP
            )
            """
        )

    def _create_market_universe(self) -> None:
        self.con.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {MARKET_UNIVERSE} (
                symbol VARCHAR,
                company_name VARCHAR,
                cik VARCHAR,
                exchange_raw VARCHAR,
                exchange_normalized VARCHAR,
                security_type VARCHAR,
                include_in_universe BOOLEAN,
                exclusion_reason VARCHAR,
                is_common_stock BOOLEAN,
                is_adr BOOLEAN,
                is_etf BOOLEAN,
                is_preferred BOOLEAN,
                is_warrant BOOLEAN,
                is_right BOOLEAN,
                is_unit BOOLEAN,
                source_name VARCHAR,
                as_of_date DATE,
                created_at TIMESTAMP
            )
            """
        )

    def _create_market_universe_conflicts(self) -> None:
        self.con.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {MARKET_UNIVERSE_CONFLICTS} (
                symbol VARCHAR,
                chosen_source VARCHAR,
                rejected_source VARCHAR,
                reason VARCHAR,
                payload_json VARCHAR,
                created_at TIMESTAMP
            )
            """
        )

    def _create_symbol_reference(self) -> None:
        self.con.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {SYMBOL_REFERENCE} (
                symbol VARCHAR,
                cik VARCHAR,
                company_name VARCHAR,
                company_name_clean VARCHAR,
                aliases_json VARCHAR,
                exchange VARCHAR,
                source_name VARCHAR,
                symbol_match_enabled BOOLEAN,
                name_match_enabled BOOLEAN,
                created_at TIMESTAMP
            )
            """
        )

    def _create_price_history(self) -> None:
        self.con.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {PRICE_HISTORY} (
                symbol VARCHAR,
                price_date DATE,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                volume BIGINT,
                source_name VARCHAR,
                ingested_at TIMESTAMP
            )
            """
        )

    def _create_price_latest(self) -> None:
        self.con.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {PRICE_LATEST} (
                symbol VARCHAR,
                latest_price_date DATE,
                close DOUBLE,
                volume BIGINT,
                source_name VARCHAR,
                updated_at TIMESTAMP
            )
            """
        )

    def _create_finra_short_interest_history(self) -> None:
        self.con.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {FINRA_SHORT_INTEREST_HISTORY} (
                symbol VARCHAR,
                settlement_date DATE,
                short_interest BIGINT,
                previous_short_interest BIGINT,
                avg_daily_volume DOUBLE,
                days_to_cover DOUBLE,
                shares_float BIGINT,
                short_interest_pct_float DOUBLE,
                revision_flag VARCHAR,
                source_market VARCHAR,
                source_file VARCHAR,
                ingested_at TIMESTAMP
            )
            """
        )

    def _create_finra_short_interest_latest(self) -> None:
        self.con.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {FINRA_SHORT_INTEREST_LATEST} (
                symbol VARCHAR,
                settlement_date DATE,
                short_interest BIGINT,
                previous_short_interest BIGINT,
                avg_daily_volume DOUBLE,
                days_to_cover DOUBLE,
                shares_float BIGINT,
                short_interest_pct_float DOUBLE,
                revision_flag VARCHAR,
                source_market VARCHAR,
                source_file VARCHAR,
                updated_at TIMESTAMP
            )
            """
        )

    def _create_finra_short_interest_sources(self) -> None:
        self.con.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {FINRA_SHORT_INTEREST_SOURCES} (
                source_file VARCHAR,
                source_market VARCHAR,
                source_date DATE,
                row_count BIGINT,
                loaded_at TIMESTAMP
            )
            """
        )

    def _create_news_articles_raw(self) -> None:
        self.con.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {NEWS_ARTICLES_RAW} (
                raw_id BIGINT,
                published_at TIMESTAMP,
                source_name VARCHAR,
                title VARCHAR,
                article_url VARCHAR,
                domain VARCHAR,
                raw_payload_json VARCHAR,
                ingested_at TIMESTAMP
            )
            """
        )

    def _create_news_symbol_candidates(self) -> None:
        self.con.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {NEWS_SYMBOL_CANDIDATES} (
                raw_id BIGINT,
                symbol VARCHAR,
                match_type VARCHAR,
                match_score DOUBLE,
                matched_text VARCHAR,
                created_at TIMESTAMP
            )
            """
        )
