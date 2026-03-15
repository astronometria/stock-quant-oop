from __future__ import annotations

from stock_quant.infrastructure.db.unit_of_work import DuckDbUnitOfWork


class ShortDataSchema:
    def __init__(self, uow: DuckDbUnitOfWork) -> None:
        self.uow = uow

    def initialize(self) -> None:
        with self.uow as uow:
            con = uow.connection

            self._create_finra_daily_short_volume_source_raw(con)
            self._create_short_interest_history(con)
            self._create_daily_short_volume_history(con)
            self._create_short_features_daily(con)

            self._evolve_finra_daily_short_volume_source_raw(con)
            self._evolve_short_interest_history(con)
            self._evolve_daily_short_volume_history(con)
            self._evolve_short_features_daily(con)

            self._create_short_indexes(con)

    def validate(self) -> None:
        required_tables = (
            "finra_daily_short_volume_source_raw",
            "short_interest_history",
            "daily_short_volume_history",
            "short_features_daily",
        )
        with self.uow as uow:
            con = uow.connection
            existing = {
                row[0]
                for row in con.execute(
                    """
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = 'main'
                    """
                ).fetchall()
            }
        missing = [name for name in required_tables if name not in existing]
        if missing:
            raise RuntimeError(f"missing short-data tables: {missing}")

    def _create_finra_daily_short_volume_source_raw(self, con) -> None:
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS finra_daily_short_volume_source_raw (
                symbol VARCHAR,
                trade_date DATE,
                short_volume DOUBLE,
                short_exempt_volume DOUBLE,
                total_volume DOUBLE,
                market_code VARCHAR,
                source_file VARCHAR,
                source_name VARCHAR,
                publication_date DATE,
                available_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

    def _create_short_interest_history(self, con) -> None:
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS short_interest_history (
                instrument_id VARCHAR,
                company_id VARCHAR,
                symbol VARCHAR,
                settlement_date DATE,
                publication_date DATE,
                available_at TIMESTAMP,
                short_interest DOUBLE,
                previous_short_interest DOUBLE,
                avg_daily_volume DOUBLE,
                days_to_cover DOUBLE,
                source_name VARCHAR,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

    def _create_daily_short_volume_history(self, con) -> None:
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS daily_short_volume_history (
                instrument_id VARCHAR,
                company_id VARCHAR,
                symbol VARCHAR,
                trade_date DATE,
                publication_date DATE,
                available_at TIMESTAMP,
                short_volume DOUBLE,
                short_exempt_volume DOUBLE,
                total_volume DOUBLE,
                short_volume_ratio DOUBLE,
                source_name VARCHAR,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

    def _create_short_features_daily(self, con) -> None:
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS short_features_daily (
                instrument_id VARCHAR,
                company_id VARCHAR,
                symbol VARCHAR,
                as_of_date DATE,
                short_interest DOUBLE,
                avg_daily_volume DOUBLE,
                days_to_cover DOUBLE,
                short_volume DOUBLE,
                short_exempt_volume DOUBLE,
                total_volume DOUBLE,
                short_volume_ratio DOUBLE,
                short_interest_change DOUBLE,
                short_interest_change_pct DOUBLE,
                short_squeeze_score DOUBLE,
                short_pressure_zscore DOUBLE,
                days_to_cover_zscore DOUBLE,
                max_source_available_at TIMESTAMP,
                source_name VARCHAR,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

    def _evolve_finra_daily_short_volume_source_raw(self, con) -> None:
        self._add_column_if_missing(con, "finra_daily_short_volume_source_raw", "short_exempt_volume", "DOUBLE")
        self._add_column_if_missing(con, "finra_daily_short_volume_source_raw", "market_code", "VARCHAR")
        self._add_column_if_missing(con, "finra_daily_short_volume_source_raw", "source_file", "VARCHAR")
        self._add_column_if_missing(con, "finra_daily_short_volume_source_raw", "publication_date", "DATE")
        self._add_column_if_missing(con, "finra_daily_short_volume_source_raw", "available_at", "TIMESTAMP")

    def _evolve_short_interest_history(self, con) -> None:
        self._add_column_if_missing(con, "short_interest_history", "publication_date", "DATE")
        self._add_column_if_missing(con, "short_interest_history", "available_at", "TIMESTAMP")

    def _evolve_daily_short_volume_history(self, con) -> None:
        self._add_column_if_missing(con, "daily_short_volume_history", "publication_date", "DATE")
        self._add_column_if_missing(con, "daily_short_volume_history", "available_at", "TIMESTAMP")
        self._add_column_if_missing(con, "daily_short_volume_history", "short_exempt_volume", "DOUBLE")

    def _evolve_short_features_daily(self, con) -> None:
        self._add_column_if_missing(con, "short_features_daily", "short_exempt_volume", "DOUBLE")
        self._add_column_if_missing(con, "short_features_daily", "max_source_available_at", "TIMESTAMP")

    def _create_short_indexes(self, con) -> None:
        self._create_index_if_columns_exist(con, "short_interest_history", "idx_short_interest_history_symbol_settlement", "symbol, settlement_date")
        self._create_index_if_columns_exist(con, "short_interest_history", "idx_short_interest_history_instrument_settlement", "instrument_id, settlement_date")
        self._create_index_if_columns_exist(con, "short_interest_history", "idx_short_interest_history_available_at", "available_at")

        self._create_index_if_columns_exist(con, "daily_short_volume_history", "idx_daily_short_volume_history_symbol_trade_date", "symbol, trade_date")
        self._create_index_if_columns_exist(con, "daily_short_volume_history", "idx_daily_short_volume_history_instrument_trade_date", "instrument_id, trade_date")
        self._create_index_if_columns_exist(con, "daily_short_volume_history", "idx_daily_short_volume_history_available_at", "available_at")

        self._create_index_if_columns_exist(con, "short_features_daily", "idx_short_features_daily_instrument_asof", "instrument_id, as_of_date")
        self._create_index_if_columns_exist(con, "short_features_daily", "idx_short_features_daily_symbol_asof", "symbol, as_of_date")

    def _add_column_if_missing(self, con, table_name: str, column_name: str, column_type: str) -> None:
        existing = {row[1] for row in con.execute(f"PRAGMA table_info('{table_name}')").fetchall()}
        if column_name not in existing:
            con.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}")

    def _create_index_if_columns_exist(self, con, table_name: str, index_name: str, column_sql: str) -> None:
        existing = {row[1] for row in con.execute(f"PRAGMA table_info('{table_name}')").fetchall()}
        needed = [col.strip() for col in column_sql.split(",")]
        if all(col in existing for col in needed):
            con.execute(f"CREATE INDEX IF NOT EXISTS {index_name} ON {table_name}({column_sql})")


class ShortDataSchemaManager(ShortDataSchema):
    pass
