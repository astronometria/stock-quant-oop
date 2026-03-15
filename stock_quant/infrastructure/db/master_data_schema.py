from __future__ import annotations

from stock_quant.infrastructure.db.unit_of_work import DuckDbUnitOfWork


class MasterDataSchema:
    def __init__(self, uow: DuckDbUnitOfWork) -> None:
        self.uow = uow

    def initialize(self) -> None:
        with self.uow as uow:
            con = uow.connection

            self._create_company_master(con)
            self._create_instrument_master(con)
            self._create_ticker_history(con)
            self._create_instrument_identifier_map(con)
            self._create_universe_membership_history(con)

            self._evolve_company_master(con)
            self._evolve_instrument_master(con)
            self._evolve_ticker_history(con)
            self._evolve_instrument_identifier_map(con)
            self._evolve_universe_membership_history(con)

            self._create_company_master_indexes(con)
            self._create_instrument_master_indexes(con)
            self._create_ticker_history_indexes(con)
            self._create_instrument_identifier_map_indexes(con)
            self._create_universe_membership_history_indexes(con)

    def validate(self) -> None:
        required_tables = (
            "company_master",
            "instrument_master",
            "ticker_history",
            "instrument_identifier_map",
            "universe_membership_history",
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
            raise RuntimeError(f"missing master-data tables: {missing}")

    def _create_company_master(self, con) -> None:
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS company_master (
                company_id VARCHAR,
                company_name VARCHAR,
                cik VARCHAR,
                lei VARCHAR,
                country_code VARCHAR,
                region_code VARCHAR,
                is_active BOOLEAN,
                source_name VARCHAR,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

    def _create_instrument_master(self, con) -> None:
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS instrument_master (
                instrument_id VARCHAR,
                company_id VARCHAR,
                symbol VARCHAR,
                instrument_type VARCHAR,
                primary_exchange VARCHAR,
                mic_code VARCHAR,
                currency_code VARCHAR,
                is_active BOOLEAN,
                listing_date DATE,
                delisting_date DATE,
                source_name VARCHAR,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

    def _create_ticker_history(self, con) -> None:
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS ticker_history (
                instrument_id VARCHAR,
                company_id VARCHAR,
                symbol VARCHAR,
                ticker VARCHAR,
                valid_from DATE,
                valid_to DATE,
                is_primary BOOLEAN,
                source_name VARCHAR,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

    def _create_instrument_identifier_map(self, con) -> None:
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS instrument_identifier_map (
                instrument_id VARCHAR,
                identifier_type VARCHAR,
                identifier_value VARCHAR,
                valid_from DATE,
                valid_to DATE,
                is_primary BOOLEAN,
                source_name VARCHAR,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

    def _create_universe_membership_history(self, con) -> None:
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS universe_membership_history (
                instrument_id VARCHAR,
                company_id VARCHAR,
                symbol VARCHAR,
                universe_name VARCHAR,
                effective_from DATE,
                effective_to DATE,
                membership_status VARCHAR,
                reason VARCHAR,
                source_name VARCHAR,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

    def _evolve_company_master(self, con) -> None:
        self._add_column_if_missing(con, "company_master", "lei", "VARCHAR")
        self._add_column_if_missing(con, "company_master", "country_code", "VARCHAR")
        self._add_column_if_missing(con, "company_master", "region_code", "VARCHAR")
        self._add_column_if_missing(con, "company_master", "is_active", "BOOLEAN")
        self._add_column_if_missing(con, "company_master", "source_name", "VARCHAR")
        self._add_column_if_missing(con, "company_master", "created_at", "TIMESTAMP")

    def _evolve_instrument_master(self, con) -> None:
        self._add_column_if_missing(con, "instrument_master", "company_id", "VARCHAR")
        self._add_column_if_missing(con, "instrument_master", "instrument_type", "VARCHAR")
        self._add_column_if_missing(con, "instrument_master", "primary_exchange", "VARCHAR")
        self._add_column_if_missing(con, "instrument_master", "mic_code", "VARCHAR")
        self._add_column_if_missing(con, "instrument_master", "currency_code", "VARCHAR")
        self._add_column_if_missing(con, "instrument_master", "listing_date", "DATE")
        self._add_column_if_missing(con, "instrument_master", "delisting_date", "DATE")
        self._add_column_if_missing(con, "instrument_master", "source_name", "VARCHAR")
        self._add_column_if_missing(con, "instrument_master", "created_at", "TIMESTAMP")

    def _evolve_ticker_history(self, con) -> None:
        self._add_column_if_missing(con, "ticker_history", "company_id", "VARCHAR")
        self._add_column_if_missing(con, "ticker_history", "symbol", "VARCHAR")
        self._add_column_if_missing(con, "ticker_history", "ticker", "VARCHAR")
        self._add_column_if_missing(con, "ticker_history", "valid_from", "DATE")
        self._add_column_if_missing(con, "ticker_history", "valid_to", "DATE")
        self._add_column_if_missing(con, "ticker_history", "is_primary", "BOOLEAN")
        self._add_column_if_missing(con, "ticker_history", "source_name", "VARCHAR")
        self._add_column_if_missing(con, "ticker_history", "created_at", "TIMESTAMP")

    def _evolve_instrument_identifier_map(self, con) -> None:
        self._add_column_if_missing(con, "instrument_identifier_map", "valid_from", "DATE")
        self._add_column_if_missing(con, "instrument_identifier_map", "valid_to", "DATE")
        self._add_column_if_missing(con, "instrument_identifier_map", "is_primary", "BOOLEAN")
        self._add_column_if_missing(con, "instrument_identifier_map", "source_name", "VARCHAR")
        self._add_column_if_missing(con, "instrument_identifier_map", "created_at", "TIMESTAMP")

    def _evolve_universe_membership_history(self, con) -> None:
        self._add_column_if_missing(con, "universe_membership_history", "instrument_id", "VARCHAR")
        self._add_column_if_missing(con, "universe_membership_history", "company_id", "VARCHAR")
        self._add_column_if_missing(con, "universe_membership_history", "symbol", "VARCHAR")
        self._add_column_if_missing(con, "universe_membership_history", "universe_name", "VARCHAR")
        self._add_column_if_missing(con, "universe_membership_history", "effective_from", "DATE")
        self._add_column_if_missing(con, "universe_membership_history", "effective_to", "DATE")
        self._add_column_if_missing(con, "universe_membership_history", "membership_status", "VARCHAR")
        self._add_column_if_missing(con, "universe_membership_history", "reason", "VARCHAR")
        self._add_column_if_missing(con, "universe_membership_history", "source_name", "VARCHAR")
        self._add_column_if_missing(con, "universe_membership_history", "created_at", "TIMESTAMP")

    def _create_company_master_indexes(self, con) -> None:
        self._create_index_if_columns_exist(con, "company_master", "idx_company_master_company_id", "company_id")
        self._create_index_if_columns_exist(con, "company_master", "idx_company_master_cik", "cik")

    def _create_instrument_master_indexes(self, con) -> None:
        self._create_index_if_columns_exist(con, "instrument_master", "idx_instrument_master_instrument_id", "instrument_id")
        self._create_index_if_columns_exist(con, "instrument_master", "idx_instrument_master_company_id", "company_id")
        self._create_index_if_columns_exist(con, "instrument_master", "idx_instrument_master_symbol", "symbol")

    def _create_ticker_history_indexes(self, con) -> None:
        self._create_index_if_columns_exist(con, "ticker_history", "idx_ticker_history_instrument_id", "instrument_id")
        self._create_index_if_columns_exist(con, "ticker_history", "idx_ticker_history_ticker_validity", "ticker, valid_from, valid_to")
        self._create_index_if_columns_exist(con, "ticker_history", "idx_ticker_history_symbol_validity", "symbol, valid_from, valid_to")

    def _create_instrument_identifier_map_indexes(self, con) -> None:
        self._create_index_if_columns_exist(con, "instrument_identifier_map", "idx_instrument_identifier_map_instrument_id", "instrument_id")
        self._create_index_if_columns_exist(con, "instrument_identifier_map", "idx_instrument_identifier_map_type_value", "identifier_type, identifier_value")

    def _create_universe_membership_history_indexes(self, con) -> None:
        self._create_index_if_columns_exist(con, "universe_membership_history", "idx_universe_membership_history_instrument_universe", "instrument_id, universe_name, effective_from, effective_to")
        self._create_index_if_columns_exist(con, "universe_membership_history", "idx_universe_membership_history_symbol_universe", "symbol, universe_name, effective_from, effective_to")
        self._create_index_if_columns_exist(con, "universe_membership_history", "idx_universe_membership_history_status", "universe_name, membership_status")

    def _add_column_if_missing(self, con, table_name: str, column_name: str, column_type: str) -> None:
        existing = {row[1] for row in con.execute(f"PRAGMA table_info('{table_name}')").fetchall()}
        if column_name not in existing:
            con.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}")

    def _create_index_if_columns_exist(self, con, table_name: str, index_name: str, column_sql: str) -> None:
        existing = {row[1] for row in con.execute(f"PRAGMA table_info('{table_name}')").fetchall()}
        needed = [col.strip() for col in column_sql.split(",")]
        if all(col in existing for col in needed):
            con.execute(f"CREATE INDEX IF NOT EXISTS {index_name} ON {table_name}({column_sql})")


class MasterDataSchemaManager(MasterDataSchema):
    pass
