from __future__ import annotations

from stock_quant.infrastructure.db.unit_of_work import DuckDbUnitOfWork


class MasterDataSchemaManager:
    def __init__(self, uow: DuckDbUnitOfWork) -> None:
        self.uow = uow

    @property
    def con(self):
        if self.uow.connection is None:
            raise RuntimeError("active DB connection is required")
        return self.uow.connection

    def initialize(self) -> None:
        self._create_company_master()
        self._create_instrument_master()
        self._create_ticker_history()
        self._create_instrument_identifier_map()

    def _create_company_master(self) -> None:
        self.con.execute(
            """
            CREATE TABLE IF NOT EXISTS company_master (
                company_id VARCHAR,
                company_name VARCHAR,
                cik VARCHAR,
                issuer_name_normalized VARCHAR,
                country_code VARCHAR,
                created_at TIMESTAMP
            )
            """
        )

    def _create_instrument_master(self) -> None:
        self.con.execute(
            """
            CREATE TABLE IF NOT EXISTS instrument_master (
                instrument_id VARCHAR,
                company_id VARCHAR,
                symbol VARCHAR,
                exchange VARCHAR,
                security_type VARCHAR,
                share_class VARCHAR,
                is_active BOOLEAN,
                created_at TIMESTAMP
            )
            """
        )

    def _create_ticker_history(self) -> None:
        self.con.execute(
            """
            CREATE TABLE IF NOT EXISTS ticker_history (
                instrument_id VARCHAR,
                symbol VARCHAR,
                exchange VARCHAR,
                valid_from DATE,
                valid_to DATE,
                is_current BOOLEAN,
                created_at TIMESTAMP
            )
            """
        )

    def _create_instrument_identifier_map(self) -> None:
        self.con.execute(
            """
            CREATE TABLE IF NOT EXISTS instrument_identifier_map (
                instrument_id VARCHAR,
                company_id VARCHAR,
                identifier_type VARCHAR,
                identifier_value VARCHAR,
                is_primary BOOLEAN,
                created_at TIMESTAMP
            )
            """
        )
