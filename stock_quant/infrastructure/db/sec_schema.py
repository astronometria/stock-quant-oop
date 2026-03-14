from __future__ import annotations

from stock_quant.infrastructure.db.unit_of_work import DuckDbUnitOfWork


class SecSchemaManager:
    def __init__(self, uow: DuckDbUnitOfWork) -> None:
        self.uow = uow

    @property
    def con(self):
        if self.uow.connection is None:
            raise RuntimeError("active DB connection is required")
        return self.uow.connection

    def initialize(self) -> None:
        self._create_sec_filing_raw_index()
        self._create_sec_filing_raw_document()
        self._create_sec_xbrl_fact_raw()
        self._create_sec_filing()
        self._create_sec_filing_document()
        self._create_sec_fact_normalized()

    def _create_sec_filing_raw_index(self) -> None:
        self.con.execute(
            """
            CREATE TABLE IF NOT EXISTS sec_filing_raw_index (
                cik VARCHAR,
                company_name VARCHAR,
                form_type VARCHAR,
                filing_date DATE,
                accepted_at TIMESTAMP,
                accession_number VARCHAR,
                primary_document VARCHAR,
                filing_url VARCHAR,
                source_name VARCHAR,
                ingested_at TIMESTAMP
            )
            """
        )

    def _create_sec_filing_raw_document(self) -> None:
        self.con.execute(
            """
            CREATE TABLE IF NOT EXISTS sec_filing_raw_document (
                accession_number VARCHAR,
                document_type VARCHAR,
                document_url VARCHAR,
                document_text VARCHAR,
                source_name VARCHAR,
                ingested_at TIMESTAMP
            )
            """
        )

    def _create_sec_xbrl_fact_raw(self) -> None:
        self.con.execute(
            """
            CREATE TABLE IF NOT EXISTS sec_xbrl_fact_raw (
                accession_number VARCHAR,
                taxonomy VARCHAR,
                concept VARCHAR,
                period_end_date DATE,
                unit VARCHAR,
                value_text VARCHAR,
                value_numeric DOUBLE,
                source_name VARCHAR,
                ingested_at TIMESTAMP
            )
            """
        )

    def _create_sec_filing(self) -> None:
        self.con.execute(
            """
            CREATE TABLE IF NOT EXISTS sec_filing (
                filing_id VARCHAR,
                company_id VARCHAR,
                cik VARCHAR,
                form_type VARCHAR,
                filing_date DATE,
                accepted_at TIMESTAMP,
                accession_number VARCHAR,
                filing_url VARCHAR,
                primary_document VARCHAR,
                available_at TIMESTAMP,
                source_name VARCHAR,
                created_at TIMESTAMP
            )
            """
        )

    def _create_sec_filing_document(self) -> None:
        self.con.execute(
            """
            CREATE TABLE IF NOT EXISTS sec_filing_document (
                filing_id VARCHAR,
                document_type VARCHAR,
                document_url VARCHAR,
                document_text VARCHAR,
                created_at TIMESTAMP
            )
            """
        )

    def _create_sec_fact_normalized(self) -> None:
        self.con.execute(
            """
            CREATE TABLE IF NOT EXISTS sec_fact_normalized (
                filing_id VARCHAR,
                company_id VARCHAR,
                cik VARCHAR,
                taxonomy VARCHAR,
                concept VARCHAR,
                period_end_date DATE,
                unit VARCHAR,
                value_text VARCHAR,
                value_numeric DOUBLE,
                source_name VARCHAR,
                created_at TIMESTAMP
            )
            """
        )
