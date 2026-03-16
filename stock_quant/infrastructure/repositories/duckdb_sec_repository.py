from __future__ import annotations

# =============================================================================
# DuckDB SEC repository
# -----------------------------------------------------------------------------
# Repository d'accès aux tables SEC dans DuckDB.
#
# Ajustements importants dans cette version:
# - alignement strict avec le schéma SEC existant + migré
# - conservation de available_at dans sec_fact_normalized
# - méthodes de lecture utiles pour les services de build
# - helpers PIT pour futures étapes de recherche quant
# =============================================================================

from datetime import datetime
from typing import Any, Iterable, Sequence

from stock_quant.domain.entities.sec_filing import (
    SecFactNormalized,
    SecFiling,
    SecFilingDocument,
    SecFilingRawDocument,
    SecFilingRawIndexEntry,
    SecXbrlFactRaw,
)


class DuckDbSecRepository:
    def __init__(self, con: Any) -> None:
        self.con = con

    def _executemany(self, sql: str, rows: Sequence[tuple[Any, ...]]) -> int:
        if not rows:
            return 0
        self.con.executemany(sql, rows)
        return len(rows)

    @staticmethod
    def _safe_datetime(value: datetime | None) -> datetime | None:
        return value

    # =========================================================================
    # RAW INDEX
    # =========================================================================
    def insert_sec_filing_raw_index_entries(
        self,
        entries: Iterable[SecFilingRawIndexEntry],
    ) -> int:
        rows: list[tuple[Any, ...]] = []
        for entry in entries:
            rows.append(
                (
                    entry.cik,
                    entry.company_name,
                    entry.form_type,
                    entry.filing_date,
                    self._safe_datetime(entry.accepted_at),
                    entry.accession_number,
                    entry.primary_document,
                    entry.filing_url,
                    entry.source_name,
                    entry.source_file,
                    self._safe_datetime(entry.ingested_at),
                )
            )

        sql = """
            INSERT INTO sec_filing_raw_index (
                cik,
                company_name,
                form_type,
                filing_date,
                accepted_at,
                accession_number,
                primary_document,
                filing_url,
                source_name,
                source_file,
                ingested_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        return self._executemany(sql, rows)

    def load_sec_filing_raw_index_rows(self) -> list[dict[str, Any]]:
        rows = self.con.execute(
            """
            SELECT
                cik,
                company_name,
                form_type,
                filing_date,
                accepted_at,
                accession_number,
                primary_document,
                filing_url,
                source_name,
                source_file,
                ingested_at
            FROM sec_filing_raw_index
            ORDER BY COALESCE(accepted_at, CAST(filing_date AS TIMESTAMP)) DESC NULLS LAST,
                     accession_number
            """
        ).fetchall()

        return [
            {
                "cik": row[0],
                "company_name": row[1],
                "form_type": row[2],
                "filing_date": row[3],
                "accepted_at": row[4],
                "accession_number": row[5],
                "primary_document": row[6],
                "filing_url": row[7],
                "source_name": row[8],
                "source_file": row[9],
                "ingested_at": row[10],
            }
            for row in rows
        ]

    # =========================================================================
    # RAW DOCUMENTS
    # =========================================================================
    def insert_sec_filing_raw_documents(
        self,
        documents: Iterable[SecFilingRawDocument],
    ) -> int:
        rows: list[tuple[Any, ...]] = []
        for doc in documents:
            rows.append(
                (
                    doc.accession_number,
                    doc.document_type,
                    doc.document_url,
                    doc.document_path,
                    doc.document_text,
                    doc.source_name,
                    doc.source_file,
                    self._safe_datetime(doc.ingested_at),
                )
            )

        sql = """
            INSERT INTO sec_filing_raw_document (
                accession_number,
                document_type,
                document_url,
                document_path,
                document_text,
                source_name,
                source_file,
                ingested_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """
        return self._executemany(sql, rows)

    # =========================================================================
    # RAW XBRL FACTS
    # =========================================================================
    def insert_sec_xbrl_fact_raw(
        self,
        facts: Iterable[SecXbrlFactRaw],
    ) -> int:
        rows: list[tuple[Any, ...]] = []
        for fact in facts:
            rows.append(
                (
                    fact.accession_number,
                    fact.cik,
                    fact.taxonomy,
                    fact.concept,
                    fact.unit,
                    fact.period_end_date,
                    fact.period_start_date,
                    fact.fiscal_year,
                    fact.fiscal_period,
                    fact.frame,
                    fact.value_text,
                    fact.value_numeric,
                    fact.source_name,
                    fact.source_file,
                    self._safe_datetime(fact.ingested_at),
                )
            )

        sql = """
            INSERT INTO sec_xbrl_fact_raw (
                accession_number,
                cik,
                taxonomy,
                concept,
                unit,
                period_end_date,
                period_start_date,
                fiscal_year,
                fiscal_period,
                frame,
                value_text,
                value_numeric,
                source_name,
                source_file,
                ingested_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        return self._executemany(sql, rows)

    def load_sec_xbrl_fact_raw_rows(self) -> list[dict[str, Any]]:
        rows = self.con.execute(
            """
            SELECT
                accession_number,
                cik,
                taxonomy,
                concept,
                unit,
                period_end_date,
                period_start_date,
                fiscal_year,
                fiscal_period,
                frame,
                value_text,
                value_numeric,
                source_name,
                source_file,
                ingested_at
            FROM sec_xbrl_fact_raw
            ORDER BY accession_number, concept, period_end_date
            """
        ).fetchall()

        return [
            {
                "accession_number": row[0],
                "cik": row[1],
                "taxonomy": row[2],
                "concept": row[3],
                "unit": row[4],
                "period_end_date": row[5],
                "period_start_date": row[6],
                "fiscal_year": row[7],
                "fiscal_period": row[8],
                "frame": row[9],
                "value_text": row[10],
                "value_numeric": row[11],
                "source_name": row[12],
                "source_file": row[13],
                "ingested_at": row[14],
            }
            for row in rows
        ]

    # =========================================================================
    # NORMALIZED FILINGS
    # =========================================================================
    def upsert_sec_filings(self, filings: Iterable[SecFiling]) -> int:
        rows: list[tuple[Any, ...]] = []
        for filing in filings:
            rows.append(
                (
                    filing.filing_id,
                    filing.company_id,
                    filing.cik,
                    filing.form_type,
                    filing.filing_date,
                    self._safe_datetime(filing.accepted_at),
                    filing.accession_number,
                    filing.filing_url,
                    filing.primary_document,
                    self._safe_datetime(filing.available_at),
                    filing.source_name,
                    self._safe_datetime(filing.created_at),
                )
            )

        if not rows:
            return 0

        self.con.execute(
            """
            CREATE TEMP TABLE tmp_sec_filing_upsert (
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
        try:
            self.con.executemany(
                """
                INSERT INTO tmp_sec_filing_upsert (
                    filing_id,
                    company_id,
                    cik,
                    form_type,
                    filing_date,
                    accepted_at,
                    accession_number,
                    filing_url,
                    primary_document,
                    available_at,
                    source_name,
                    created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )

            self.con.execute(
                """
                DELETE FROM sec_filing AS target
                USING tmp_sec_filing_upsert AS source
                WHERE target.filing_id = source.filing_id
                """
            )

            self.con.execute(
                """
                INSERT INTO sec_filing (
                    filing_id,
                    company_id,
                    cik,
                    form_type,
                    filing_date,
                    accepted_at,
                    accession_number,
                    filing_url,
                    primary_document,
                    available_at,
                    source_name,
                    created_at
                )
                SELECT
                    filing_id,
                    company_id,
                    cik,
                    form_type,
                    filing_date,
                    accepted_at,
                    accession_number,
                    filing_url,
                    primary_document,
                    available_at,
                    source_name,
                    created_at
                FROM (
                    SELECT
                        *,
                        ROW_NUMBER() OVER (
                            PARTITION BY filing_id
                            ORDER BY available_at DESC NULLS LAST,
                                     accepted_at DESC NULLS LAST,
                                     created_at DESC NULLS LAST
                        ) AS rn
                    FROM tmp_sec_filing_upsert
                ) x
                WHERE rn = 1
                """
            )
        finally:
            self.con.execute("DROP TABLE IF EXISTS tmp_sec_filing_upsert")

        return len(rows)

    def load_sec_filing_rows(self) -> list[dict[str, Any]]:
        rows = self.con.execute(
            """
            SELECT
                filing_id,
                company_id,
                cik,
                form_type,
                filing_date,
                accepted_at,
                accession_number,
                filing_url,
                primary_document,
                available_at,
                source_name,
                created_at
            FROM sec_filing
            ORDER BY cik, available_at, filing_date, accession_number
            """
        ).fetchall()

        return [
            {
                "filing_id": row[0],
                "company_id": row[1],
                "cik": row[2],
                "form_type": row[3],
                "filing_date": row[4],
                "accepted_at": row[5],
                "accession_number": row[6],
                "filing_url": row[7],
                "primary_document": row[8],
                "available_at": row[9],
                "source_name": row[10],
                "created_at": row[11],
            }
            for row in rows
        ]

    # =========================================================================
    # NORMALIZED DOCUMENTS
    # =========================================================================
    def insert_sec_filing_documents(
        self,
        documents: Iterable[SecFilingDocument],
    ) -> int:
        rows: list[tuple[Any, ...]] = []
        for doc in documents:
            rows.append(
                (
                    doc.filing_id,
                    doc.document_type,
                    doc.document_url,
                    doc.document_path,
                    doc.document_text,
                    self._safe_datetime(doc.created_at),
                )
            )

        sql = """
            INSERT INTO sec_filing_document (
                filing_id,
                document_type,
                document_url,
                document_path,
                document_text,
                created_at
            )
            VALUES (?, ?, ?, ?, ?, ?)
        """
        return self._executemany(sql, rows)

    # =========================================================================
    # NORMALIZED FACTS
    # =========================================================================
    def insert_sec_fact_normalized(
        self,
        facts: Iterable[SecFactNormalized],
    ) -> int:
        rows: list[tuple[Any, ...]] = []
        for fact in facts:
            rows.append(
                (
                    fact.filing_id,
                    fact.company_id,
                    fact.cik,
                    fact.taxonomy,
                    fact.concept,
                    fact.period_end_date,
                    fact.unit,
                    fact.value_text,
                    fact.value_numeric,
                    self._safe_datetime(fact.available_at),
                    fact.source_name,
                    self._safe_datetime(fact.created_at),
                )
            )

        sql = """
            INSERT INTO sec_fact_normalized (
                filing_id,
                company_id,
                cik,
                taxonomy,
                concept,
                period_end_date,
                unit,
                value_text,
                value_numeric,
                available_at,
                source_name,
                created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        return self._executemany(sql, rows)

    def load_sec_fact_normalized_rows(self) -> list[dict[str, Any]]:
        rows = self.con.execute(
            """
            SELECT
                filing_id,
                company_id,
                cik,
                taxonomy,
                concept,
                period_end_date,
                unit,
                value_text,
                value_numeric,
                available_at,
                source_name,
                created_at
            FROM sec_fact_normalized
            ORDER BY cik, concept, period_end_date, available_at
            """
        ).fetchall()

        return [
            {
                "filing_id": row[0],
                "company_id": row[1],
                "cik": row[2],
                "taxonomy": row[3],
                "concept": row[4],
                "period_end_date": row[5],
                "unit": row[6],
                "value_text": row[7],
                "value_numeric": row[8],
                "available_at": row[9],
                "source_name": row[10],
                "created_at": row[11],
            }
            for row in rows
        ]

    # =========================================================================
    # PIT helper
    # =========================================================================
    def load_latest_available_filing_for_company_and_form(
        self,
        company_id: str,
        form_type: str,
        as_of: datetime,
    ) -> dict[str, Any] | None:
        row = self.con.execute(
            """
            SELECT
                filing_id,
                company_id,
                cik,
                form_type,
                filing_date,
                accepted_at,
                accession_number,
                filing_url,
                primary_document,
                available_at,
                source_name,
                created_at
            FROM sec_filing
            WHERE company_id = ?
              AND form_type = ?
              AND available_at IS NOT NULL
              AND available_at <= ?
            ORDER BY available_at DESC,
                     accepted_at DESC NULLS LAST,
                     filing_date DESC NULLS LAST,
                     accession_number DESC NULLS LAST
            LIMIT 1
            """,
            [company_id, form_type, as_of],
        ).fetchone()

        if row is None:
            return None

        return {
            "filing_id": row[0],
            "company_id": row[1],
            "cik": row[2],
            "form_type": row[3],
            "filing_date": row[4],
            "accepted_at": row[5],
            "accession_number": row[6],
            "filing_url": row[7],
            "primary_document": row[8],
            "available_at": row[9],
            "source_name": row[10],
            "created_at": row[11],
        }
