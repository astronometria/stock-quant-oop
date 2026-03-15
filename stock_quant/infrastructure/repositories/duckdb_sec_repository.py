from __future__ import annotations

from datetime import datetime
from typing import Any

from stock_quant.domain.entities.sec_filing import SecFiling
from stock_quant.infrastructure.db.unit_of_work import DuckDbUnitOfWork
from stock_quant.shared.exceptions import RepositoryError


class DuckDbSecRepository:
    def __init__(self, uow: DuckDbUnitOfWork) -> None:
        self.uow = uow

    @property
    def con(self):
        if self.uow.connection is None:
            raise RepositoryError("active DB connection is required")
        return self.uow.connection

    def replace_sec_filing_raw_index(self, rows: list[dict[str, Any]]) -> int:
        return self.upsert_sec_filing_raw_index(rows)

    def upsert_sec_filing_raw_index(self, rows: list[dict[str, Any]]) -> int:
        try:
            if not rows:
                return 0

            payload = [
                (
                    str(row.get("cik")).strip().zfill(10) if row.get("cik") is not None else None,
                    row.get("company_name"),
                    row.get("form_type"),
                    row.get("filing_date"),
                    row.get("accepted_at"),
                    str(row.get("accession_number")).strip() if row.get("accession_number") is not None else None,
                    row.get("primary_document"),
                    row.get("filing_url"),
                    row.get("source_name", "sec"),
                    row.get("ingested_at", datetime.utcnow()),
                )
                for row in rows
                if row.get("accession_number")
            ]
            if not payload:
                return 0

            self.con.execute(
                """
                CREATE TEMP TABLE tmp_sec_filing_raw_index_stage (
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
            try:
                self.con.executemany(
                    """
                    INSERT INTO tmp_sec_filing_raw_index_stage (
                        cik,
                        company_name,
                        form_type,
                        filing_date,
                        accepted_at,
                        accession_number,
                        primary_document,
                        filing_url,
                        source_name,
                        ingested_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    payload,
                )

                self.con.execute(
                    """
                    DELETE FROM sec_filing_raw_index AS target
                    USING tmp_sec_filing_raw_index_stage AS stage
                    WHERE target.accession_number = stage.accession_number
                    """
                )

                self.con.execute(
                    """
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
                        ingested_at
                    )
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
                        ingested_at
                    FROM (
                        SELECT
                            *,
                            ROW_NUMBER() OVER (
                                PARTITION BY accession_number
                                ORDER BY accepted_at DESC NULLS LAST, ingested_at DESC NULLS LAST
                            ) AS rn
                        FROM tmp_sec_filing_raw_index_stage
                    ) x
                    WHERE rn = 1
                    """
                )
            finally:
                self.con.execute("DROP TABLE IF EXISTS tmp_sec_filing_raw_index_stage")

            return len(payload)
        except Exception as exc:
            raise RepositoryError(f"failed to upsert sec_filing_raw_index: {exc}") from exc

    def load_sec_filing_raw_index_rows(self) -> list[dict[str, Any]]:
        try:
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
                    filing_url
                FROM sec_filing_raw_index
                ORDER BY filing_date, cik, accession_number
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
                }
                for row in rows
            ]
        except Exception as exc:
            raise RepositoryError(f"failed to load sec_filing_raw_index rows: {exc}") from exc

    def load_cik_company_map(self) -> dict[str, str]:
        try:
            rows = self.con.execute(
                """
                SELECT cik, company_id
                FROM company_master
                WHERE cik IS NOT NULL
                ORDER BY cik
                """
            ).fetchall()
            return {str(row[0]).strip().zfill(10): row[1] for row in rows}
        except Exception as exc:
            raise RepositoryError(f"failed to load cik company map: {exc}") from exc

    def replace_sec_filing(self, rows: list[SecFiling]) -> int:
        return self.upsert_sec_filing(rows)

    def upsert_sec_filing(self, rows: list[SecFiling]) -> int:
        try:
            if not rows:
                return 0

            payload = [
                (
                    row.filing_id,
                    row.company_id,
                    row.cik,
                    row.form_type,
                    row.filing_date,
                    row.accepted_at,
                    row.accession_number,
                    row.filing_url,
                    row.primary_document,
                    row.available_at,
                    row.source_name,
                    row.created_at or datetime.utcnow(),
                )
                for row in rows
                if row.filing_id and row.accession_number
            ]
            if not payload:
                return 0

            self.con.execute(
                """
                CREATE TEMP TABLE tmp_sec_filing_stage (
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
                    INSERT INTO tmp_sec_filing_stage (
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
                    payload,
                )

                self.con.execute(
                    """
                    DELETE FROM sec_filing AS target
                    USING tmp_sec_filing_stage AS stage
                    WHERE target.filing_id = stage.filing_id
                       OR target.accession_number = stage.accession_number
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
                                ORDER BY accepted_at DESC NULLS LAST, available_at DESC NULLS LAST, created_at DESC NULLS LAST
                            ) AS rn
                        FROM tmp_sec_filing_stage
                    ) x
                    WHERE rn = 1
                    """
                )
            finally:
                self.con.execute("DROP TABLE IF EXISTS tmp_sec_filing_stage")

            return len(payload)
        except Exception as exc:
            raise RepositoryError(f"failed to upsert sec_filing: {exc}") from exc
