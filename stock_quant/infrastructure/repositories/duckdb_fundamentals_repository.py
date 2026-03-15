from __future__ import annotations

from datetime import datetime
from typing import Any

from stock_quant.domain.entities.fundamental import FundamentalFeatureDaily, FundamentalSnapshot
from stock_quant.infrastructure.db.unit_of_work import DuckDbUnitOfWork
from stock_quant.shared.exceptions import RepositoryError


class DuckDbFundamentalsRepository:
    def __init__(self, uow: DuckDbUnitOfWork) -> None:
        self.uow = uow

    @property
    def con(self):
        if self.uow.connection is None:
            raise RepositoryError("active DB connection is required")
        return self.uow.connection

    def load_sec_filing_rows(self) -> list[dict[str, Any]]:
        try:
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
                    available_at
                FROM sec_filing
                ORDER BY cik, filing_date
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
                    "available_at": row[7],
                }
                for row in rows
            ]
        except Exception as exc:
            raise RepositoryError(f"failed to load sec_filing rows: {exc}") from exc

    def load_sec_fact_normalized_rows(self) -> list[dict[str, Any]]:
        try:
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
                    source_name
                FROM sec_fact_normalized
                ORDER BY cik, period_end_date, concept
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
                    "source_name": row[9],
                }
                for row in rows
            ]
        except Exception as exc:
            raise RepositoryError(f"failed to load sec_fact_normalized rows: {exc}") from exc

    def replace_fundamental_snapshot_quarterly(self, rows: list[FundamentalSnapshot]) -> int:
        return self.upsert_fundamental_snapshot_quarterly(rows)

    def replace_fundamental_snapshot_annual(self, rows: list[FundamentalSnapshot]) -> int:
        return self.upsert_fundamental_snapshot_annual(rows)

    def replace_fundamental_ttm(self, rows: list[FundamentalSnapshot]) -> int:
        return self.upsert_fundamental_ttm(rows)

    def replace_fundamental_features_daily(self, rows: list[FundamentalFeatureDaily]) -> int:
        return self.upsert_fundamental_features_daily(rows)

    def upsert_fundamental_snapshot_quarterly(self, rows: list[FundamentalSnapshot]) -> int:
        return self._upsert_snapshot_table("fundamental_snapshot_quarterly", rows)

    def upsert_fundamental_snapshot_annual(self, rows: list[FundamentalSnapshot]) -> int:
        return self._upsert_snapshot_table("fundamental_snapshot_annual", rows)

    def upsert_fundamental_ttm(self, rows: list[FundamentalSnapshot]) -> int:
        return self._upsert_snapshot_table("fundamental_ttm", rows)

    def upsert_fundamental_features_daily(self, rows: list[FundamentalFeatureDaily]) -> int:
        try:
            if not rows:
                return 0

            payload = [
                (
                    row.company_id,
                    row.as_of_date,
                    row.period_type,
                    row.revenue,
                    row.net_income,
                    row.assets,
                    row.liabilities,
                    row.equity,
                    row.operating_cash_flow,
                    row.shares_outstanding,
                    row.net_margin,
                    row.debt_to_equity,
                    row.return_on_assets,
                    row.source_name,
                    row.created_at or datetime.utcnow(),
                )
                for row in rows
                if row.company_id and row.as_of_date is not None
            ]
            if not payload:
                return 0

            self.con.execute(
                """
                CREATE TEMP TABLE tmp_fundamental_features_daily_stage (
                    company_id VARCHAR,
                    as_of_date DATE,
                    period_type VARCHAR,
                    revenue DOUBLE,
                    net_income DOUBLE,
                    assets DOUBLE,
                    liabilities DOUBLE,
                    equity DOUBLE,
                    operating_cash_flow DOUBLE,
                    shares_outstanding DOUBLE,
                    net_margin DOUBLE,
                    debt_to_equity DOUBLE,
                    return_on_assets DOUBLE,
                    source_name VARCHAR,
                    created_at TIMESTAMP
                )
                """
            )
            try:
                self.con.executemany(
                    """
                    INSERT INTO tmp_fundamental_features_daily_stage (
                        company_id,
                        as_of_date,
                        period_type,
                        revenue,
                        net_income,
                        assets,
                        liabilities,
                        equity,
                        operating_cash_flow,
                        shares_outstanding,
                        net_margin,
                        debt_to_equity,
                        return_on_assets,
                        source_name,
                        created_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    payload,
                )

                self.con.execute(
                    """
                    DELETE FROM fundamental_features_daily AS target
                    USING tmp_fundamental_features_daily_stage AS stage
                    WHERE target.company_id = stage.company_id
                      AND target.as_of_date = stage.as_of_date
                    """
                )

                self.con.execute(
                    """
                    INSERT INTO fundamental_features_daily (
                        company_id,
                        as_of_date,
                        period_type,
                        revenue,
                        net_income,
                        assets,
                        liabilities,
                        equity,
                        operating_cash_flow,
                        shares_outstanding,
                        net_margin,
                        debt_to_equity,
                        return_on_assets,
                        source_name,
                        created_at
                    )
                    SELECT
                        company_id,
                        as_of_date,
                        period_type,
                        revenue,
                        net_income,
                        assets,
                        liabilities,
                        equity,
                        operating_cash_flow,
                        shares_outstanding,
                        net_margin,
                        debt_to_equity,
                        return_on_assets,
                        source_name,
                        created_at
                    FROM (
                        SELECT
                            *,
                            ROW_NUMBER() OVER (
                                PARTITION BY company_id, as_of_date
                                ORDER BY created_at DESC NULLS LAST
                            ) AS rn
                        FROM tmp_fundamental_features_daily_stage
                    ) x
                    WHERE rn = 1
                    """
                )
            finally:
                self.con.execute("DROP TABLE IF EXISTS tmp_fundamental_features_daily_stage")

            return len(payload)
        except Exception as exc:
            raise RepositoryError(f"failed to upsert fundamental_features_daily: {exc}") from exc

    def _upsert_snapshot_table(self, table_name: str, rows: list[FundamentalSnapshot]) -> int:
        try:
            if not rows:
                return 0

            payload = [
                (
                    row.company_id,
                    row.cik,
                    row.period_type,
                    row.period_end_date,
                    row.available_at,
                    row.revenue,
                    row.net_income,
                    row.assets,
                    row.liabilities,
                    row.equity,
                    row.operating_cash_flow,
                    row.shares_outstanding,
                    row.source_name,
                    row.created_at or datetime.utcnow(),
                )
                for row in rows
                if row.company_id and row.period_end_date is not None
            ]
            if not payload:
                return 0

            stage_table = f"tmp_{table_name}_stage"
            self.con.execute(
                f"""
                CREATE TEMP TABLE {stage_table} (
                    company_id VARCHAR,
                    cik VARCHAR,
                    period_type VARCHAR,
                    period_end_date DATE,
                    available_at TIMESTAMP,
                    revenue DOUBLE,
                    net_income DOUBLE,
                    assets DOUBLE,
                    liabilities DOUBLE,
                    equity DOUBLE,
                    operating_cash_flow DOUBLE,
                    shares_outstanding DOUBLE,
                    source_name VARCHAR,
                    created_at TIMESTAMP
                )
                """
            )
            try:
                self.con.executemany(
                    f"""
                    INSERT INTO {stage_table} (
                        company_id,
                        cik,
                        period_type,
                        period_end_date,
                        available_at,
                        revenue,
                        net_income,
                        assets,
                        liabilities,
                        equity,
                        operating_cash_flow,
                        shares_outstanding,
                        source_name,
                        created_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    payload,
                )

                self.con.execute(
                    f"""
                    DELETE FROM {table_name} AS target
                    USING {stage_table} AS stage
                    WHERE target.company_id = stage.company_id
                      AND target.period_end_date = stage.period_end_date
                    """
                )

                self.con.execute(
                    f"""
                    INSERT INTO {table_name} (
                        company_id,
                        cik,
                        period_type,
                        period_end_date,
                        available_at,
                        revenue,
                        net_income,
                        assets,
                        liabilities,
                        equity,
                        operating_cash_flow,
                        shares_outstanding,
                        source_name,
                        created_at
                    )
                    SELECT
                        company_id,
                        cik,
                        period_type,
                        period_end_date,
                        available_at,
                        revenue,
                        net_income,
                        assets,
                        liabilities,
                        equity,
                        operating_cash_flow,
                        shares_outstanding,
                        source_name,
                        created_at
                    FROM (
                        SELECT
                            *,
                            ROW_NUMBER() OVER (
                                PARTITION BY company_id, period_end_date
                                ORDER BY available_at DESC NULLS LAST, created_at DESC NULLS LAST
                            ) AS rn
                        FROM {stage_table}
                    ) x
                    WHERE rn = 1
                    """
                )
            finally:
                self.con.execute(f"DROP TABLE IF EXISTS {stage_table}")

            return len(payload)
        except Exception as exc:
            raise RepositoryError(f"failed to upsert {table_name}: {exc}") from exc
