from __future__ import annotations

from stock_quant.app.dto.pipeline_result import PipelineResult
from stock_quant.infrastructure.db.unit_of_work import DuckDbUnitOfWork
from stock_quant.pipelines.base_pipeline import BasePipeline
from stock_quant.shared.exceptions import PipelineError


class BuildFundamentalsPipeline(BasePipeline):
    pipeline_name = "build_fundamentals"

    def __init__(self, repository=None, uow: DuckDbUnitOfWork | None = None) -> None:
        if repository is not None:
            self.uow = repository.uow
        else:
            self.uow = uow

        if self.uow is None:
            raise ValueError("BuildFundamentalsPipeline requires repository or uow")

        self._metrics: dict[str, int] = {}
        self._rows_written = 0

    @property
    def con(self):
        if self.uow.connection is None:
            raise PipelineError("active DB connection is required")
        return self.uow.connection

    def extract(self):
        return None

    def transform(self, data):
        return None

    def validate(self, data) -> None:
        sec_filing_rows = int(self.con.execute("SELECT COUNT(*) FROM sec_filing").fetchone()[0])
        if sec_filing_rows == 0:
            raise PipelineError("no sec_filing rows available")

    def load(self, data) -> None:
        con = self.con

        sec_filing_rows = int(con.execute("SELECT COUNT(*) FROM sec_filing").fetchone()[0])
        sec_fact_rows = int(con.execute("SELECT COUNT(*) FROM sec_fact_normalized").fetchone()[0])

        con.execute("DELETE FROM fundamental_snapshot_quarterly")
        con.execute("DELETE FROM fundamental_snapshot_annual")
        con.execute("DELETE FROM fundamental_ttm")
        con.execute("DELETE FROM fundamental_features_daily")

        con.execute("DROP TABLE IF EXISTS tmp_fundamental_fact_base")
        con.execute(
            """
            CREATE TEMP TABLE tmp_fundamental_fact_base AS
            SELECT
                sf.filing_id,
                sf.company_id,
                sf.cik,
                sf.form_type,
                sf.filing_date,
                sf.accepted_at,
                sf.available_at,
                sfn.period_end_date,
                LOWER(TRIM(sfn.concept)) AS concept,
                CAST(sfn.value_numeric AS DOUBLE) AS value_numeric,
                COALESCE(sfn.source_name, 'sec') AS source_name
            FROM sec_fact_normalized sfn
            INNER JOIN sec_filing sf
              ON sfn.filing_id = sf.filing_id
            WHERE sfn.value_numeric IS NOT NULL
            """
        )

        con.execute("DROP TABLE IF EXISTS tmp_fundamental_snapshots_raw")
        con.execute(
            """
            CREATE TEMP TABLE tmp_fundamental_snapshots_raw AS
            SELECT
                company_id,
                cik,
                CASE
                    WHEN UPPER(form_type) LIKE '10-Q%' THEN 'QUARTERLY'
                    WHEN UPPER(form_type) LIKE '10-K%' THEN 'ANNUAL'
                    ELSE NULL
                END AS period_type,
                period_end_date,
                available_at,
                MAX(CASE
                    WHEN concept IN (
                        'revenue',
                        'salesrevenue',
                        'salesrevenuenet',
                        'revenues',
                        'revenuefromcontractwithcustomerexcludingassessedtax',
                        'salesrevenuegoodsnet'
                    ) THEN value_numeric
                END) AS revenue,
                MAX(CASE
                    WHEN concept IN (
                        'netincome',
                        'netincomeloss',
                        'profitloss'
                    ) THEN value_numeric
                END) AS net_income,
                MAX(CASE
                    WHEN concept IN ('assets') THEN value_numeric
                END) AS assets,
                MAX(CASE
                    WHEN concept IN (
                        'liabilities',
                        'liabilitiescurrent',
                        'liabilitiesnoncurrent'
                    ) THEN value_numeric
                END) AS liabilities,
                MAX(CASE
                    WHEN concept IN (
                        'stockholdersequity',
                        'stockholdersequityincludingportionattributablenoncontrollinginterest',
                        'equity'
                    ) THEN value_numeric
                END) AS equity,
                MAX(CASE
                    WHEN concept IN (
                        'netcashprovidedbyusedinoperatingactivities',
                        'netcashprovidedbyusedinoperatingactivitiescontinuingoperations',
                        'operatingcashflow'
                    ) THEN value_numeric
                END) AS operating_cash_flow,
                MAX(CASE
                    WHEN concept IN (
                        'commonstocksharesoutstanding',
                        'entitycommonstocksharesoutstanding',
                        'sharesoutstanding'
                    ) THEN value_numeric
                END) AS shares_outstanding,
                MAX(source_name) AS source_name
            FROM tmp_fundamental_fact_base
            WHERE period_end_date IS NOT NULL
            GROUP BY
                company_id,
                cik,
                CASE
                    WHEN UPPER(form_type) LIKE '10-Q%' THEN 'QUARTERLY'
                    WHEN UPPER(form_type) LIKE '10-K%' THEN 'ANNUAL'
                    ELSE NULL
                END,
                period_end_date,
                available_at
            HAVING period_type IS NOT NULL
            """
        )

        con.execute(
            """
            INSERT INTO fundamental_snapshot_quarterly (
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
                CURRENT_TIMESTAMP
            FROM tmp_fundamental_snapshots_raw
            WHERE period_type = 'QUARTERLY'
            """
        )

        quarterly_rows = int(
            con.execute("SELECT COUNT(*) FROM fundamental_snapshot_quarterly").fetchone()[0]
        )

        con.execute(
            """
            INSERT INTO fundamental_snapshot_annual (
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
                CURRENT_TIMESTAMP
            FROM tmp_fundamental_snapshots_raw
            WHERE period_type = 'ANNUAL'
            """
        )

        annual_rows = int(
            con.execute("SELECT COUNT(*) FROM fundamental_snapshot_annual").fetchone()[0]
        )

        con.execute("DROP TABLE IF EXISTS tmp_fundamental_ttm")
        con.execute(
            """
            CREATE TEMP TABLE tmp_fundamental_ttm AS
            WITH q AS (
                SELECT
                    company_id,
                    cik,
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
                    COUNT(revenue) OVER (
                        PARTITION BY company_id
                        ORDER BY period_end_date
                        ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
                    ) AS revenue_obs_4,
                    COUNT(net_income) OVER (
                        PARTITION BY company_id
                        ORDER BY period_end_date
                        ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
                    ) AS net_income_obs_4,
                    COUNT(operating_cash_flow) OVER (
                        PARTITION BY company_id
                        ORDER BY period_end_date
                        ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
                    ) AS ocf_obs_4,
                    SUM(revenue) OVER (
                        PARTITION BY company_id
                        ORDER BY period_end_date
                        ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
                    ) AS revenue_ttm,
                    SUM(net_income) OVER (
                        PARTITION BY company_id
                        ORDER BY period_end_date
                        ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
                    ) AS net_income_ttm,
                    SUM(operating_cash_flow) OVER (
                        PARTITION BY company_id
                        ORDER BY period_end_date
                        ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
                    ) AS operating_cash_flow_ttm
                FROM fundamental_snapshot_quarterly
            )
            SELECT
                company_id,
                cik,
                'TTM' AS period_type,
                period_end_date,
                available_at,
                CASE WHEN revenue_obs_4 = 4 THEN revenue_ttm ELSE NULL END AS revenue,
                CASE WHEN net_income_obs_4 = 4 THEN net_income_ttm ELSE NULL END AS net_income,
                assets,
                liabilities,
                equity,
                CASE WHEN ocf_obs_4 = 4 THEN operating_cash_flow_ttm ELSE NULL END AS operating_cash_flow,
                shares_outstanding,
                source_name
            FROM q
            WHERE revenue_obs_4 = 4
               OR net_income_obs_4 = 4
               OR ocf_obs_4 = 4
            """
        )

        con.execute(
            """
            INSERT INTO fundamental_ttm (
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
                CURRENT_TIMESTAMP
            FROM tmp_fundamental_ttm
            """
        )

        ttm_rows = int(
            con.execute("SELECT COUNT(*) FROM fundamental_ttm").fetchone()[0]
        )

        con.execute(
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
                period_end_date AS as_of_date,
                period_type,
                revenue,
                net_income,
                assets,
                liabilities,
                equity,
                operating_cash_flow,
                shares_outstanding,
                CASE
                    WHEN revenue IS NOT NULL AND revenue <> 0 AND net_income IS NOT NULL
                    THEN net_income / revenue
                    ELSE NULL
                END AS net_margin,
                CASE
                    WHEN equity IS NOT NULL AND equity <> 0 AND liabilities IS NOT NULL
                    THEN liabilities / equity
                    ELSE NULL
                END AS debt_to_equity,
                CASE
                    WHEN assets IS NOT NULL AND assets <> 0 AND net_income IS NOT NULL
                    THEN net_income / assets
                    ELSE NULL
                END AS return_on_assets,
                source_name,
                CURRENT_TIMESTAMP
            FROM (
                SELECT * FROM fundamental_snapshot_quarterly
                UNION ALL
                SELECT * FROM fundamental_snapshot_annual
                UNION ALL
                SELECT * FROM fundamental_ttm
            ) s
            """
        )

        feature_rows = int(
            con.execute("SELECT COUNT(*) FROM fundamental_features_daily").fetchone()[0]
        )

        self._rows_written = quarterly_rows + annual_rows + ttm_rows + feature_rows
        self._metrics = {
            "sec_filing_rows": sec_filing_rows,
            "sec_fact_rows": sec_fact_rows,
            "quarterly_rows": quarterly_rows,
            "annual_rows": annual_rows,
            "ttm_rows": ttm_rows,
            "fundamental_feature_rows": feature_rows,
            "written_quarterly": quarterly_rows,
            "written_annual": annual_rows,
            "written_ttm": ttm_rows,
            "written_features": feature_rows,
        }

    def finalize(self, result: PipelineResult) -> PipelineResult:
        result.rows_read = int(self._metrics.get("sec_fact_rows", 0))
        result.rows_written = self._rows_written
        result.metrics.update(self._metrics)
        return result
