from __future__ import annotations

from stock_quant.app.dto.pipeline_result import PipelineResult
from stock_quant.infrastructure.db.unit_of_work import DuckDbUnitOfWork
from stock_quant.pipelines.base_pipeline import BasePipeline
from stock_quant.shared.exceptions import PipelineError


class BuildFeatureEnginePipeline(BasePipeline):
    pipeline_name = "build_feature_engine"

    def __init__(self, repository=None, uow: DuckDbUnitOfWork | None = None) -> None:
        if repository is not None:
            self.uow = repository.uow
        else:
            self.uow = uow

        if self.uow is None:
            raise ValueError("BuildFeatureEnginePipeline requires repository or uow")

        self._technical_rows = 0
        self._research_rows = 0

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
        count = self.con.execute("SELECT COUNT(*) FROM price_bars_adjusted").fetchone()[0]
        if int(count) == 0:
            raise PipelineError("no rows available in price_bars_adjusted")

    def load(self, data) -> None:
        con = self.con

        con.execute("DELETE FROM technical_features_daily")
        con.execute("DELETE FROM research_features_daily")

        con.execute("DROP TABLE IF EXISTS tmp_price_base")
        con.execute(
            """
            CREATE TEMP TABLE tmp_price_base AS
            SELECT
                p.instrument_id,
                im.company_id,
                p.symbol,
                p.bar_date AS as_of_date,
                CAST(p.adj_close AS DOUBLE) AS adj_close
            FROM price_bars_adjusted p
            LEFT JOIN instrument_master im
              ON p.instrument_id = im.instrument_id
            ORDER BY p.instrument_id, p.bar_date
            """
        )

        con.execute("DROP TABLE IF EXISTS tmp_technical_features")
        con.execute(
            """
            CREATE TEMP TABLE tmp_technical_features AS
            WITH base AS (
                SELECT
                    instrument_id,
                    company_id,
                    symbol,
                    as_of_date,
                    adj_close,
                    AVG(adj_close) OVER (
                        PARTITION BY instrument_id
                        ORDER BY as_of_date
                        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                    ) AS sma_20,
                    adj_close - LAG(adj_close) OVER (
                        PARTITION BY instrument_id
                        ORDER BY as_of_date
                    ) AS delta
                FROM tmp_price_base
            ),
            gains_losses AS (
                SELECT
                    instrument_id,
                    company_id,
                    symbol,
                    as_of_date,
                    adj_close,
                    sma_20,
                    CASE WHEN delta > 0 THEN delta ELSE 0 END AS gain,
                    CASE WHEN delta < 0 THEN -delta ELSE 0 END AS loss
                FROM base
            ),
            rsi_roll AS (
                SELECT
                    instrument_id,
                    company_id,
                    symbol,
                    as_of_date,
                    adj_close,
                    sma_20,
                    AVG(gain) OVER (
                        PARTITION BY instrument_id
                        ORDER BY as_of_date
                        ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
                    ) AS avg_gain_14,
                    AVG(loss) OVER (
                        PARTITION BY instrument_id
                        ORDER BY as_of_date
                        ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
                    ) AS avg_loss_14
                FROM gains_losses
            )
            SELECT
                instrument_id,
                company_id,
                symbol,
                as_of_date,
                CASE
                    WHEN sma_20 IS NULL OR sma_20 = 0 THEN NULL
                    ELSE (adj_close / sma_20) - 1
                END AS close_to_sma_20,
                CASE
                    WHEN avg_loss_14 IS NULL OR avg_gain_14 IS NULL THEN NULL
                    WHEN avg_loss_14 = 0 AND avg_gain_14 = 0 THEN 50.0
                    WHEN avg_loss_14 = 0 THEN 100.0
                    ELSE 100 - (100 / (1 + (avg_gain_14 / avg_loss_14)))
                END AS rsi_14
            FROM rsi_roll
            """
        )

        con.execute(
            """
            INSERT INTO technical_features_daily (
                instrument_id,
                company_id,
                symbol,
                as_of_date,
                close_to_sma_20,
                rsi_14,
                source_name,
                created_at
            )
            SELECT
                instrument_id,
                company_id,
                symbol,
                as_of_date,
                close_to_sma_20,
                rsi_14,
                'prices' AS source_name,
                CURRENT_TIMESTAMP AS created_at
            FROM tmp_technical_features
            """
        )

        self._technical_rows = int(
            con.execute("SELECT COUNT(*) FROM technical_features_daily").fetchone()[0]
        )

        con.execute(
            """
            INSERT INTO research_features_daily (
                instrument_id,
                company_id,
                symbol,
                as_of_date,
                close_to_sma_20,
                rsi_14,
                revenue,
                net_income,
                net_margin,
                debt_to_equity,
                return_on_assets,
                short_interest,
                days_to_cover,
                short_volume_ratio,
                article_count_1d,
                unique_cluster_count_1d,
                avg_link_confidence,
                source_name,
                created_at
            )
            SELECT
                t.instrument_id,
                t.company_id,
                t.symbol,
                t.as_of_date,
                t.close_to_sma_20,
                t.rsi_14,
                f.revenue,
                f.net_income,
                f.net_margin,
                f.debt_to_equity,
                f.return_on_assets,
                s.short_interest,
                s.days_to_cover,
                s.short_volume_ratio,
                n.article_count_1d,
                n.unique_cluster_count_1d,
                n.avg_link_confidence,
                'research' AS source_name,
                CURRENT_TIMESTAMP AS created_at
            FROM technical_features_daily t
            LEFT JOIN fundamental_features_daily f
              ON t.company_id = f.company_id
             AND t.as_of_date = f.as_of_date
            LEFT JOIN short_features_daily s
              ON t.instrument_id = s.instrument_id
             AND t.as_of_date = s.as_of_date
            LEFT JOIN news_features_daily n
              ON t.instrument_id = n.instrument_id
             AND t.as_of_date = n.as_of_date
            """
        )

        self._research_rows = int(
            con.execute("SELECT COUNT(*) FROM research_features_daily").fetchone()[0]
        )

    def finalize(self, result: PipelineResult) -> PipelineResult:
        result.rows_read = self._technical_rows
        result.rows_written = self._technical_rows + self._research_rows
        result.metrics["technical_feature_rows"] = self._technical_rows
        result.metrics["research_feature_rows"] = self._research_rows
        result.metrics["written_technical"] = self._technical_rows
        result.metrics["written_research"] = self._research_rows
        return result
