from __future__ import annotations

from datetime import datetime

from stock_quant.app.dto.pipeline_result import PipelineResult
from stock_quant.app.services.dataset_version_service import DatasetVersionService
from stock_quant.infrastructure.db.unit_of_work import DuckDbUnitOfWork
from stock_quant.pipelines.base_pipeline import BasePipeline
from stock_quant.shared.exceptions import PipelineError


class BuildDatasetBuilderPipeline(BasePipeline):
    pipeline_name = "build_dataset_builder"

    def __init__(
        self,
        repository=None,
        uow: DuckDbUnitOfWork | None = None,
        dataset_name: str = "research_dataset_v1",
        dataset_version: str = "v1",
    ) -> None:
        if repository is not None:
            self.uow = repository.uow
            self.repository = repository
        else:
            self.uow = uow
            self.repository = None

        if self.uow is None:
            raise ValueError("BuildDatasetBuilderPipeline requires repository or uow")

        self.dataset_name = dataset_name
        self.dataset_version = dataset_version
        self.version_service = DatasetVersionService()

        self._rows_read = 0
        self._dataset_rows = 0
        self._version_rows = 0

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
        self._rows_read = int(
            self.con.execute("SELECT COUNT(*) FROM research_features_daily").fetchone()[0]
        )
        if self._rows_read == 0:
            raise PipelineError("no rows available in research_features_daily")

    def load(self, data) -> None:
        con = self.con

        con.execute(
            """
            DELETE FROM research_dataset_daily
            WHERE dataset_name = ? AND dataset_version = ?
            """,
            [self.dataset_name, self.dataset_version],
        )

        con.execute(
            """
            DELETE FROM dataset_versions
            WHERE dataset_name = ? AND dataset_version = ?
            """,
            [self.dataset_name, self.dataset_version],
        )

        con.execute("DROP TABLE IF EXISTS tmp_dataset_joined")
        con.execute(
            """
            CREATE TEMP TABLE tmp_dataset_joined AS
            SELECT
                ? AS dataset_name,
                ? AS dataset_version,
                f.instrument_id,
                f.company_id,
                f.symbol,
                f.as_of_date,
                f.close_to_sma_20,
                f.rsi_14,
                f.revenue,
                f.net_income,
                f.net_margin,
                f.debt_to_equity,
                f.return_on_assets,
                f.short_interest,
                f.days_to_cover,
                f.short_volume_ratio,
                f.article_count_1d,
                f.unique_cluster_count_1d,
                f.avg_link_confidence,
                r.fwd_return_1d,
                r.fwd_return_5d,
                r.fwd_return_20d,
                r.direction_1d,
                r.direction_5d,
                r.direction_20d,
                v.realized_vol_20d,
                CURRENT_TIMESTAMP AS created_at
            FROM research_features_daily f
            LEFT JOIN return_labels_daily r
              ON f.instrument_id = r.instrument_id
             AND f.as_of_date = r.as_of_date
            LEFT JOIN volatility_labels_daily v
              ON f.instrument_id = v.instrument_id
             AND f.as_of_date = v.as_of_date
            """,
            [self.dataset_name, self.dataset_version],
        )

        con.execute(
            """
            INSERT INTO research_dataset_daily (
                dataset_name,
                dataset_version,
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
                fwd_return_1d,
                fwd_return_5d,
                fwd_return_20d,
                direction_1d,
                direction_5d,
                direction_20d,
                realized_vol_20d,
                created_at
            )
            SELECT
                dataset_name,
                dataset_version,
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
                fwd_return_1d,
                fwd_return_5d,
                fwd_return_20d,
                direction_1d,
                direction_5d,
                direction_20d,
                realized_vol_20d,
                created_at
            FROM tmp_dataset_joined
            """
        )

        self._dataset_rows = int(
            con.execute(
                """
                SELECT COUNT(*)
                FROM research_dataset_daily
                WHERE dataset_name = ? AND dataset_version = ?
                """,
                [self.dataset_name, self.dataset_version],
            ).fetchone()[0]
        )

        max_as_of = con.execute(
            """
            SELECT MAX(as_of_date)
            FROM research_dataset_daily
            WHERE dataset_name = ? AND dataset_version = ?
            """,
            [self.dataset_name, self.dataset_version],
        ).fetchone()[0]

        if max_as_of is None:
            raise PipelineError("dataset build produced zero rows")

        if self.repository is not None:
            version_row = self.version_service.build_dataset_version(
                dataset_name=self.dataset_name,
                dataset_version=self.dataset_version,
                as_of_date=str(max_as_of),
                row_count=self._dataset_rows,
            )
            self._version_rows = int(self.repository.insert_dataset_version(version_row))
        else:
            con.execute(
                """
                INSERT INTO dataset_versions (
                    dataset_name,
                    dataset_version,
                    universe_name,
                    as_of_date,
                    feature_run_id,
                    label_run_id,
                    row_count,
                    config_json,
                    created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    self.dataset_name,
                    self.dataset_version,
                    "default",
                    max_as_of,
                    None,
                    None,
                    self._dataset_rows,
                    "{}",
                    datetime.utcnow(),
                ],
            )
            self._version_rows = 1

    def finalize(self, result: PipelineResult) -> PipelineResult:
        result.rows_read = self._rows_read
        result.rows_written = self._dataset_rows + self._version_rows
        result.metrics["dataset_name"] = self.dataset_name
        result.metrics["dataset_version"] = self.dataset_version
        result.metrics["dataset_rows"] = self._dataset_rows
        result.metrics["written_dataset_rows"] = self._dataset_rows
        result.metrics["written_dataset_version"] = self._version_rows
        return result
