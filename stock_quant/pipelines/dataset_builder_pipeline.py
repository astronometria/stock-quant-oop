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
        self._research_universe_rows = 0

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
        included_universe_count = int(
            self.con.execute(
                """
                SELECT COUNT(*)
                FROM research_universe
                WHERE include_in_research_universe = TRUE
                """
            ).fetchone()[0]
        )
        if included_universe_count == 0:
            raise PipelineError("no included rows available in research_universe")

        self._rows_read = int(
            self.con.execute(
                """
                SELECT COUNT(*)
                FROM research_features_daily f
                INNER JOIN research_universe ru
                    ON UPPER(TRIM(f.symbol)) = UPPER(TRIM(ru.symbol))
                WHERE ru.include_in_research_universe = TRUE
                """
            ).fetchone()[0]
        )
        if self._rows_read == 0:
            raise PipelineError("no filtered rows available in research_features_daily")

    def _rebuild_dataset_schema_if_needed(self) -> None:
        info = self.con.execute("PRAGMA table_info('research_dataset_daily')").fetchall()
        cols = [row[1] for row in info]
        if "short_interest_pct_volume" not in cols:
            return

        self.con.execute("DROP TABLE IF EXISTS research_dataset_daily__new")
        self.con.execute(
            """
            CREATE TABLE research_dataset_daily__new (
                dataset_name VARCHAR,
                dataset_version VARCHAR,
                instrument_id VARCHAR,
                company_id VARCHAR,
                symbol VARCHAR,
                as_of_date DATE,
                close_to_sma_20 DOUBLE,
                rsi_14 DOUBLE,
                revenue DOUBLE,
                net_income DOUBLE,
                net_margin DOUBLE,
                debt_to_equity DOUBLE,
                return_on_assets DOUBLE,
                short_interest DOUBLE,
                days_to_cover DOUBLE,
                short_volume_ratio DOUBLE,
                article_count_1d BIGINT,
                unique_cluster_count_1d BIGINT,
                avg_link_confidence DOUBLE,
                fwd_return_1d DOUBLE,
                fwd_return_5d DOUBLE,
                fwd_return_20d DOUBLE,
                direction_1d INTEGER,
                direction_5d INTEGER,
                direction_20d INTEGER,
                realized_vol_20d DOUBLE,
                created_at TIMESTAMP,
                short_squeeze_score DOUBLE,
                short_pressure_zscore DOUBLE,
                days_to_cover_zscore DOUBLE,
                short_interest_change_pct DOUBLE
            )
            """
        )
        self.con.execute("DROP TABLE research_dataset_daily")
        self.con.execute("ALTER TABLE research_dataset_daily__new RENAME TO research_dataset_daily")

    def load(self, data) -> None:
        con = self.con
        self._rebuild_dataset_schema_if_needed()

        existing_columns = {
            row[1]
            for row in con.execute("PRAGMA table_info('research_dataset_daily')").fetchall()
        }
        for column_name, sql in [
            ("short_interest_change_pct", "ALTER TABLE research_dataset_daily ADD COLUMN short_interest_change_pct DOUBLE"),
            ("short_squeeze_score", "ALTER TABLE research_dataset_daily ADD COLUMN short_squeeze_score DOUBLE"),
            ("short_pressure_zscore", "ALTER TABLE research_dataset_daily ADD COLUMN short_pressure_zscore DOUBLE"),
            ("days_to_cover_zscore", "ALTER TABLE research_dataset_daily ADD COLUMN days_to_cover_zscore DOUBLE"),
        ]:
            if column_name not in existing_columns:
                con.execute(sql)

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

        con.execute("DROP TABLE IF EXISTS tmp_research_universe_symbols")
        con.execute("DROP TABLE IF EXISTS tmp_dataset_joined")

        con.execute(
            """
            CREATE TEMP TABLE tmp_research_universe_symbols AS
            SELECT DISTINCT symbol
            FROM research_universe
            WHERE include_in_research_universe = TRUE
            """
        )

        self._research_universe_rows = int(
            con.execute("SELECT COUNT(*) FROM tmp_research_universe_symbols").fetchone()[0]
        )

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
                CURRENT_TIMESTAMP AS created_at,
                f.short_squeeze_score,
                f.short_pressure_zscore,
                f.days_to_cover_zscore,
                f.short_interest_change_pct
            FROM research_features_daily f
            INNER JOIN tmp_research_universe_symbols ru
                ON UPPER(TRIM(f.symbol)) = UPPER(TRIM(ru.symbol))
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
                created_at,
                short_squeeze_score,
                short_pressure_zscore,
                days_to_cover_zscore,
                short_interest_change_pct
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
                created_at,
                short_squeeze_score,
                short_pressure_zscore,
                days_to_cover_zscore,
                short_interest_change_pct
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
                    row_count,
                    created_at
                )
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                [
                    self.dataset_name,
                    self.dataset_version,
                    "research_universe",
                    max_as_of,
                    self._dataset_rows,
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
        result.metrics["research_universe_rows"] = self._research_universe_rows
        result.metrics["written_dataset_rows"] = self._dataset_rows
        result.metrics["written_dataset_version"] = self._version_rows
        return result
