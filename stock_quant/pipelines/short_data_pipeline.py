from __future__ import annotations

from stock_quant.app.dto.pipeline_result import PipelineResult
from stock_quant.infrastructure.db.unit_of_work import DuckDbUnitOfWork
from stock_quant.pipelines.base_pipeline import BasePipeline
from stock_quant.shared.exceptions import PipelineError


class BuildShortDataPipeline(BasePipeline):
    pipeline_name = "build_short_data"

    def __init__(self, repository=None, uow: DuckDbUnitOfWork | None = None) -> None:
        if repository is not None:
            self.uow = repository.uow
        else:
            self.uow = uow

        if self.uow is None:
            raise ValueError("BuildShortDataPipeline requires repository or uow")

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
        raw_short_interest_rows = int(
            self.con.execute("SELECT COUNT(*) FROM finra_short_interest_source_raw").fetchone()[0]
        )
        raw_daily_short_volume_rows = int(
            self.con.execute("SELECT COUNT(*) FROM finra_daily_short_volume_source_raw").fetchone()[0]
        )

        if raw_short_interest_rows == 0 and raw_daily_short_volume_rows == 0:
            raise PipelineError("no short raw rows available")

    def load(self, data) -> None:
        con = self.con

        raw_short_interest_rows = int(
            con.execute("SELECT COUNT(*) FROM finra_short_interest_source_raw").fetchone()[0]
        )
        raw_daily_short_volume_rows = int(
            con.execute("SELECT COUNT(*) FROM finra_daily_short_volume_source_raw").fetchone()[0]
        )

        con.execute("DELETE FROM short_interest_history")
        con.execute("DELETE FROM daily_short_volume_history")
        con.execute("DELETE FROM short_features_daily")

        con.execute("DROP TABLE IF EXISTS tmp_symbol_map")
        con.execute(
            """
            CREATE TEMP TABLE tmp_symbol_map AS
            SELECT
                UPPER(TRIM(symbol)) AS symbol_key,
                instrument_id,
                company_id,
                symbol
            FROM instrument_master
            """
        )

        con.execute("DROP TABLE IF EXISTS tmp_short_interest_base")
        con.execute(
            """
            CREATE TEMP TABLE tmp_short_interest_base AS
            SELECT
                sm.instrument_id,
                sm.company_id,
                sm.symbol,
                r.settlement_date,
                CAST(r.short_interest AS DOUBLE) AS short_interest,
                CAST(r.previous_short_interest AS DOUBLE) AS previous_short_interest,
                CAST(r.avg_daily_volume AS DOUBLE) AS avg_daily_volume,
                CASE
                    WHEN r.avg_daily_volume IS NOT NULL
                         AND r.avg_daily_volume <> 0
                         AND r.short_interest IS NOT NULL
                    THEN CAST(r.short_interest AS DOUBLE) / CAST(r.avg_daily_volume AS DOUBLE)
                    ELSE NULL
                END AS days_to_cover,
                COALESCE(r.source_file, 'finra_short_interest') AS source_name
            FROM finra_short_interest_source_raw r
            INNER JOIN tmp_symbol_map sm
              ON UPPER(TRIM(r.symbol)) = sm.symbol_key
            """
        )

        con.execute(
            """
            INSERT INTO short_interest_history (
                instrument_id,
                company_id,
                symbol,
                settlement_date,
                short_interest,
                previous_short_interest,
                avg_daily_volume,
                days_to_cover,
                source_name,
                created_at
            )
            SELECT
                instrument_id,
                company_id,
                symbol,
                settlement_date,
                short_interest,
                previous_short_interest,
                avg_daily_volume,
                days_to_cover,
                source_name,
                CURRENT_TIMESTAMP AS created_at
            FROM tmp_short_interest_base
            """
        )

        short_interest_history_rows = int(
            con.execute("SELECT COUNT(*) FROM short_interest_history").fetchone()[0]
        )

        con.execute("DROP TABLE IF EXISTS tmp_daily_short_volume_base")
        con.execute(
            """
            CREATE TEMP TABLE tmp_daily_short_volume_base AS
            SELECT
                sm.instrument_id,
                sm.company_id,
                sm.symbol,
                r.trade_date,
                CAST(r.short_volume AS DOUBLE) AS short_volume,
                CAST(r.total_volume AS DOUBLE) AS total_volume,
                CASE
                    WHEN r.total_volume IS NOT NULL
                         AND r.total_volume <> 0
                         AND r.short_volume IS NOT NULL
                    THEN CAST(r.short_volume AS DOUBLE) / CAST(r.total_volume AS DOUBLE)
                    ELSE NULL
                END AS short_volume_ratio,
                COALESCE(r.source_name, 'finra') AS source_name
            FROM finra_daily_short_volume_source_raw r
            INNER JOIN tmp_symbol_map sm
              ON UPPER(TRIM(r.symbol)) = sm.symbol_key
            """
        )

        con.execute(
            """
            INSERT INTO daily_short_volume_history (
                instrument_id,
                company_id,
                symbol,
                trade_date,
                short_volume,
                total_volume,
                short_volume_ratio,
                source_name,
                created_at
            )
            SELECT
                instrument_id,
                company_id,
                symbol,
                trade_date,
                short_volume,
                total_volume,
                short_volume_ratio,
                source_name,
                CURRENT_TIMESTAMP AS created_at
            FROM tmp_daily_short_volume_base
            """
        )

        daily_short_volume_history_rows = int(
            con.execute("SELECT COUNT(*) FROM daily_short_volume_history").fetchone()[0]
        )

        con.execute(
            """
            INSERT INTO short_features_daily (
                instrument_id,
                company_id,
                symbol,
                as_of_date,
                short_interest,
                avg_daily_volume,
                days_to_cover,
                short_volume,
                total_volume,
                short_volume_ratio,
                short_interest_change,
                source_name,
                created_at
            )
            SELECT
                COALESCE(d.instrument_id, s.instrument_id) AS instrument_id,
                COALESCE(d.company_id, s.company_id) AS company_id,
                COALESCE(d.symbol, s.symbol) AS symbol,
                COALESCE(d.trade_date, s.settlement_date) AS as_of_date,
                s.short_interest,
                s.avg_daily_volume,
                s.days_to_cover,
                d.short_volume,
                d.total_volume,
                d.short_volume_ratio,
                CASE
                    WHEN s.short_interest IS NOT NULL
                         AND s.previous_short_interest IS NOT NULL
                    THEN s.short_interest - s.previous_short_interest
                    ELSE NULL
                END AS short_interest_change,
                COALESCE(d.source_name, s.source_name, 'finra') AS source_name,
                CURRENT_TIMESTAMP AS created_at
            FROM tmp_daily_short_volume_base d
            FULL OUTER JOIN tmp_short_interest_base s
              ON d.instrument_id = s.instrument_id
             AND d.trade_date = s.settlement_date
            """
        )

        short_feature_rows = int(
            con.execute("SELECT COUNT(*) FROM short_features_daily").fetchone()[0]
        )

        self._rows_written = (
            short_interest_history_rows
            + daily_short_volume_history_rows
            + short_feature_rows
        )
        self._metrics = {
            "raw_short_interest_rows": raw_short_interest_rows,
            "raw_daily_short_volume_rows": raw_daily_short_volume_rows,
            "short_interest_history_rows": short_interest_history_rows,
            "daily_short_volume_history_rows": daily_short_volume_history_rows,
            "short_feature_rows": short_feature_rows,
            "written_short_interest": short_interest_history_rows,
            "written_daily_short_volume": daily_short_volume_history_rows,
            "written_short_features": short_feature_rows,
        }

    def finalize(self, result: PipelineResult) -> PipelineResult:
        result.rows_read = int(
            self._metrics.get("raw_short_interest_rows", 0)
            + self._metrics.get("raw_daily_short_volume_rows", 0)
        )
        result.rows_written = self._rows_written
        result.metrics.update(self._metrics)
        return result
