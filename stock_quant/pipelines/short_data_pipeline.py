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

        con.execute("DROP TABLE IF EXISTS tmp_ticker_map")
        con.execute(
            """
            CREATE TEMP TABLE tmp_ticker_map AS
            SELECT
                UPPER(TRIM(th.symbol)) AS symbol_key,
                th.symbol,
                th.instrument_id,
                im.company_id,
                th.valid_from,
                COALESCE(th.valid_to, DATE '9999-12-31') AS valid_to,
                th.is_current
            FROM ticker_history th
            LEFT JOIN instrument_master im
              ON th.instrument_id = im.instrument_id
            WHERE th.symbol IS NOT NULL
              AND TRIM(th.symbol) <> ''
            """
        )

        con.execute("DROP TABLE IF EXISTS tmp_short_interest_base")
        con.execute(
            """
            CREATE TEMP TABLE tmp_short_interest_base AS
            WITH ranked AS (
                SELECT
                    m.instrument_id,
                    m.company_id,
                    COALESCE(m.symbol, UPPER(TRIM(r.symbol))) AS symbol,
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
                    COALESCE(r.source_file, 'finra_short_interest') AS source_name,
                    ROW_NUMBER() OVER (
                        PARTITION BY UPPER(TRIM(r.symbol)), r.settlement_date, COALESCE(r.source_file, 'finra_short_interest')
                        ORDER BY m.valid_from DESC NULLS LAST, m.is_current DESC
                    ) AS rn
                FROM finra_short_interest_source_raw r
                LEFT JOIN tmp_ticker_map m
                  ON UPPER(TRIM(r.symbol)) = m.symbol_key
                 AND r.settlement_date >= m.valid_from
                 AND r.settlement_date <= m.valid_to
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
                source_name
            FROM ranked
            WHERE rn = 1
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
        pit_short_interest_mapped_rows = int(
            con.execute(
                "SELECT COUNT(*) FROM tmp_short_interest_base WHERE instrument_id IS NOT NULL"
            ).fetchone()[0]
        )
        unmapped_short_interest_rows = int(
            con.execute(
                "SELECT COUNT(*) FROM tmp_short_interest_base WHERE instrument_id IS NULL"
            ).fetchone()[0]
        )

        con.execute("DROP TABLE IF EXISTS tmp_daily_short_volume_base")
        con.execute(
            """
            CREATE TEMP TABLE tmp_daily_short_volume_base AS
            WITH ranked AS (
                SELECT
                    m.instrument_id,
                    m.company_id,
                    COALESCE(m.symbol, UPPER(TRIM(r.symbol))) AS symbol,
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
                    COALESCE(r.source_name, 'finra') AS source_name,
                    ROW_NUMBER() OVER (
                        PARTITION BY UPPER(TRIM(r.symbol)), r.trade_date, COALESCE(r.source_name, 'finra')
                        ORDER BY m.valid_from DESC NULLS LAST, m.is_current DESC
                    ) AS rn
                FROM finra_daily_short_volume_source_raw r
                LEFT JOIN tmp_ticker_map m
                  ON UPPER(TRIM(r.symbol)) = m.symbol_key
                 AND r.trade_date >= m.valid_from
                 AND r.trade_date <= m.valid_to
            )
            SELECT
                instrument_id,
                company_id,
                symbol,
                trade_date,
                short_volume,
                total_volume,
                short_volume_ratio,
                source_name
            FROM ranked
            WHERE rn = 1
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
        pit_daily_short_volume_mapped_rows = int(
            con.execute(
                "SELECT COUNT(*) FROM tmp_daily_short_volume_base WHERE instrument_id IS NOT NULL"
            ).fetchone()[0]
        )
        unmapped_daily_short_volume_rows = int(
            con.execute(
                "SELECT COUNT(*) FROM tmp_daily_short_volume_base WHERE instrument_id IS NULL"
            ).fetchone()[0]
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
              ON d.trade_date = s.settlement_date
             AND (
                    (d.instrument_id IS NOT NULL AND s.instrument_id IS NOT NULL AND d.instrument_id = s.instrument_id)
                 OR (d.instrument_id IS NULL AND s.instrument_id IS NULL AND d.symbol = s.symbol)
                 )
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
            "pit_short_interest_mapped_rows": pit_short_interest_mapped_rows,
            "pit_daily_short_volume_mapped_rows": pit_daily_short_volume_mapped_rows,
            "unmapped_short_interest_rows": unmapped_short_interest_rows,
            "unmapped_daily_short_volume_rows": unmapped_daily_short_volume_rows,
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
