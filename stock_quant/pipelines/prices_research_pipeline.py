from __future__ import annotations

from stock_quant.app.dto.pipeline_result import PipelineResult
from stock_quant.infrastructure.db.unit_of_work import DuckDbUnitOfWork
from stock_quant.pipelines.base_pipeline import BasePipeline
from stock_quant.shared.exceptions import PipelineError


class BuildPricesResearchPipeline(BasePipeline):
    pipeline_name = "build_prices_research"

    def __init__(self, repository=None, uow: DuckDbUnitOfWork | None = None) -> None:
        if repository is not None:
            self.uow = repository.uow
        else:
            self.uow = uow

        if self.uow is None:
            raise ValueError("BuildPricesResearchPipeline requires repository or uow")

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
        count = self.con.execute("SELECT COUNT(*) FROM price_history").fetchone()[0]
        if int(count) == 0:
            raise PipelineError("no price_history rows available")

    def load(self, data) -> None:
        con = self.con

        input_price_rows = int(con.execute("SELECT COUNT(*) FROM price_history").fetchone()[0])

        con.execute("DELETE FROM price_bars_unadjusted")
        con.execute("DELETE FROM price_bars_adjusted")
        con.execute("DELETE FROM price_quality_flags")

        con.execute("DROP TABLE IF EXISTS tmp_prices_research_base")
        con.execute(
            """
            CREATE TEMP TABLE tmp_prices_research_base AS
            SELECT
                im.instrument_id,
                ph.symbol,
                ph.price_date AS bar_date,
                CAST(ph.open AS DOUBLE) AS open,
                CAST(ph.high AS DOUBLE) AS high,
                CAST(ph.low AS DOUBLE) AS low,
                CAST(ph.close AS DOUBLE) AS close,
                CAST(ph.volume AS BIGINT) AS volume,
                ph.source_name
            FROM price_history ph
            INNER JOIN instrument_master im
              ON UPPER(TRIM(ph.symbol)) = UPPER(TRIM(im.symbol))
            ORDER BY im.instrument_id, ph.price_date
            """
        )

        missing_instrument_map = int(
            con.execute(
                """
                SELECT COUNT(*)
                FROM price_history ph
                LEFT JOIN instrument_master im
                  ON UPPER(TRIM(ph.symbol)) = UPPER(TRIM(im.symbol))
                WHERE im.instrument_id IS NULL
                """
            ).fetchone()[0]
        )

        con.execute(
            """
            INSERT INTO price_bars_unadjusted (
                instrument_id,
                symbol,
                bar_date,
                open,
                high,
                low,
                close,
                volume,
                source_name,
                created_at
            )
            SELECT
                instrument_id,
                symbol,
                bar_date,
                open,
                high,
                low,
                close,
                volume,
                source_name,
                CURRENT_TIMESTAMP AS created_at
            FROM tmp_prices_research_base
            """
        )

        unadjusted_rows = int(
            con.execute("SELECT COUNT(*) FROM price_bars_unadjusted").fetchone()[0]
        )

        con.execute(
            """
            INSERT INTO price_bars_adjusted (
                instrument_id,
                symbol,
                bar_date,
                adj_open,
                adj_high,
                adj_low,
                adj_close,
                volume,
                adjustment_factor,
                source_name,
                created_at
            )
            SELECT
                instrument_id,
                symbol,
                bar_date,
                open AS adj_open,
                high AS adj_high,
                low AS adj_low,
                close AS adj_close,
                volume,
                1.0 AS adjustment_factor,
                source_name,
                CURRENT_TIMESTAMP AS created_at
            FROM tmp_prices_research_base
            """
        )

        adjusted_rows = int(
            con.execute("SELECT COUNT(*) FROM price_bars_adjusted").fetchone()[0]
        )

        con.execute(
            """
            INSERT INTO price_quality_flags (
                instrument_id,
                price_date,
                flag_type,
                flag_value,
                source_name,
                created_at
            )
            SELECT
                instrument_id,
                bar_date AS price_date,
                flag_type,
                flag_value,
                source_name,
                CURRENT_TIMESTAMP AS created_at
            FROM (
                SELECT
                    instrument_id,
                    bar_date,
                    source_name,
                    'non_positive_price' AS flag_type,
                    'close<=0_or_open<=0_or_high<=0_or_low<=0' AS flag_value
                FROM tmp_prices_research_base
                WHERE COALESCE(open, 0) <= 0
                   OR COALESCE(high, 0) <= 0
                   OR COALESCE(low, 0) <= 0
                   OR COALESCE(close, 0) <= 0

                UNION ALL

                SELECT
                    instrument_id,
                    bar_date,
                    source_name,
                    'invalid_ohlc_range' AS flag_type,
                    'high<low_or_close_outside_range_or_open_outside_range' AS flag_value
                FROM tmp_prices_research_base
                WHERE high < low
                   OR close < low
                   OR close > high
                   OR open < low
                   OR open > high

                UNION ALL

                SELECT
                    instrument_id,
                    bar_date,
                    source_name,
                    'negative_volume' AS flag_type,
                    'volume<0' AS flag_value
                FROM tmp_prices_research_base
                WHERE COALESCE(volume, 0) < 0
            ) q
            """
        )

        quality_flags = int(
            con.execute("SELECT COUNT(*) FROM price_quality_flags").fetchone()[0]
        )

        self._rows_written = unadjusted_rows + adjusted_rows + quality_flags
        self._metrics = {
            "input_price_rows": input_price_rows,
            "missing_instrument_map": missing_instrument_map,
            "unadjusted_rows": unadjusted_rows,
            "adjusted_rows": adjusted_rows,
            "quality_flags": quality_flags,
            "written_unadjusted": unadjusted_rows,
            "written_adjusted": adjusted_rows,
            "written_quality_flags": quality_flags,
        }

    def finalize(self, result: PipelineResult) -> PipelineResult:
        result.rows_read = int(self._metrics.get("input_price_rows", 0))
        result.rows_written = self._rows_written
        result.metrics.update(self._metrics)
        return result
