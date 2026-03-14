from __future__ import annotations

from stock_quant.app.dto.pipeline_result import PipelineResult
from stock_quant.infrastructure.db.unit_of_work import DuckDbUnitOfWork
from stock_quant.pipelines.base_pipeline import BasePipeline
from stock_quant.shared.exceptions import PipelineError


class BuildLabelEnginePipeline(BasePipeline):
    pipeline_name = "build_label_engine"

    def __init__(self, repository=None, uow: DuckDbUnitOfWork | None = None) -> None:
        if repository is not None:
            self.uow = repository.uow
        else:
            self.uow = uow

        if self.uow is None:
            raise ValueError("BuildLabelEnginePipeline requires repository or uow")

        self._return_rows = 0
        self._vol_rows = 0

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

        con.execute("DELETE FROM return_labels_daily")
        con.execute("DELETE FROM volatility_labels_daily")

        con.execute("DROP TABLE IF EXISTS tmp_label_base")
        con.execute(
            """
            CREATE TEMP TABLE tmp_label_base AS
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

        con.execute("DROP TABLE IF EXISTS tmp_return_labels")
        con.execute(
            """
            CREATE TEMP TABLE tmp_return_labels AS
            SELECT
                instrument_id,
                company_id,
                symbol,
                as_of_date,

                CASE
                    WHEN LEAD(adj_close, 1) OVER w IS NULL OR adj_close = 0 THEN NULL
                    ELSE (LEAD(adj_close, 1) OVER w / adj_close) - 1
                END AS fwd_return_1d,

                CASE
                    WHEN LEAD(adj_close, 5) OVER w IS NULL OR adj_close = 0 THEN NULL
                    ELSE (LEAD(adj_close, 5) OVER w / adj_close) - 1
                END AS fwd_return_5d,

                CASE
                    WHEN LEAD(adj_close, 20) OVER w IS NULL OR adj_close = 0 THEN NULL
                    ELSE (LEAD(adj_close, 20) OVER w / adj_close) - 1
                END AS fwd_return_20d

            FROM tmp_label_base
            WINDOW w AS (
                PARTITION BY instrument_id
                ORDER BY as_of_date
            )
            """
        )

        con.execute(
            """
            INSERT INTO return_labels_daily (
                instrument_id,
                company_id,
                symbol,
                as_of_date,
                fwd_return_1d,
                fwd_return_5d,
                fwd_return_20d,
                direction_1d,
                direction_5d,
                direction_20d,
                source_name,
                created_at
            )
            SELECT
                instrument_id,
                company_id,
                symbol,
                as_of_date,
                fwd_return_1d,
                fwd_return_5d,
                fwd_return_20d,
                CASE
                    WHEN fwd_return_1d IS NULL THEN NULL
                    WHEN fwd_return_1d > 0 THEN 1
                    WHEN fwd_return_1d < 0 THEN -1
                    ELSE 0
                END AS direction_1d,
                CASE
                    WHEN fwd_return_5d IS NULL THEN NULL
                    WHEN fwd_return_5d > 0 THEN 1
                    WHEN fwd_return_5d < 0 THEN -1
                    ELSE 0
                END AS direction_5d,
                CASE
                    WHEN fwd_return_20d IS NULL THEN NULL
                    WHEN fwd_return_20d > 0 THEN 1
                    WHEN fwd_return_20d < 0 THEN -1
                    ELSE 0
                END AS direction_20d,
                'labels' AS source_name,
                CURRENT_TIMESTAMP AS created_at
            FROM tmp_return_labels
            """
        )

        self._return_rows = int(
            con.execute("SELECT COUNT(*) FROM return_labels_daily").fetchone()[0]
        )

        con.execute("DROP TABLE IF EXISTS tmp_vol_base")
        con.execute(
            """
            CREATE TEMP TABLE tmp_vol_base AS
            WITH base AS (
                SELECT
                    instrument_id,
                    company_id,
                    symbol,
                    as_of_date,
                    adj_close,
                    LAG(adj_close) OVER (
                        PARTITION BY instrument_id
                        ORDER BY as_of_date
                    ) AS prev_close
                FROM tmp_label_base
            ),
            returns AS (
                SELECT
                    instrument_id,
                    company_id,
                    symbol,
                    as_of_date,
                    CASE
                        WHEN prev_close IS NULL OR prev_close = 0 THEN NULL
                        ELSE (adj_close / prev_close) - 1
                    END AS daily_ret
                FROM base
            )
            SELECT
                instrument_id,
                company_id,
                symbol,
                as_of_date,
                STDDEV_SAMP(daily_ret) OVER (
                    PARTITION BY instrument_id
                    ORDER BY as_of_date
                    ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                ) AS realized_vol_20d
            FROM returns
            """
        )

        con.execute(
            """
            INSERT INTO volatility_labels_daily (
                instrument_id,
                company_id,
                symbol,
                as_of_date,
                realized_vol_20d,
                source_name,
                created_at
            )
            SELECT
                instrument_id,
                company_id,
                symbol,
                as_of_date,
                realized_vol_20d,
                'labels' AS source_name,
                CURRENT_TIMESTAMP AS created_at
            FROM tmp_vol_base
            """
        )

        self._vol_rows = int(
            con.execute("SELECT COUNT(*) FROM volatility_labels_daily").fetchone()[0]
        )

    def finalize(self, result: PipelineResult) -> PipelineResult:
        result.rows_read = self._return_rows
        result.rows_written = self._return_rows + self._vol_rows
        result.metrics["return_label_rows"] = self._return_rows
        result.metrics["volatility_label_rows"] = self._vol_rows
        result.metrics["written_return_labels"] = self._return_rows
        result.metrics["written_volatility_labels"] = self._vol_rows
        return result
