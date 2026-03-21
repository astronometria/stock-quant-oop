from __future__ import annotations

from stock_quant.app.dto.pipeline_result import PipelineResult
from stock_quant.infrastructure.db.unit_of_work import DuckDbUnitOfWork
# REMOVED: BasePipeline supprimé pendant la consolidation brutale
from stock_quant.shared.exceptions import PipelineError


class BuildLabelEnginePipeline(BasePipeline):
    pipeline_name = "build_label_engine"

    def __init__(self, repository=None, uow: DuckDbUnitOfWork | None = None) -> None:
        if repository is not None:
            self.uow = repository.uow
            self.repository = repository
        else:
            self.uow = uow
            self.repository = None

        if self.uow is None:
            raise ValueError("BuildLabelEnginePipeline requires repository or uow")

        self._input_rows = 0
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

    def _ensure_label_schema(self) -> None:
        con = self.con

        con.execute(
            """
            CREATE TABLE IF NOT EXISTS return_labels_daily (
                instrument_id VARCHAR,
                as_of_date DATE,
                fwd_return_1d DOUBLE,
                fwd_return_5d DOUBLE,
                fwd_return_20d DOUBLE,
                direction_1d INTEGER,
                direction_5d INTEGER,
                direction_20d INTEGER,
                source_name VARCHAR,
                created_at TIMESTAMP
            )
            """
        )

        con.execute(
            """
            CREATE TABLE IF NOT EXISTS volatility_labels_daily (
                instrument_id VARCHAR,
                as_of_date DATE,
                realized_vol_20d DOUBLE,
                source_name VARCHAR,
                created_at TIMESTAMP
            )
            """
        )

        con.execute("ALTER TABLE return_labels_daily ADD COLUMN IF NOT EXISTS direction_1d INTEGER")
        con.execute("ALTER TABLE return_labels_daily ADD COLUMN IF NOT EXISTS direction_5d INTEGER")
        con.execute("ALTER TABLE return_labels_daily ADD COLUMN IF NOT EXISTS direction_20d INTEGER")
        con.execute("ALTER TABLE return_labels_daily ADD COLUMN IF NOT EXISTS source_name VARCHAR")
        con.execute("ALTER TABLE return_labels_daily ADD COLUMN IF NOT EXISTS created_at TIMESTAMP")

        con.execute("ALTER TABLE volatility_labels_daily ADD COLUMN IF NOT EXISTS source_name VARCHAR")
        con.execute("ALTER TABLE volatility_labels_daily ADD COLUMN IF NOT EXISTS created_at TIMESTAMP")

    def load(self, data) -> None:
        con = self.con

        self._input_rows = int(
            con.execute("SELECT COUNT(*) FROM price_bars_adjusted").fetchone()[0]
        )

        self._ensure_label_schema()

        con.execute("DELETE FROM return_labels_daily")
        con.execute("DELETE FROM volatility_labels_daily")

        con.execute(
            """
            INSERT INTO return_labels_daily (
                instrument_id,
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
            WITH base AS (
                SELECT
                    instrument_id,
                    bar_date AS as_of_date,
                    adj_close
                FROM price_bars_adjusted
            ),
            forward_returns AS (
                SELECT
                    instrument_id,
                    as_of_date,
                    CASE
                        WHEN LEAD(adj_close, 1) OVER w IS NULL OR adj_close IS NULL OR adj_close = 0
                            THEN NULL
                        ELSE (LEAD(adj_close, 1) OVER w / adj_close) - 1
                    END AS fwd_return_1d,
                    CASE
                        WHEN LEAD(adj_close, 5) OVER w IS NULL OR adj_close IS NULL OR adj_close = 0
                            THEN NULL
                        ELSE (LEAD(adj_close, 5) OVER w / adj_close) - 1
                    END AS fwd_return_5d,
                    CASE
                        WHEN LEAD(adj_close, 20) OVER w IS NULL OR adj_close IS NULL OR adj_close = 0
                            THEN NULL
                        ELSE (LEAD(adj_close, 20) OVER w / adj_close) - 1
                    END AS fwd_return_20d
                FROM base
                WINDOW w AS (
                    PARTITION BY instrument_id
                    ORDER BY as_of_date
                )
            )
            SELECT
                instrument_id,
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
                'prices' AS source_name,
                CURRENT_TIMESTAMP AS created_at
            FROM forward_returns
            """
        )

        self._return_rows = int(
            con.execute("SELECT COUNT(*) FROM return_labels_daily").fetchone()[0]
        )

        con.execute(
            """
            INSERT INTO volatility_labels_daily (
                instrument_id,
                as_of_date,
                realized_vol_20d,
                source_name,
                created_at
            )
            WITH base AS (
                SELECT
                    instrument_id,
                    bar_date AS as_of_date,
                    LN(
                        adj_close /
                        LAG(adj_close) OVER (
                            PARTITION BY instrument_id
                            ORDER BY bar_date
                        )
                    ) AS log_ret
                FROM price_bars_adjusted
            ),
            forward_vol AS (
                SELECT
                    instrument_id,
                    as_of_date,
                    STDDEV_SAMP(log_ret) OVER (
                        PARTITION BY instrument_id
                        ORDER BY as_of_date
                        ROWS BETWEEN 1 FOLLOWING AND 20 FOLLOWING
                    ) AS realized_vol_20d
                FROM base
            )
            SELECT
                instrument_id,
                as_of_date,
                realized_vol_20d,
                'prices' AS source_name,
                CURRENT_TIMESTAMP AS created_at
            FROM forward_vol
            """
        )

        self._vol_rows = int(
            con.execute("SELECT COUNT(*) FROM volatility_labels_daily").fetchone()[0]
        )

    def finalize(self, result: PipelineResult) -> PipelineResult:
        result.rows_read = self._input_rows
        result.rows_written = self._return_rows + self._vol_rows
        result.metrics["return_label_rows"] = self._return_rows
        result.metrics["volatility_label_rows"] = self._vol_rows
        result.metrics["written_return_labels"] = self._return_rows
        result.metrics["written_volatility_labels"] = self._vol_rows
        return result
