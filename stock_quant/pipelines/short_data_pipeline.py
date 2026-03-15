from __future__ import annotations

from stock_quant.app.dto.pipeline_result import PipelineResult
from stock_quant.infrastructure.db.unit_of_work import DuckDbUnitOfWork
from stock_quant.pipelines.base_pipeline import BasePipeline
from stock_quant.shared.exceptions import PipelineError

try:
    from tqdm import tqdm
except Exception:
    def tqdm(iterable, **kwargs):
        return iterable


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
        self._progress_total_steps = 7

    @property
    def con(self):
        if self.uow.connection is None:
            raise PipelineError("active DB connection required")
        return self.uow.connection

    def extract(self):
        return None

    def transform(self, data):
        return None

    def validate(self, data):
        short_interest_rows = int(
            self.con.execute("SELECT COUNT(*) FROM short_interest_history").fetchone()[0]
        )
        short_volume_rows = int(
            self.con.execute("SELECT COUNT(*) FROM finra_daily_short_volume_source_raw").fetchone()[0]
        )

        if short_interest_rows == 0:
            raise PipelineError("short_interest_history is empty — run build_finra_short_interest first")

        if short_volume_rows == 0:
            raise PipelineError("daily short volume raw table empty")

    def _log_step(self, step_no: int, label: str):
        print(
            f"[build_short_data] step {step_no}/{self._progress_total_steps}: {label}",
            flush=True,
        )

    def _rebuild_short_features_schema_if_needed(self) -> None:
        info = self.con.execute("PRAGMA table_info('short_features_daily')").fetchall()
        cols = [row[1] for row in info]
        if "short_interest_pct_volume" not in cols:
            return

        self.con.execute("DROP TABLE IF EXISTS short_features_daily__new")
        self.con.execute(
            """
            CREATE TABLE short_features_daily__new (
                instrument_id VARCHAR,
                company_id VARCHAR,
                symbol VARCHAR,
                as_of_date DATE,
                short_interest DOUBLE,
                avg_daily_volume DOUBLE,
                days_to_cover DOUBLE,
                short_volume DOUBLE,
                total_volume DOUBLE,
                short_volume_ratio DOUBLE,
                short_interest_change DOUBLE,
                short_interest_change_pct DOUBLE,
                short_squeeze_score DOUBLE,
                short_pressure_zscore DOUBLE,
                days_to_cover_zscore DOUBLE,
                source_name VARCHAR,
                created_at TIMESTAMP
            )
            """
        )
        self.con.execute("DROP TABLE short_features_daily")
        self.con.execute("ALTER TABLE short_features_daily__new RENAME TO short_features_daily")

    def load(self, data):
        con = self.con
        self._rebuild_short_features_schema_if_needed()

        existing_columns = {
            row[1]
            for row in con.execute("PRAGMA table_info('short_features_daily')").fetchall()
        }
        for column_name, sql in [
            ("short_interest_change_pct", "ALTER TABLE short_features_daily ADD COLUMN short_interest_change_pct DOUBLE"),
            ("short_squeeze_score", "ALTER TABLE short_features_daily ADD COLUMN short_squeeze_score DOUBLE"),
            ("short_pressure_zscore", "ALTER TABLE short_features_daily ADD COLUMN short_pressure_zscore DOUBLE"),
            ("days_to_cover_zscore", "ALTER TABLE short_features_daily ADD COLUMN days_to_cover_zscore DOUBLE"),
        ]:
            if column_name not in existing_columns:
                con.execute(sql)

        short_interest_rows = int(
            con.execute("SELECT COUNT(*) FROM short_interest_history").fetchone()[0]
        )
        short_volume_rows = int(
            con.execute("SELECT COUNT(*) FROM finra_daily_short_volume_source_raw").fetchone()[0]
        )

        progress = tqdm(
            total=self._progress_total_steps,
            desc="build_short_data",
            unit="step",
            dynamic_ncols=True,
            mininterval=0.2,
        )

        self._log_step(1, "reset target tables")
        con.execute("DELETE FROM daily_short_volume_history")
        con.execute("DELETE FROM short_features_daily")
        progress.update(1)

        self._log_step(2, "build ticker map")
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
            """
        )
        progress.update(1)

        self._log_step(3, "build daily short volume history")
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
                        WHEN r.total_volume <> 0
                        THEN r.short_volume / r.total_volume
                        ELSE NULL
                    END AS short_volume_ratio,
                    r.source_name,
                    ROW_NUMBER() OVER (
                        PARTITION BY UPPER(TRIM(r.symbol)), r.trade_date
                        ORDER BY m.valid_from DESC NULLS LAST
                    ) AS rn
                FROM finra_daily_short_volume_source_raw r
                LEFT JOIN tmp_ticker_map m
                    ON UPPER(TRIM(r.symbol)) = m.symbol_key
                   AND r.trade_date BETWEEN m.valid_from AND m.valid_to
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
                CURRENT_TIMESTAMP
            FROM tmp_daily_short_volume_base
            """
        )
        progress.update(1)

        self._log_step(4, "resolve settlement date mapping")
        con.execute("DROP TABLE IF EXISTS tmp_short_interest_match_dates")
        con.execute(
            """
            CREATE TEMP TABLE tmp_short_interest_match_dates AS
            SELECT
                d.symbol,
                d.trade_date,
                MAX(s.settlement_date) AS settlement_date
            FROM tmp_daily_short_volume_base d
            LEFT JOIN short_interest_history s
                ON d.symbol = s.symbol
               AND s.settlement_date <= d.trade_date
            GROUP BY d.symbol, d.trade_date
            """
        )
        progress.update(1)

        self._log_step(5, "join short interest")
        con.execute("DROP TABLE IF EXISTS tmp_short_interest_join")
        con.execute(
            """
            CREATE TEMP TABLE tmp_short_interest_join AS
            SELECT
                d.instrument_id,
                d.company_id,
                d.symbol,
                d.trade_date,
                d.short_volume,
                d.total_volume,
                d.short_volume_ratio,
                d.source_name,
                s.short_interest,
                s.avg_daily_volume,
                s.days_to_cover,
                s.previous_short_interest
            FROM tmp_daily_short_volume_base d
            LEFT JOIN tmp_short_interest_match_dates m
                ON d.symbol = m.symbol
               AND d.trade_date = m.trade_date
            LEFT JOIN short_interest_history s
                ON d.symbol = s.symbol
               AND s.settlement_date = m.settlement_date
            """
        )
        progress.update(1)

        self._log_step(6, "build short features")
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
                short_interest_change_pct,
                short_squeeze_score,
                short_pressure_zscore,
                days_to_cover_zscore,
                source_name,
                created_at
            )
            SELECT
                instrument_id,
                company_id,
                symbol,
                trade_date,
                short_interest,
                avg_daily_volume,
                days_to_cover,
                short_volume,
                total_volume,
                short_volume_ratio,
                CASE
                    WHEN short_interest IS NOT NULL AND previous_short_interest IS NOT NULL
                    THEN short_interest - previous_short_interest
                    ELSE NULL
                END AS short_interest_change,
                CASE
                    WHEN previous_short_interest IS NOT NULL
                     AND previous_short_interest <> 0
                     AND short_interest IS NOT NULL
                    THEN (short_interest - previous_short_interest) / previous_short_interest
                    ELSE NULL
                END AS short_interest_change_pct,
                CASE
                    WHEN days_to_cover IS NOT NULL AND short_volume_ratio IS NOT NULL
                    THEN (0.7 * days_to_cover) + (0.3 * (100.0 * short_volume_ratio))
                    ELSE NULL
                END AS short_squeeze_score,
                CASE
                    WHEN COUNT(short_volume_ratio) OVER (
                        PARTITION BY instrument_id
                        ORDER BY trade_date
                        ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
                    ) >= 20
                     AND STDDEV_SAMP(short_volume_ratio) OVER (
                        PARTITION BY instrument_id
                        ORDER BY trade_date
                        ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
                    ) IS NOT NULL
                     AND STDDEV_SAMP(short_volume_ratio) OVER (
                        PARTITION BY instrument_id
                        ORDER BY trade_date
                        ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
                    ) <> 0
                    THEN (
                        short_volume_ratio
                        - AVG(short_volume_ratio) OVER (
                            PARTITION BY instrument_id
                            ORDER BY trade_date
                            ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
                        )
                    ) / STDDEV_SAMP(short_volume_ratio) OVER (
                        PARTITION BY instrument_id
                        ORDER BY trade_date
                        ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
                    )
                    ELSE NULL
                END AS short_pressure_zscore,
                CASE
                    WHEN COUNT(days_to_cover) OVER (
                        PARTITION BY instrument_id
                        ORDER BY trade_date
                        ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
                    ) >= 20
                     AND STDDEV_SAMP(days_to_cover) OVER (
                        PARTITION BY instrument_id
                        ORDER BY trade_date
                        ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
                    ) IS NOT NULL
                     AND STDDEV_SAMP(days_to_cover) OVER (
                        PARTITION BY instrument_id
                        ORDER BY trade_date
                        ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
                    ) <> 0
                    THEN (
                        days_to_cover
                        - AVG(days_to_cover) OVER (
                            PARTITION BY instrument_id
                            ORDER BY trade_date
                            ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
                        )
                    ) / STDDEV_SAMP(days_to_cover) OVER (
                        PARTITION BY instrument_id
                        ORDER BY trade_date
                        ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
                    )
                    ELSE NULL
                END AS days_to_cover_zscore,
                source_name,
                CURRENT_TIMESTAMP
            FROM tmp_short_interest_join
            """
        )
        progress.update(1)

        self._log_step(7, "collect metrics")
        short_feature_rows = int(
            con.execute("SELECT COUNT(*) FROM short_features_daily").fetchone()[0]
        )
        progress.update(1)
        progress.close()

        self._rows_written = short_feature_rows
        self._metrics = {
            "short_interest_history_rows": short_interest_rows,
            "daily_short_volume_raw_rows": short_volume_rows,
            "short_feature_rows": short_feature_rows,
        }

    def finalize(self, result: PipelineResult):
        result.rows_written = self._rows_written
        result.metrics.update(self._metrics)
        return result
