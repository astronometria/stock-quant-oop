from __future__ import annotations

import os

from stock_quant.app.dto.pipeline_result import PipelineResult
from stock_quant.infrastructure.db.unit_of_work import DuckDbUnitOfWork
from stock_quant.pipelines.base_pipeline import BasePipeline
from stock_quant.shared.exceptions import PipelineError

try:
    from tqdm import tqdm
except Exception:
    def tqdm(iterable=None, **kwargs):
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

    def validate(self, data) -> None:
        short_interest_rows = int(
            self.con.execute("SELECT COUNT(*) FROM short_interest_history").fetchone()[0]
        )
        short_volume_raw_rows = int(
            self.con.execute("SELECT COUNT(*) FROM finra_daily_short_volume_source_raw").fetchone()[0]
        )

        if short_interest_rows == 0:
            raise PipelineError(
                "short_interest_history is empty — run build_finra_short_interest / canonical short history load first"
            )
        if short_volume_raw_rows == 0:
            raise PipelineError("finra_daily_short_volume_source_raw is empty")

    def _log_step(self, step_no: int, label: str) -> None:
        print(
            f"[build_short_data] step {step_no}/{self._progress_total_steps}: {label}",
            flush=True,
        )

    def _table_columns(self, table_name: str) -> set[str]:
        rows = self.con.execute(f"PRAGMA table_info('{table_name}')").fetchall()
        return {str(row[1]).strip() for row in rows}

    def _ensure_column(self, table_name: str, column_name: str, sql_type: str) -> None:
        columns = self._table_columns(table_name)
        if column_name not in columns:
            self.con.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {sql_type}")

    def _ensure_schema(self) -> None:
        cpu_threads = max(1, min((os.cpu_count() or 8), 16))
        self.con.execute(f"PRAGMA threads={cpu_threads}")

        self._ensure_column("short_interest_history", "publication_date", "DATE")
        self._ensure_column("short_interest_history", "available_at", "TIMESTAMP")

        self._ensure_column("daily_short_volume_history", "publication_date", "DATE")
        self._ensure_column("daily_short_volume_history", "available_at", "TIMESTAMP")
        self._ensure_column("daily_short_volume_history", "short_exempt_volume", "DOUBLE")

        self._ensure_column("short_features_daily", "short_exempt_volume", "DOUBLE")
        self._ensure_column("short_features_daily", "short_interest_change_pct", "DOUBLE")
        self._ensure_column("short_features_daily", "short_squeeze_score", "DOUBLE")
        self._ensure_column("short_features_daily", "short_pressure_zscore", "DOUBLE")
        self._ensure_column("short_features_daily", "days_to_cover_zscore", "DOUBLE")
        self._ensure_column("short_features_daily", "max_source_available_at", "TIMESTAMP")

        self.con.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_daily_short_volume_history_instrument_trade_date
            ON daily_short_volume_history(instrument_id, trade_date)
            """
        )
        self.con.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_short_features_daily_instrument_asof
            ON short_features_daily(instrument_id, as_of_date)
            """
        )
        self.con.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_short_interest_history_instrument_settlement
            ON short_interest_history(instrument_id, settlement_date)
            """
        )

    def _build_ticker_map_temp(self) -> None:
        self.con.execute("DROP TABLE IF EXISTS tmp_ticker_map")
        self.con.execute(
            """
            CREATE TEMP TABLE tmp_ticker_map AS
            SELECT
                UPPER(TRIM(th.symbol)) AS symbol_key,
                th.symbol,
                th.instrument_id,
                im.company_id,
                th.valid_from,
                COALESCE(th.valid_to, DATE '9999-12-31') AS valid_to
            FROM ticker_history th
            LEFT JOIN instrument_master im
              ON th.instrument_id = im.instrument_id
            WHERE th.symbol IS NOT NULL
              AND TRIM(th.symbol) <> ''
              AND th.instrument_id IS NOT NULL
            """
        )

    def _cleanup_orphan_rows(self) -> dict[str, int]:
        con = self.con

        before_daily_null = int(
            con.execute(
                "SELECT COUNT(*) FROM daily_short_volume_history WHERE instrument_id IS NULL"
            ).fetchone()[0]
        )
        before_features_null = int(
            con.execute(
                "SELECT COUNT(*) FROM short_features_daily WHERE instrument_id IS NULL"
            ).fetchone()[0]
        )

        con.execute("DELETE FROM daily_short_volume_history WHERE instrument_id IS NULL")
        con.execute("DELETE FROM short_features_daily WHERE instrument_id IS NULL")

        after_daily_null = int(
            con.execute(
                "SELECT COUNT(*) FROM daily_short_volume_history WHERE instrument_id IS NULL"
            ).fetchone()[0]
        )
        after_features_null = int(
            con.execute(
                "SELECT COUNT(*) FROM short_features_daily WHERE instrument_id IS NULL"
            ).fetchone()[0]
        )

        return {
            "deleted_daily_short_volume_history_orphans": before_daily_null - after_daily_null,
            "deleted_short_features_daily_orphans": before_features_null - after_features_null,
        }

    def _backfill_short_interest_pit(self) -> dict[str, int]:
        con = self.con

        before_missing_pub = int(
            con.execute(
                """
                SELECT COUNT(*)
                FROM short_interest_history
                WHERE instrument_id IS NOT NULL
                  AND publication_date IS NULL
                """
            ).fetchone()[0]
        )
        before_missing_avail = int(
            con.execute(
                """
                SELECT COUNT(*)
                FROM short_interest_history
                WHERE instrument_id IS NOT NULL
                  AND available_at IS NULL
                """
            ).fetchone()[0]
        )

        con.execute(
            """
            UPDATE short_interest_history
            SET publication_date = COALESCE(publication_date, settlement_date)
            WHERE instrument_id IS NOT NULL
              AND publication_date IS NULL
            """
        )
        con.execute(
            """
            UPDATE short_interest_history
            SET available_at = COALESCE(
                available_at,
                CAST(COALESCE(publication_date, settlement_date) AS TIMESTAMP)
            )
            WHERE instrument_id IS NOT NULL
              AND available_at IS NULL
            """
        )

        after_missing_pub = int(
            con.execute(
                """
                SELECT COUNT(*)
                FROM short_interest_history
                WHERE instrument_id IS NOT NULL
                  AND publication_date IS NULL
                """
            ).fetchone()[0]
        )
        after_missing_avail = int(
            con.execute(
                """
                SELECT COUNT(*)
                FROM short_interest_history
                WHERE instrument_id IS NOT NULL
                  AND available_at IS NULL
                """
            ).fetchone()[0]
        )

        return {
            "backfilled_short_interest_publication_date": before_missing_pub - after_missing_pub,
            "backfilled_short_interest_available_at": before_missing_avail - after_missing_avail,
        }

    def _source_exprs_for_daily_raw(self) -> tuple[str, str, str]:
        raw_cols = self._table_columns("finra_daily_short_volume_source_raw")

        short_exempt_expr = "CAST(NULL AS DOUBLE)"
        if "short_exempt_volume" in raw_cols:
            short_exempt_expr = "CAST(r.short_exempt_volume AS DOUBLE)"

        publication_expr = "CAST(r.trade_date AS DATE)"
        if "publication_date" in raw_cols:
            publication_expr = "CAST(COALESCE(r.publication_date, r.trade_date) AS DATE)"

        available_expr = f"CAST({publication_expr} AS TIMESTAMP)"
        if "available_at" in raw_cols:
            available_expr = (
                f"COALESCE(CAST(r.available_at AS TIMESTAMP), CAST({publication_expr} AS TIMESTAMP))"
            )

        return short_exempt_expr, publication_expr, available_expr

    def _upsert_canonical_daily_short_volume_history(self) -> dict[str, int]:
        con = self.con
        short_exempt_expr, publication_expr, available_expr = self._source_exprs_for_daily_raw()

        before_count = int(
            con.execute("SELECT COUNT(*) FROM daily_short_volume_history").fetchone()[0]
        )

        con.execute("DROP TABLE IF EXISTS tmp_daily_short_volume_canonical_source")
        con.execute(
            f"""
            CREATE TEMP TABLE tmp_daily_short_volume_canonical_source AS
            WITH mapped AS (
                SELECT
                    m.instrument_id,
                    m.company_id,
                    COALESCE(m.symbol, UPPER(TRIM(r.symbol))) AS symbol,
                    CAST(r.trade_date AS DATE) AS trade_date,
                    CAST(r.short_volume AS DOUBLE) AS short_volume,
                    {short_exempt_expr} AS short_exempt_volume,
                    CAST(r.total_volume AS DOUBLE) AS total_volume,
                    {publication_expr} AS publication_date,
                    {available_expr} AS available_at,
                    COALESCE(r.source_name, 'finra_daily_non_otc') AS source_name,
                    COALESCE(r.source_file, '') AS source_file,
                    COALESCE(r.market_code, '') AS market_code,
                    ROW_NUMBER() OVER (
                        PARTITION BY
                            UPPER(TRIM(r.symbol)),
                            CAST(r.trade_date AS DATE),
                            COALESCE(r.source_file, ''),
                            COALESCE(r.market_code, ''),
                            COALESCE(r.source_name, '')
                        ORDER BY m.valid_from DESC NULLS LAST
                    ) AS rn
                FROM finra_daily_short_volume_source_raw r
                LEFT JOIN tmp_ticker_map m
                  ON UPPER(TRIM(r.symbol)) = m.symbol_key
                 AND CAST(r.trade_date AS DATE) BETWEEN m.valid_from AND m.valid_to
                WHERE r.symbol IS NOT NULL
                  AND TRIM(r.symbol) <> ''
                  AND r.trade_date IS NOT NULL
            ),
            dedup AS (
                SELECT
                    instrument_id,
                    company_id,
                    symbol,
                    trade_date,
                    short_volume,
                    short_exempt_volume,
                    total_volume,
                    publication_date,
                    available_at,
                    source_name
                FROM mapped
                WHERE rn = 1
                  AND instrument_id IS NOT NULL
            )
            SELECT
                instrument_id,
                MAX(company_id) AS company_id,
                MAX(symbol) AS symbol,
                trade_date,
                SUM(short_volume) AS short_volume,
                CASE
                    WHEN COUNT(short_exempt_volume) > 0 THEN SUM(COALESCE(short_exempt_volume, 0))
                    ELSE NULL
                END AS short_exempt_volume,
                SUM(total_volume) AS total_volume,
                CASE
                    WHEN SUM(total_volume) IS NOT NULL AND SUM(total_volume) <> 0
                    THEN SUM(short_volume) / SUM(total_volume)
                    ELSE NULL
                END AS short_volume_ratio,
                MIN(publication_date) AS publication_date,
                MAX(available_at) AS available_at,
                MAX(source_name) AS source_name
            FROM dedup
            GROUP BY instrument_id, trade_date
            """
        )

        con.execute("DROP TABLE IF EXISTS tmp_daily_short_volume_missing")
        con.execute(
            """
            CREATE TEMP TABLE tmp_daily_short_volume_missing AS
            SELECT s.*
            FROM tmp_daily_short_volume_canonical_source s
            LEFT JOIN daily_short_volume_history h
              ON h.instrument_id = s.instrument_id
             AND h.trade_date = s.trade_date
            WHERE h.instrument_id IS NULL
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
                short_exempt_volume,
                total_volume,
                short_volume_ratio,
                publication_date,
                available_at,
                source_name,
                created_at
            )
            SELECT
                instrument_id,
                company_id,
                symbol,
                trade_date,
                short_volume,
                short_exempt_volume,
                total_volume,
                short_volume_ratio,
                publication_date,
                available_at,
                source_name,
                CURRENT_TIMESTAMP
            FROM tmp_daily_short_volume_missing
            """
        )

        before_null_short_exempt = int(
            con.execute(
                """
                SELECT COUNT(*)
                FROM daily_short_volume_history
                WHERE instrument_id IS NOT NULL
                  AND short_exempt_volume IS NULL
                """
            ).fetchone()[0]
        )

        con.execute("DROP TABLE IF EXISTS tmp_daily_short_volume_updates")
        con.execute(
            """
            CREATE TEMP TABLE tmp_daily_short_volume_updates AS
            SELECT
                h.instrument_id,
                h.trade_date,
                s.company_id,
                s.symbol,
                s.short_volume,
                s.short_exempt_volume,
                s.total_volume,
                s.short_volume_ratio,
                s.publication_date,
                s.available_at,
                s.source_name
            FROM daily_short_volume_history h
            INNER JOIN tmp_daily_short_volume_canonical_source s
              ON h.instrument_id = s.instrument_id
             AND h.trade_date = s.trade_date
            WHERE
                h.company_id IS NULL
                OR h.symbol IS NULL
                OR h.publication_date IS NULL
                OR h.available_at IS NULL
                OR (
                    h.short_exempt_volume IS NULL
                    AND s.short_exempt_volume IS NOT NULL
                )
            """
        )

        con.execute(
            """
            UPDATE daily_short_volume_history AS h
            SET
                company_id = COALESCE(h.company_id, u.company_id),
                symbol = COALESCE(h.symbol, u.symbol),
                publication_date = COALESCE(h.publication_date, u.publication_date),
                available_at = COALESCE(h.available_at, u.available_at),
                short_exempt_volume = COALESCE(h.short_exempt_volume, u.short_exempt_volume),
                source_name = COALESCE(h.source_name, u.source_name)
            FROM tmp_daily_short_volume_updates u
            WHERE h.instrument_id = u.instrument_id
              AND h.trade_date = u.trade_date
            """
        )

        con.execute(
            """
            UPDATE daily_short_volume_history
            SET publication_date = COALESCE(publication_date, trade_date)
            WHERE instrument_id IS NOT NULL
              AND publication_date IS NULL
            """
        )
        con.execute(
            """
            UPDATE daily_short_volume_history
            SET available_at = COALESCE(
                available_at,
                CAST(COALESCE(publication_date, trade_date) AS TIMESTAMP)
            )
            WHERE instrument_id IS NOT NULL
              AND available_at IS NULL
            """
        )

        after_count = int(
            con.execute("SELECT COUNT(*) FROM daily_short_volume_history").fetchone()[0]
        )
        after_null_short_exempt = int(
            con.execute(
                """
                SELECT COUNT(*)
                FROM daily_short_volume_history
                WHERE instrument_id IS NOT NULL
                  AND short_exempt_volume IS NULL
                """
            ).fetchone()[0]
        )
        null_pub = int(
            con.execute(
                """
                SELECT COUNT(*)
                FROM daily_short_volume_history
                WHERE instrument_id IS NOT NULL
                  AND publication_date IS NULL
                """
            ).fetchone()[0]
        )
        null_avail = int(
            con.execute(
                """
                SELECT COUNT(*)
                FROM daily_short_volume_history
                WHERE instrument_id IS NOT NULL
                  AND available_at IS NULL
                """
            ).fetchone()[0]
        )

        return {
            "inserted_daily_short_volume_history": after_count - before_count,
            "backfilled_daily_short_volume_history_short_exempt_volume": (
                before_null_short_exempt - after_null_short_exempt
            ),
            "remaining_daily_short_volume_history_null_short_exempt_volume": after_null_short_exempt,
            "remaining_daily_short_volume_history_null_publication_date": null_pub,
            "remaining_daily_short_volume_history_null_available_at": null_avail,
        }

    def _backfill_existing_short_feature_metadata(self) -> dict[str, int]:
        con = self.con

        before_null_available = int(
            con.execute(
                """
                SELECT COUNT(*)
                FROM short_features_daily
                WHERE instrument_id IS NOT NULL
                  AND max_source_available_at IS NULL
                """
            ).fetchone()[0]
        )
        before_null_short_exempt = int(
            con.execute(
                """
                SELECT COUNT(*)
                FROM short_features_daily
                WHERE instrument_id IS NOT NULL
                  AND short_exempt_volume IS NULL
                """
            ).fetchone()[0]
        )

        con.execute("DROP TABLE IF EXISTS tmp_short_feature_updates")
        con.execute(
            """
            CREATE TEMP TABLE tmp_short_feature_updates AS
            SELECT
                f.instrument_id,
                f.as_of_date,
                d.company_id,
                d.symbol,
                d.short_volume,
                d.short_exempt_volume,
                d.total_volume,
                d.short_volume_ratio,
                d.available_at
            FROM short_features_daily AS f
            INNER JOIN daily_short_volume_history AS d
              ON f.instrument_id = d.instrument_id
             AND f.as_of_date = d.trade_date
            WHERE
                f.company_id IS NULL
                OR f.symbol IS NULL
                OR f.short_volume IS NULL
                OR f.total_volume IS NULL
                OR f.short_volume_ratio IS NULL
                OR f.max_source_available_at IS NULL
                OR (
                    f.short_exempt_volume IS NULL
                    AND d.short_exempt_volume IS NOT NULL
                )
            """
        )

        con.execute(
            """
            UPDATE short_features_daily AS f
            SET
                company_id = COALESCE(f.company_id, u.company_id),
                symbol = COALESCE(f.symbol, u.symbol),
                short_volume = COALESCE(f.short_volume, u.short_volume),
                short_exempt_volume = COALESCE(f.short_exempt_volume, u.short_exempt_volume),
                total_volume = COALESCE(f.total_volume, u.total_volume),
                short_volume_ratio = COALESCE(f.short_volume_ratio, u.short_volume_ratio),
                max_source_available_at = COALESCE(f.max_source_available_at, u.available_at)
            FROM tmp_short_feature_updates u
            WHERE f.instrument_id = u.instrument_id
              AND f.as_of_date = u.as_of_date
            """
        )

        after_null_available = int(
            con.execute(
                """
                SELECT COUNT(*)
                FROM short_features_daily
                WHERE instrument_id IS NOT NULL
                  AND max_source_available_at IS NULL
                """
            ).fetchone()[0]
        )
        after_null_short_exempt = int(
            con.execute(
                """
                SELECT COUNT(*)
                FROM short_features_daily
                WHERE instrument_id IS NOT NULL
                  AND short_exempt_volume IS NULL
                """
            ).fetchone()[0]
        )

        remaining_null_short_pressure = int(
            con.execute(
                """
                SELECT COUNT(*)
                FROM short_features_daily
                WHERE instrument_id IS NOT NULL
                  AND short_pressure_zscore IS NULL
                """
            ).fetchone()[0]
        )
        remaining_null_days_to_cover_zscore = int(
            con.execute(
                """
                SELECT COUNT(*)
                FROM short_features_daily
                WHERE instrument_id IS NOT NULL
                  AND days_to_cover_zscore IS NULL
                """
            ).fetchone()[0]
        )

        return {
            "backfilled_short_features_max_source_available_at": (
                before_null_available - after_null_available
            ),
            "backfilled_short_features_short_exempt_volume": (
                before_null_short_exempt - after_null_short_exempt
            ),
            "remaining_short_features_null_max_source_available_at": after_null_available,
            "remaining_short_features_null_short_exempt_volume": after_null_short_exempt,
            "remaining_short_features_null_short_pressure_zscore": remaining_null_short_pressure,
            "remaining_short_features_null_days_to_cover_zscore": remaining_null_days_to_cover_zscore,
        }

    def _insert_missing_short_feature_rows(self) -> dict[str, int]:
        con = self.con
        before_count = int(
            con.execute("SELECT COUNT(*) FROM short_features_daily").fetchone()[0]
        )

        con.execute("DROP TABLE IF EXISTS tmp_short_interest_match_dates")
        con.execute(
            """
            CREATE TEMP TABLE tmp_short_interest_match_dates AS
            SELECT
                d.instrument_id,
                d.trade_date,
                MAX(s.settlement_date) AS settlement_date
            FROM daily_short_volume_history d
            LEFT JOIN short_interest_history s
              ON d.instrument_id = s.instrument_id
             AND s.settlement_date <= d.trade_date
            GROUP BY d.instrument_id, d.trade_date
            """
        )

        con.execute("DROP TABLE IF EXISTS tmp_missing_short_features_base")
        con.execute(
            """
            CREATE TEMP TABLE tmp_missing_short_features_base AS
            SELECT
                d.instrument_id,
                d.company_id,
                d.symbol,
                d.trade_date AS as_of_date,
                s.short_interest,
                s.avg_daily_volume,
                s.days_to_cover,
                d.short_volume,
                d.short_exempt_volume,
                d.total_volume,
                d.short_volume_ratio,
                CASE
                    WHEN s.short_interest IS NOT NULL
                     AND s.previous_short_interest IS NOT NULL
                    THEN s.short_interest - s.previous_short_interest
                    ELSE NULL
                END AS short_interest_change,
                CASE
                    WHEN s.short_interest IS NOT NULL
                     AND s.previous_short_interest IS NOT NULL
                     AND s.previous_short_interest <> 0
                    THEN (s.short_interest - s.previous_short_interest) / s.previous_short_interest
                    ELSE NULL
                END AS short_interest_change_pct,
                CASE
                    WHEN s.days_to_cover IS NOT NULL
                     AND d.short_volume_ratio IS NOT NULL
                    THEN (0.7 * s.days_to_cover) + (0.3 * (100.0 * d.short_volume_ratio))
                    ELSE NULL
                END AS short_squeeze_score,
                d.available_at AS max_source_available_at,
                d.source_name
            FROM daily_short_volume_history d
            LEFT JOIN tmp_short_interest_match_dates m
              ON d.instrument_id = m.instrument_id
             AND d.trade_date = m.trade_date
            LEFT JOIN short_interest_history s
              ON d.instrument_id = s.instrument_id
             AND s.settlement_date = m.settlement_date
            LEFT JOIN short_features_daily f
              ON d.instrument_id = f.instrument_id
             AND d.trade_date = f.as_of_date
            WHERE f.instrument_id IS NULL
            """
        )

        con.execute("DROP TABLE IF EXISTS tmp_missing_short_features_scored")
        con.execute(
            """
            CREATE TEMP TABLE tmp_missing_short_features_scored AS
            SELECT
                instrument_id,
                company_id,
                symbol,
                as_of_date,
                short_interest,
                avg_daily_volume,
                days_to_cover,
                short_volume,
                short_exempt_volume,
                total_volume,
                short_volume_ratio,
                short_interest_change,
                short_interest_change_pct,
                short_squeeze_score,
                CASE
                    WHEN COUNT(short_volume_ratio) OVER (
                        PARTITION BY instrument_id
                        ORDER BY as_of_date
                        ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
                    ) >= 20
                     AND STDDEV_SAMP(short_volume_ratio) OVER (
                        PARTITION BY instrument_id
                        ORDER BY as_of_date
                        ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
                    ) IS NOT NULL
                     AND STDDEV_SAMP(short_volume_ratio) OVER (
                        PARTITION BY instrument_id
                        ORDER BY as_of_date
                        ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
                    ) <> 0
                    THEN (
                        short_volume_ratio - AVG(short_volume_ratio) OVER (
                            PARTITION BY instrument_id
                            ORDER BY as_of_date
                            ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
                        )
                    ) / STDDEV_SAMP(short_volume_ratio) OVER (
                        PARTITION BY instrument_id
                        ORDER BY as_of_date
                        ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
                    )
                    ELSE NULL
                END AS short_pressure_zscore,
                CASE
                    WHEN COUNT(days_to_cover) OVER (
                        PARTITION BY instrument_id
                        ORDER BY as_of_date
                        ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
                    ) >= 20
                     AND STDDEV_SAMP(days_to_cover) OVER (
                        PARTITION BY instrument_id
                        ORDER BY as_of_date
                        ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
                    ) IS NOT NULL
                     AND STDDEV_SAMP(days_to_cover) OVER (
                        PARTITION BY instrument_id
                        ORDER BY as_of_date
                        ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
                    ) <> 0
                    THEN (
                        days_to_cover - AVG(days_to_cover) OVER (
                            PARTITION BY instrument_id
                            ORDER BY as_of_date
                            ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
                        )
                    ) / STDDEV_SAMP(days_to_cover) OVER (
                        PARTITION BY instrument_id
                        ORDER BY as_of_date
                        ROWS BETWEEN 59 PRECEDING AND CURRENT ROW
                    )
                    ELSE NULL
                END AS days_to_cover_zscore,
                max_source_available_at,
                source_name
            FROM tmp_missing_short_features_base
            """
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
                short_exempt_volume,
                total_volume,
                short_volume_ratio,
                short_interest_change,
                short_interest_change_pct,
                short_squeeze_score,
                short_pressure_zscore,
                days_to_cover_zscore,
                max_source_available_at,
                source_name,
                created_at
            )
            SELECT
                instrument_id,
                company_id,
                symbol,
                as_of_date,
                short_interest,
                avg_daily_volume,
                days_to_cover,
                short_volume,
                short_exempt_volume,
                total_volume,
                short_volume_ratio,
                short_interest_change,
                short_interest_change_pct,
                short_squeeze_score,
                short_pressure_zscore,
                days_to_cover_zscore,
                max_source_available_at,
                source_name,
                CURRENT_TIMESTAMP
            FROM tmp_missing_short_features_scored
            """
        )

        after_count = int(
            con.execute("SELECT COUNT(*) FROM short_features_daily").fetchone()[0]
        )

        return {
            "inserted_short_features_daily": after_count - before_count,
        }

    def load(self, data) -> None:
        con = self.con

        self._metrics["rows_read_raw"] = int(
            con.execute("SELECT COUNT(*) FROM finra_daily_short_volume_source_raw").fetchone()[0]
        )

        progress = tqdm(
            total=self._progress_total_steps,
            desc="build_short_data",
            unit="step",
            dynamic_ncols=True,
            mininterval=0.2,
        )

        self._log_step(1, "ensure schema")
        self._ensure_schema()
        progress.update(1)

        self._log_step(2, "cleanup orphan canonical rows")
        self._metrics.update(self._cleanup_orphan_rows())
        progress.update(1)

        self._log_step(3, "backfill short-interest PIT fields")
        self._metrics.update(self._backfill_short_interest_pit())
        progress.update(1)

        self._log_step(4, "build ticker map and upsert canonical daily short volume history")
        self._build_ticker_map_temp()
        self._metrics.update(self._upsert_canonical_daily_short_volume_history())
        progress.update(1)

        self._log_step(5, "backfill existing short feature metadata")
        self._metrics.update(self._backfill_existing_short_feature_metadata())
        progress.update(1)

        self._log_step(6, "insert missing short feature rows")
        self._metrics.update(self._insert_missing_short_feature_rows())
        progress.update(1)

        self._log_step(7, "collect final counts")
        self._metrics["short_interest_history_rows_after"] = int(
            con.execute("SELECT COUNT(*) FROM short_interest_history").fetchone()[0]
        )
        self._metrics["daily_short_volume_history_rows_after"] = int(
            con.execute("SELECT COUNT(*) FROM daily_short_volume_history").fetchone()[0]
        )
        self._metrics["short_features_daily_rows_after"] = int(
            con.execute("SELECT COUNT(*) FROM short_features_daily").fetchone()[0]
        )
        progress.update(1)

        self._rows_written = (
            int(self._metrics.get("inserted_daily_short_volume_history", 0))
            + int(self._metrics.get("inserted_short_features_daily", 0))
        )

    def finalize(self, result: PipelineResult) -> PipelineResult:
        result.rows_read = int(self._metrics.get("rows_read_raw", 0))
        result.rows_written = self._rows_written
        result.metrics.update(self._metrics)
        return result
