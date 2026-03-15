from __future__ import annotations

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
            raise PipelineError("short_interest_history is empty — run build_finra_short_interest / canonical short history load first")

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
        # Canonical PIT fields on history tables
        self._ensure_column("short_interest_history", "publication_date", "DATE")
        self._ensure_column("short_interest_history", "available_at", "TIMESTAMP")

        self._ensure_column("daily_short_volume_history", "publication_date", "DATE")
        self._ensure_column("daily_short_volume_history", "available_at", "TIMESTAMP")
        self._ensure_column("daily_short_volume_history", "short_exempt_volume", "DOUBLE")

        # Feature fields
        self._ensure_column("short_features_daily", "short_exempt_volume", "DOUBLE")
        self._ensure_column("short_features_daily", "short_interest_change_pct", "DOUBLE")
        self._ensure_column("short_features_daily", "short_squeeze_score", "DOUBLE")
        self._ensure_column("short_features_daily", "short_pressure_zscore", "DOUBLE")
        self._ensure_column("short_features_daily", "days_to_cover_zscore", "DOUBLE")
        self._ensure_column("short_features_daily", "max_source_available_at", "TIMESTAMP")

        # Helpful indexes
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
                "SELECT COUNT(*) FROM short_interest_history WHERE instrument_id IS NOT NULL AND publication_date IS NULL"
            ).fetchone()[0]
        )
        before_missing_avail = int(
            con.execute(
                "SELECT COUNT(*) FROM short_interest_history WHERE instrument_id IS NOT NULL AND available_at IS NULL"
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
                "SELECT COUNT(*) FROM short_interest_history WHERE instrument_id IS NOT NULL AND publication_date IS NULL"
            ).fetchone()[0]
        )
        after_missing_avail = int(
            con.execute(
                "SELECT COUNT(*) FROM short_interest_history WHERE instrument_id IS NOT NULL AND available_at IS NULL"
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
            available_expr = f"COALESCE(CAST(r.available_at AS TIMESTAMP), CAST({publication_expr} AS TIMESTAMP))"

        return short_exempt_expr, publication_expr, available_expr

    def _upsert_missing_daily_short_volume_history(self) -> dict[str, int]:
        con = self.con
        short_exempt_expr, publication_expr, available_expr = self._source_exprs_for_daily_raw()

        before_count = int(
            con.execute("SELECT COUNT(*) FROM daily_short_volume_history").fetchone()[0]
        )

        con.execute("DROP TABLE IF EXISTS tmp_missing_daily_short_volume")
        con.execute(
            f"""
            CREATE TEMP TABLE tmp_missing_daily_short_volume AS
            WITH ranked AS (
                SELECT
                    m.instrument_id,
                    m.company_id,
                    COALESCE(m.symbol, UPPER(TRIM(r.symbol))) AS symbol,
                    CAST(r.trade_date AS DATE) AS trade_date,
                    CAST(r.short_volume AS DOUBLE) AS short_volume,
                    {short_exempt_expr} AS short_exempt_volume,
                    CAST(r.total_volume AS DOUBLE) AS total_volume,
                    CASE
                        WHEN r.total_volume IS NOT NULL AND r.total_volume <> 0
                             AND r.short_volume IS NOT NULL
                        THEN CAST(r.short_volume AS DOUBLE) / CAST(r.total_volume AS DOUBLE)
                        ELSE NULL
                    END AS short_volume_ratio,
                    {publication_expr} AS publication_date,
                    {available_expr} AS available_at,
                    COALESCE(r.source_name, 'finra_daily_non_otc') AS source_name,
                    ROW_NUMBER() OVER (
                        PARTITION BY UPPER(TRIM(r.symbol)), CAST(r.trade_date AS DATE)
                        ORDER BY m.valid_from DESC NULLS LAST
                    ) AS rn
                FROM finra_daily_short_volume_source_raw r
                LEFT JOIN tmp_ticker_map m
                  ON UPPER(TRIM(r.symbol)) = m.symbol_key
                 AND CAST(r.trade_date AS DATE) BETWEEN m.valid_from AND m.valid_to
                WHERE r.symbol IS NOT NULL
                  AND TRIM(r.symbol) <> ''
                  AND r.trade_date IS NOT NULL
            )
            SELECT
                r.instrument_id,
                r.company_id,
                r.symbol,
                r.trade_date,
                r.short_volume,
                r.short_exempt_volume,
                r.total_volume,
                r.short_volume_ratio,
                r.publication_date,
                r.available_at,
                r.source_name
            FROM ranked r
            LEFT JOIN daily_short_volume_history h
              ON h.instrument_id = r.instrument_id
             AND h.trade_date = r.trade_date
            WHERE r.rn = 1
              AND r.instrument_id IS NOT NULL
              AND h.instrument_id IS NULL
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
            FROM tmp_missing_daily_short_volume
            """
        )

        # Backfill PIT fields on already-existing canonical rows
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

        null_pub = int(
            con.execute(
                "SELECT COUNT(*) FROM daily_short_volume_history WHERE instrument_id IS NOT NULL AND publication_date IS NULL"
            ).fetchone()[0]
        )
        null_avail = int(
            con.execute(
                "SELECT COUNT(*) FROM daily_short_volume_history WHERE instrument_id IS NOT NULL AND available_at IS NULL"
            ).fetchone()[0]
        )

        return {
            "inserted_daily_short_volume_history": after_count - before_count,
            "remaining_daily_short_volume_history_null_publication_date": null_pub,
            "remaining_daily_short_volume_history_null_available_at": null_avail,
        }

    def _backfill_existing_short_features_metadata(self) -> dict[str, int]:
        con = self.con

        before_missing_max_available = int(
            con.execute(
                """
                SELECT COUNT(*)
                FROM short_features_daily
                WHERE instrument_id IS NOT NULL
                  AND max_source_available_at IS NULL
                """
            ).fetchone()[0]
        )

        con.execute(
            """
            UPDATE short_features_daily AS f
            SET
                short_exempt_volume = COALESCE(f.short_exempt_volume, d.short_exempt_volume),
                max_source_available_at = COALESCE(
                    f.max_source_available_at,
                    d.available_at,
                    CAST(d.trade_date AS TIMESTAMP)
                ),
                short_interest_change = COALESCE(
                    f.short_interest_change,
                    CASE
                        WHEN f.short_interest IS NOT NULL
                             AND f.short_interest_change IS NULL
                        THEN f.short_interest - (f.short_interest - COALESCE(f.short_interest_change, 0))
                        ELSE f.short_interest_change
                    END
                ),
                short_interest_change_pct = COALESCE(
                    f.short_interest_change_pct,
                    CASE
                        WHEN f.short_interest IS NOT NULL
                             AND f.short_interest_change IS NOT NULL
                             AND (f.short_interest - f.short_interest_change) IS NOT NULL
                             AND (f.short_interest - f.short_interest_change) <> 0
                        THEN f.short_interest_change / (f.short_interest - f.short_interest_change)
                        ELSE f.short_interest_change_pct
                    END
                ),
                short_squeeze_score = COALESCE(
                    f.short_squeeze_score,
                    CASE
                        WHEN f.days_to_cover IS NOT NULL AND f.short_volume_ratio IS NOT NULL
                        THEN (0.7 * f.days_to_cover) + (0.3 * (100.0 * f.short_volume_ratio))
                        ELSE NULL
                    END
                )
            FROM daily_short_volume_history AS d
            WHERE f.instrument_id = d.instrument_id
              AND f.as_of_date = d.trade_date
              AND f.instrument_id IS NOT NULL
              AND (
                    f.max_source_available_at IS NULL
                 OR f.short_exempt_volume IS NULL
                 OR f.short_squeeze_score IS NULL
                 OR f.short_interest_change_pct IS NULL
              )
            """
        )

        after_missing_max_available = int(
            con.execute(
                """
                SELECT COUNT(*)
                FROM short_features_daily
                WHERE instrument_id IS NOT NULL
                  AND max_source_available_at IS NULL
                """
            ).fetchone()[0]
        )

        return {
            "backfilled_short_features_max_source_available_at": before_missing_max_available - after_missing_max_available,
            "remaining_short_features_null_max_source_available_at": after_missing_max_available,
        }

    def _insert_missing_short_features(self) -> dict[str, int]:
        con = self.con

        before_count = int(
            con.execute("SELECT COUNT(*) FROM short_features_daily").fetchone()[0]
        )

        con.execute("DROP TABLE IF EXISTS tmp_missing_feature_keys")
        con.execute(
            """
            CREATE TEMP TABLE tmp_missing_feature_keys AS
            SELECT
                d.instrument_id,
                d.company_id,
                d.symbol,
                d.trade_date,
                d.short_volume,
                d.short_exempt_volume,
                d.total_volume,
                d.short_volume_ratio,
                d.available_at,
                d.source_name
            FROM daily_short_volume_history d
            LEFT JOIN short_features_daily f
              ON f.instrument_id = d.instrument_id
             AND f.as_of_date = d.trade_date
            WHERE d.instrument_id IS NOT NULL
              AND f.instrument_id IS NULL
            """
        )

        con.execute("DROP TABLE IF EXISTS tmp_missing_feature_join")
        con.execute(
            """
            CREATE TEMP TABLE tmp_missing_feature_join AS
            WITH matched AS (
                SELECT
                    d.instrument_id,
                    d.company_id,
                    d.symbol,
                    d.trade_date AS as_of_date,
                    d.short_volume,
                    d.short_exempt_volume,
                    d.total_volume,
                    d.short_volume_ratio,
                    d.available_at AS volume_available_at,
                    d.source_name,
                    s.short_interest,
                    s.previous_short_interest,
                    s.avg_daily_volume,
                    s.days_to_cover,
                    s.available_at AS short_available_at,
                    ROW_NUMBER() OVER (
                        PARTITION BY d.instrument_id, d.trade_date
                        ORDER BY s.settlement_date DESC NULLS LAST
                    ) AS rn
                FROM tmp_missing_feature_keys d
                LEFT JOIN short_interest_history s
                  ON s.instrument_id = d.instrument_id
                 AND s.settlement_date <= d.trade_date
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
                CASE
                    WHEN short_interest IS NOT NULL AND previous_short_interest IS NOT NULL
                    THEN short_interest - previous_short_interest
                    ELSE NULL
                END AS short_interest_change,
                CASE
                    WHEN short_interest IS NOT NULL
                         AND previous_short_interest IS NOT NULL
                         AND previous_short_interest <> 0
                    THEN (short_interest - previous_short_interest) / previous_short_interest
                    ELSE NULL
                END AS short_interest_change_pct,
                CASE
                    WHEN days_to_cover IS NOT NULL AND short_volume_ratio IS NOT NULL
                    THEN (0.7 * days_to_cover) + (0.3 * (100.0 * short_volume_ratio))
                    ELSE NULL
                END AS short_squeeze_score,
                CAST(NULL AS DOUBLE) AS short_pressure_zscore,
                CAST(NULL AS DOUBLE) AS days_to_cover_zscore,
                COALESCE(short_available_at, volume_available_at, CAST(as_of_date AS TIMESTAMP)) AS max_source_available_at,
                source_name
            FROM matched
            WHERE rn = 1
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
            FROM tmp_missing_feature_join
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

        self._log_step(4, "build ticker map and upsert missing daily short volume history")
        self._build_ticker_map_temp()
        self._metrics.update(self._upsert_missing_daily_short_volume_history())
        progress.update(1)

        self._log_step(5, "backfill existing short feature metadata")
        self._metrics.update(self._backfill_existing_short_features_metadata())
        progress.update(1)

        self._log_step(6, "insert missing short feature rows")
        self._metrics.update(self._insert_missing_short_features())
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
        progress.close()

        self._rows_written = (
            int(self._metrics.get("inserted_daily_short_volume_history", 0))
            + int(self._metrics.get("inserted_short_features_daily", 0))
        )

    def finalize(self, result: PipelineResult) -> PipelineResult:
        result.rows_read = int(
            self.con.execute("SELECT COUNT(*) FROM finra_daily_short_volume_source_raw").fetchone()[0]
        )
        result.rows_written = self._rows_written
        result.metrics.update(self._metrics)
        return result
