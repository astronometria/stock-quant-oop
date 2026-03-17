from __future__ import annotations

"""
Canonical SQL-first FINRA daily short volume pipeline.

Responsibilities
----------------
- read finra_daily_short_volume_source_raw
- normalize into daily_short_volume_history
- preserve PIT metadata
- remain idempotent
"""

from datetime import datetime, timezone
from typing import Any

from stock_quant.app.dto.pipeline_result import PipelineResult


class BuildDailyShortVolumePipeline:
    pipeline_name = "build_daily_short_volume"

    def __init__(self, con: Any) -> None:
        self.con = con
        self._progress_total_steps = 4

    def _now(self) -> datetime:
        return datetime.now(timezone.utc)

    def _log_step(self, step_no: int, label: str) -> None:
        print(f"[build_daily_short_volume] step {step_no}/{self._progress_total_steps}: {label}", flush=True)

    def _table_count(self, table_name: str) -> int:
        return int(self.con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0])

    def _max_date(self, table_name: str, column_name: str) -> str | None:
        value = self.con.execute(f"SELECT MAX({column_name}) FROM {table_name}").fetchone()[0]
        return None if value is None else str(value)

    def _build_result(
        self,
        *,
        status: str,
        started_at: datetime,
        finished_at: datetime,
        rows_read: int,
        rows_written: int,
        metrics: dict[str, Any],
        error_message: str | None,
    ) -> PipelineResult:
        try:
            return PipelineResult(
                pipeline_name=self.pipeline_name,
                status=status,
                started_at=started_at,
                finished_at=finished_at,
                rows_read=rows_read,
                rows_written=rows_written,
                rows_skipped=0,
                warnings=[],
                metrics=metrics,
                error_message=error_message,
            )
        except TypeError:
            return PipelineResult(
                pipeline_name=self.pipeline_name,
                status=status,
                started_at=started_at,
                finished_at=finished_at,
                rows_read=rows_read,
                rows_written=rows_written,
                metrics=metrics,
                error_message=error_message,
            )

    def run(self) -> PipelineResult:
        started_at = self._now()
        metrics: dict[str, Any] = {}

        try:
            self._log_step(1, "inspect raw and canonical state")
            raw_count = self._table_count("finra_daily_short_volume_source_raw")
            history_before = self._table_count("daily_short_volume_history")
            metrics["raw_row_count"] = raw_count
            metrics["history_rows_before"] = history_before
            metrics["max_raw_trade_date"] = self._max_date("finra_daily_short_volume_source_raw", "trade_date")
            metrics["max_history_trade_date_before"] = self._max_date("daily_short_volume_history", "trade_date")

            if raw_count == 0:
                return self._build_result(
                    status="noop",
                    started_at=started_at,
                    finished_at=self._now(),
                    rows_read=0,
                    rows_written=0,
                    metrics=metrics | {"build_decision": "noop_no_raw_rows"},
                    error_message=None,
                )

            self._log_step(2, "create temporary canonical source")
            self.con.execute("DROP TABLE IF EXISTS tmp_daily_short_volume_canonical_source")
            self.con.execute(
                """
                CREATE TEMP TABLE tmp_daily_short_volume_canonical_source AS
                WITH base AS (
                    SELECT
                        UPPER(TRIM(symbol)) AS symbol,
                        trade_date,
                        short_volume,
                        short_exempt_volume,
                        total_volume,
                        CASE
                            WHEN total_volume IS NULL OR total_volume = 0 THEN NULL
                            ELSE short_volume / total_volume
                        END AS short_volume_ratio,
                        market_code,
                        source_name,
                        source_file,
                        publication_date,
                        available_at,
                        ingested_at
                    FROM finra_daily_short_volume_source_raw
                    WHERE symbol IS NOT NULL
                      AND TRIM(symbol) <> ''
                      AND trade_date IS NOT NULL
                ),
                ranked AS (
                    SELECT
                        *,
                        ROW_NUMBER() OVER (
                            PARTITION BY symbol, trade_date, source_file
                            ORDER BY available_at DESC NULLS LAST, ingested_at DESC NULLS LAST
                        ) AS rn
                    FROM base
                )
                SELECT
                    NULL::VARCHAR AS instrument_id,
                    NULL::VARCHAR AS company_id,
                    symbol,
                    trade_date,
                    short_volume,
                    short_exempt_volume,
                    total_volume,
                    short_volume_ratio,
                    market_code,
                    source_name,
                    source_file,
                    publication_date,
                    available_at,
                    ingested_at
                FROM ranked
                WHERE rn = 1
                """
            )
            metrics["history_stage_rows"] = self._table_count("tmp_daily_short_volume_canonical_source")

            self._log_step(3, "upsert canonical daily short volume history")
            self.con.execute(
                """
                DELETE FROM daily_short_volume_history AS target
                USING tmp_daily_short_volume_canonical_source AS stage
                WHERE target.symbol = stage.symbol
                  AND target.trade_date = stage.trade_date
                  AND COALESCE(target.source_file, '') = COALESCE(stage.source_file, '')
                """
            )
            self.con.execute(
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
                    market_code,
                    source_name,
                    source_file,
                    publication_date,
                    available_at,
                    ingested_at
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
                    market_code,
                    source_name,
                    source_file,
                    publication_date,
                    available_at,
                    ingested_at
                FROM tmp_daily_short_volume_canonical_source
                """
            )

            self._log_step(4, "collect final canonical state")
            history_after = self._table_count("daily_short_volume_history")
            inserted = max(history_after - history_before, 0)
            metrics["history_rows_after"] = history_after
            metrics["history_rows_inserted"] = inserted
            metrics["max_history_trade_date_after"] = self._max_date("daily_short_volume_history", "trade_date")

            return self._build_result(
                status="success",
                started_at=started_at,
                finished_at=self._now(),
                rows_read=raw_count,
                rows_written=inserted,
                metrics=metrics,
                error_message=None,
            )

        except Exception as exc:
            return self._build_result(
                status="failed",
                started_at=started_at,
                finished_at=self._now(),
                rows_read=int(metrics.get("raw_row_count", 0)),
                rows_written=0,
                metrics=metrics,
                error_message=str(exc),
            )
