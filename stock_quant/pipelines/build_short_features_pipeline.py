from __future__ import annotations

"""
Canonical SQL-first short-features pipeline.

Inputs
------
- daily_short_volume_history
- finra_short_interest_history

Output
------
- short_features_daily
"""

from datetime import datetime, timezone
from typing import Any

from stock_quant.app.dto.pipeline_result import PipelineResult


class BuildShortFeaturesPipeline:
    pipeline_name = "build_short_features"

    def __init__(self, con: Any) -> None:
        self.con = con
        self._progress_total_steps = 3

    def _now(self) -> datetime:
        return datetime.now(timezone.utc)

    def _log_step(self, step_no: int, label: str) -> None:
        print(f"[build_short_features] step {step_no}/{self._progress_total_steps}: {label}", flush=True)

    def _count(self, table_name: str) -> int:
        return int(self.con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0])

    def _build_result(self, status: str, started_at: datetime, rows_read: int, rows_written: int, metrics: dict[str, Any], error_message: str | None) -> PipelineResult:
        finished_at = self._now()
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
            self._log_step(1, "inspect short-data source tables")
            dsv_count = self._count("daily_short_volume_history")
            si_count = self._count("finra_short_interest_history")
            metrics["daily_short_volume_history_count"] = dsv_count
            metrics["finra_short_interest_history_count"] = si_count

            if dsv_count == 0 or si_count == 0:
                return self._build_result(
                    "noop",
                    started_at,
                    rows_read=dsv_count + si_count,
                    rows_written=0,
                    metrics=metrics | {"build_decision": "noop_missing_inputs"},
                    error_message=None,
                )

            before_count = self._count("short_features_daily")
            self._log_step(2, "rebuild short_features_daily from canonical sources")
            self.con.execute("DELETE FROM short_features_daily")
            self.con.execute(
                """
                INSERT INTO short_features_daily (
                    instrument_id,
                    company_id,
                    symbol,
                    as_of_date,
                    short_volume,
                    short_exempt_volume,
                    total_volume,
                    short_volume_ratio,
                    short_interest,
                    previous_short_interest,
                    avg_daily_volume,
                    days_to_cover,
                    shares_float,
                    short_interest_pct_float,
                    short_interest_change,
                    short_interest_change_pct,
                    short_volume_20d_avg,
                    short_volume_ratio_20d_avg,
                    days_to_cover_zscore,
                    short_pressure_zscore,
                    short_squeeze_score,
                    max_source_available_at,
                    created_at,
                    updated_at
                )
                WITH dsv AS (
                    SELECT
                        instrument_id,
                        company_id,
                        symbol,
                        trade_date AS as_of_date,
                        short_volume,
                        short_exempt_volume,
                        total_volume,
                        short_volume_ratio,
                        available_at,
                        AVG(short_volume) OVER (
                            PARTITION BY symbol
                            ORDER BY trade_date
                            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                        ) AS short_volume_20d_avg,
                        AVG(short_volume_ratio) OVER (
                            PARTITION BY symbol
                            ORDER BY trade_date
                            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                        ) AS short_volume_ratio_20d_avg
                    FROM daily_short_volume_history
                ),
                si AS (
                    SELECT
                        symbol,
                        settlement_date,
                        short_interest,
                        previous_short_interest,
                        avg_daily_volume,
                        days_to_cover,
                        shares_float,
                        short_interest_pct_float,
                        available_at
                    FROM finra_short_interest_history
                ),
                joined AS (
                    SELECT
                        dsv.instrument_id,
                        dsv.company_id,
                        dsv.symbol,
                        dsv.as_of_date,
                        dsv.short_volume,
                        dsv.short_exempt_volume,
                        dsv.total_volume,
                        dsv.short_volume_ratio,
                        si.short_interest,
                        si.previous_short_interest,
                        si.avg_daily_volume,
                        si.days_to_cover,
                        si.shares_float,
                        si.short_interest_pct_float,
                        CASE
                            WHEN si.short_interest IS NULL OR si.previous_short_interest IS NULL THEN NULL
                            ELSE si.short_interest - si.previous_short_interest
                        END AS short_interest_change,
                        CASE
                            WHEN si.previous_short_interest IS NULL OR si.previous_short_interest = 0 THEN NULL
                            ELSE (si.short_interest - si.previous_short_interest) / si.previous_short_interest
                        END AS short_interest_change_pct,
                        dsv.short_volume_20d_avg,
                        dsv.short_volume_ratio_20d_avg,
                        NULL::DOUBLE AS days_to_cover_zscore,
                        NULL::DOUBLE AS short_pressure_zscore,
                        NULL::DOUBLE AS short_squeeze_score,
                        GREATEST(
                            COALESCE(dsv.available_at, TIMESTAMP '1900-01-01'),
                            COALESCE(si.available_at, TIMESTAMP '1900-01-01')
                        ) AS max_source_available_at,
                        CURRENT_TIMESTAMP AS created_at,
                        CURRENT_TIMESTAMP AS updated_at
                    FROM dsv
                    LEFT JOIN si
                      ON dsv.symbol = si.symbol
                     AND si.settlement_date <= dsv.as_of_date
                    QUALIFY ROW_NUMBER() OVER (
                        PARTITION BY dsv.symbol, dsv.as_of_date
                        ORDER BY si.settlement_date DESC NULLS LAST
                    ) = 1
                )
                SELECT * FROM joined
                """
            )

            self._log_step(3, "collect final short_features_daily state")
            after_count = self._count("short_features_daily")
            metrics["short_features_before"] = before_count
            metrics["short_features_after"] = after_count
            metrics["inserted_rows"] = after_count

            return self._build_result(
                "success",
                started_at,
                rows_read=dsv_count + si_count,
                rows_written=after_count,
                metrics=metrics,
                error_message=None,
            )

        except Exception as exc:
            return self._build_result(
                "failed",
                started_at,
                rows_read=int(metrics.get("daily_short_volume_history_count", 0)) + int(metrics.get("finra_short_interest_history_count", 0)),
                rows_written=0,
                metrics=metrics,
                error_message=str(exc),
            )
