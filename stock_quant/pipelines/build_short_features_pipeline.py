from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from stock_quant.app.dto.pipeline_result import PipelineResult


class BuildShortFeaturesPipeline:
    """
    SQL-first pipeline avec normalisation des symboles.

    Ajout clé :
    - normalisation via table symbol_normalization
    - toujours PIT-safe
    """

    pipeline_name = "build_short_features"

    def __init__(self, con) -> None:
        self.con = con
        self._progress_total_steps = 3

    def _now(self) -> datetime:
        return datetime.now(timezone.utc)

    def _log(self, msg: str):
        print(f"[build_short_features] {msg}", flush=True)

    def run(self) -> PipelineResult:
        started_at = self._now()

        try:
            self._log("step 1/3: inspect short-data source tables")

            total = self.con.execute("SELECT COUNT(*) FROM daily_short_volume_history").fetchone()[0]

            if total == 0:
                return PipelineResult(
                    pipeline_name=self.pipeline_name,
                    status="noop",
                    started_at=started_at,
                    finished_at=self._now(),
                    rows_read=0,
                    rows_written=0,
                    metrics={"reason": "no_input_data"},
                )

            self._log("step 2/3: rebuild short_features_daily (with normalization)")

            self.con.execute("DELETE FROM short_features_daily")

            # 🔥 SQL-first avec normalisation
            self.con.execute("""
                INSERT INTO short_features_daily
                WITH

                norm_map AS (
                    SELECT raw_symbol, normalized_symbol
                    FROM symbol_normalization
                    WHERE is_active = TRUE
                ),

                daily AS (
                    SELECT
                        COALESCE(n.normalized_symbol, d.symbol) AS symbol,
                        d.trade_date AS as_of_date,
                        SUM(d.short_volume) AS short_volume,
                        SUM(d.short_exempt_volume) AS short_exempt_volume,
                        SUM(d.total_volume) AS total_volume,
                        MAX(d.available_at) AS max_source_available_at
                    FROM daily_short_volume_history d
                    LEFT JOIN norm_map n
                        ON d.symbol = n.raw_symbol
                    GROUP BY 1,2
                ),

                daily_enriched AS (
                    SELECT
                        *,
                        short_volume / NULLIF(total_volume,0) AS short_volume_ratio,
                        AVG(short_volume / NULLIF(total_volume,0)) OVER (
                            PARTITION BY symbol
                            ORDER BY as_of_date
                            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                        ) AS short_volume_ratio_20d_avg
                    FROM daily
                ),

                si AS (
                    SELECT
                        COALESCE(n.normalized_symbol, s.symbol) AS symbol,
                        s.*
                    FROM finra_short_interest_history s
                    LEFT JOIN norm_map n
                        ON s.symbol = n.raw_symbol
                ),

                joined AS (
                    SELECT
                        d.*,
                        s.short_interest,
                        s.previous_short_interest,
                        s.days_to_cover,
                        s.shares_float,
                        s.short_interest_pct_float,
                        ROW_NUMBER() OVER (
                            PARTITION BY d.symbol, d.as_of_date
                            ORDER BY s.settlement_date DESC, s.ingested_at DESC
                        ) AS rn
                    FROM daily_enriched d
                    LEFT JOIN si s
                      ON d.symbol = s.symbol
                     AND s.settlement_date <= d.as_of_date
                     AND s.ingested_at <= d.max_source_available_at
                )

                SELECT
                    symbol,
                    as_of_date,
                    short_volume,
                    short_exempt_volume,
                    total_volume,
                    short_volume_ratio,
                    short_volume_ratio_20d_avg,
                    short_interest,
                    previous_short_interest,
                    days_to_cover,
                    shares_float,
                    short_interest_pct_float,
                    max_source_available_at,
                    CURRENT_TIMESTAMP,
                    CURRENT_TIMESTAMP
                FROM joined
                WHERE rn = 1
            """)

            self._log("step 3/3: finalize metrics")

            count = self.con.execute("SELECT COUNT(*) FROM short_features_daily").fetchone()[0]

            return PipelineResult(
                pipeline_name=self.pipeline_name,
                status="success",
                started_at=started_at,
                finished_at=self._now(),
                rows_read=total,
                rows_written=count,
                metrics={
                    "rows_written": count
                },
            )

        except Exception as e:
            return PipelineResult(
                pipeline_name=self.pipeline_name,
                status="failed",
                started_at=started_at,
                finished_at=self._now(),
                rows_read=0,
                rows_written=0,
                error_message=str(e),
            )
