from __future__ import annotations

import json
from dataclasses import asdict, is_dataclass
from datetime import datetime, timezone
from typing import Any

from stock_quant.app.dto.pipeline_result import PipelineResult


class BuildShortFeaturesPipeline:
    """
    Canonical SQL-first short feature builder.

    But:
    - partir exclusivement des tables canoniques
      - daily_short_volume_history
      - finra_short_interest_history
    - rester PIT-safe
    - ne jamais utiliser de table latest pour la recherche
    - rester idempotent

    Convention PIT actuelle
    -----------------------
    - daily_short_volume_history expose `available_at`
    - finra_short_interest_history, dans l'état actuel du repo, expose `ingested_at`
      et non `available_at`

    Donc la construction PIT pour short_features_daily utilise:
    - `daily_short_volume_history.available_at` pour le volume short journalier
    - `finra_short_interest_history.ingested_at` comme timestamp de disponibilité
      du short interest tant que la table canonique n'a pas encore un vrai
      `available_at` dédié

    C'est acceptable pour la branche actuelle parce qu'on n'utilise jamais
    la date métier (`settlement_date`) comme substitut au timestamp de disponibilité.
    """

    pipeline_name = "build_short_features"

    def __init__(self, con) -> None:
        self.con = con
        self._progress_total_steps = 3

    def _now(self) -> datetime:
        return datetime.now(timezone.utc)

    def _log_step(self, step_no: int, label: str) -> None:
        print(
            f"[build_short_features] step {step_no}/{self._progress_total_steps}: {label}",
            flush=True,
        )

    def _safe_scalar(self, sql: str, default: int = 0) -> int:
        try:
            value = self.con.execute(sql).fetchone()[0]
            if value is None:
                return default
            return int(value)
        except Exception:
            return default

    def _build_pipeline_result(
        self,
        *,
        status: str,
        started_at: datetime,
        finished_at: datetime,
        rows_read: int,
        rows_written: int,
        error_message: str | None,
        metrics: dict[str, Any],
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
                error_message=error_message,
                metrics=metrics,
            )

    def _result_to_json(self, result: Any) -> str:
        if hasattr(result, "to_json"):
            return result.to_json()

        if is_dataclass(result):
            return json.dumps(asdict(result), default=str)

        if hasattr(result, "__dict__"):
            return json.dumps(vars(result), default=str)

        return json.dumps(str(result))

    def _ensure_required_tables(self) -> None:
        required = {
            "daily_short_volume_history",
            "finra_short_interest_history",
            "short_features_daily",
        }
        existing = {
            str(row[0]).strip().lower()
            for row in self.con.execute("SHOW TABLES").fetchall()
        }
        missing = [name for name in required if name.lower() not in existing]
        if missing:
            raise RuntimeError(f"missing required tables: {', '.join(sorted(missing))}")

    def _rebuild_short_features_daily(self) -> dict[str, int]:
        """
        Rebuild complet, SQL-first, idempotent.

        Grain ciblé
        -----------
        Une ligne par:
        - symbol
        - as_of_date
        - source_file

        Pourquoi conserver `source_file`
        -------------------------------
        Le daily short volume contient plusieurs fichiers / marchés par jour.
        On conserve ce grain source-fidelity dans short_features_daily pour
        éviter de fusionner implicitement des observations distinctes sans règle
        métier explicite.
        """

        before_count = self._safe_scalar("SELECT COUNT(*) FROM short_features_daily", 0)

        self.con.execute("DELETE FROM short_features_daily")

        self.con.execute(
            """
            INSERT INTO short_features_daily (
                symbol,
                as_of_date,
                source_file,
                short_volume,
                short_exempt_volume,
                total_volume,
                short_volume_ratio,
                short_volume_ratio_20d_avg,
                short_interest,
                previous_short_interest,
                short_interest_change,
                short_interest_change_pct,
                avg_daily_volume,
                days_to_cover,
                short_interest_pct_float,
                max_source_available_at,
                created_at
            )
            WITH volume_base AS (
                SELECT
                    d.symbol,
                    d.trade_date AS as_of_date,
                    d.source_file,
                    d.short_volume,
                    d.short_exempt_volume,
                    d.total_volume,
                    d.short_volume_ratio,
                    d.available_at,
                    AVG(d.short_volume_ratio) OVER (
                        PARTITION BY d.symbol, d.source_file
                        ORDER BY d.trade_date
                        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                    ) AS short_volume_ratio_20d_avg
                FROM daily_short_volume_history AS d
            ),
            short_interest_latest_asof AS (
                SELECT
                    v.symbol,
                    v.as_of_date,
                    v.source_file,
                    s.short_interest,
                    s.previous_short_interest,
                    s.avg_daily_volume,
                    s.days_to_cover,
                    s.short_interest_pct_float,
                    s.ingested_at,
                    ROW_NUMBER() OVER (
                        PARTITION BY v.symbol, v.as_of_date, v.source_file
                        ORDER BY s.settlement_date DESC, s.ingested_at DESC
                    ) AS rn
                FROM volume_base AS v
                LEFT JOIN finra_short_interest_history AS s
                    ON s.symbol = v.symbol
                   AND s.settlement_date <= v.as_of_date
                   AND s.ingested_at <= v.available_at
            )
            SELECT
                v.symbol,
                v.as_of_date,
                v.source_file,
                v.short_volume,
                v.short_exempt_volume,
                v.total_volume,
                v.short_volume_ratio,
                v.short_volume_ratio_20d_avg,
                s.short_interest,
                s.previous_short_interest,
                CASE
                    WHEN s.short_interest IS NOT NULL AND s.previous_short_interest IS NOT NULL
                    THEN s.short_interest - s.previous_short_interest
                    ELSE NULL
                END AS short_interest_change,
                CASE
                    WHEN s.previous_short_interest IS NOT NULL AND s.previous_short_interest <> 0
                    THEN (s.short_interest - s.previous_short_interest) / s.previous_short_interest
                    ELSE NULL
                END AS short_interest_change_pct,
                s.avg_daily_volume,
                s.days_to_cover,
                s.short_interest_pct_float,
                v.available_at AS max_source_available_at,
                CURRENT_TIMESTAMP AS created_at
            FROM volume_base AS v
            LEFT JOIN short_interest_latest_asof AS s
                ON s.symbol = v.symbol
               AND s.as_of_date = v.as_of_date
               AND s.source_file = v.source_file
               AND s.rn = 1
            """
        )

        after_count = self._safe_scalar("SELECT COUNT(*) FROM short_features_daily", 0)

        return {
            "short_features_rows_before": before_count,
            "short_features_rows_inserted": max(after_count - before_count, 0),
            "short_features_rows_after": after_count,
        }

    def run(self) -> PipelineResult:
        started_at = self._now()
        rows_read = 0
        rows_written = 0
        metrics: dict[str, Any] = {}

        try:
            self._log_step(1, "inspect short-data source tables")
            self._ensure_required_tables()

            daily_count = self._safe_scalar("SELECT COUNT(*) FROM daily_short_volume_history", 0)
            short_interest_count = self._safe_scalar("SELECT COUNT(*) FROM finra_short_interest_history", 0)

            metrics["daily_short_volume_history_count"] = daily_count
            metrics["finra_short_interest_history_count"] = short_interest_count
            rows_read = daily_count + short_interest_count

            if daily_count == 0 or short_interest_count == 0:
                metrics["build_decision"] = "noop_missing_inputs"
                finished_at = self._now()
                return self._build_pipeline_result(
                    status="noop",
                    started_at=started_at,
                    finished_at=finished_at,
                    rows_read=rows_read,
                    rows_written=0,
                    error_message=None,
                    metrics=metrics,
                )

            metrics["build_decision"] = "rebuild_short_features_daily"

            self._log_step(2, "rebuild short_features_daily from canonical sources")
            rebuild_result = self._rebuild_short_features_daily()
            metrics.update(rebuild_result)
            rows_written = int(rebuild_result.get("short_features_rows_inserted", 0))

            self._log_step(3, "collect final short feature state")
            metrics["max_short_features_as_of_date"] = self.con.execute(
                "SELECT MAX(as_of_date) FROM short_features_daily"
            ).fetchone()[0]
            metrics["final_short_features_count"] = self._safe_scalar(
                "SELECT COUNT(*) FROM short_features_daily", 0
            )

            finished_at = self._now()
            return self._build_pipeline_result(
                status="success",
                started_at=started_at,
                finished_at=finished_at,
                rows_read=rows_read,
                rows_written=rows_written,
                error_message=None,
                metrics=metrics,
            )

        except Exception as exc:
            finished_at = self._now()
            return self._build_pipeline_result(
                status="failed",
                started_at=started_at,
                finished_at=finished_at,
                rows_read=rows_read,
                rows_written=rows_written,
                error_message=str(exc),
                metrics=metrics,
            )
