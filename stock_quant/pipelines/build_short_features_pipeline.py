from __future__ import annotations

import json
from dataclasses import asdict, is_dataclass
from datetime import datetime, timezone
from typing import Any

from stock_quant.app.dto.pipeline_result import PipelineResult


class BuildShortFeaturesPipeline:
    """
    Canonical SQL-first builder for short_features_daily.

    Objectif
    --------
    Construire une table dérivée de recherche à partir de :
    - daily_short_volume_history
    - finra_short_interest_history

    Contraintes de conception
    -------------------------
    - SQL-first : la logique métier principale vit dans DuckDB SQL.
    - PIT-safe : on n'utilise jamais une date métier comme substitut au moment
      de disponibilité.
    - Idempotent : le rebuild complet doit donner le même résultat si les
      tables sources n'ont pas changé.
    - Serving/latest non utilisés : on travaille uniquement à partir des tables
      historiques canoniques.
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
        """
        Lecture robuste d'un scalaire numérique depuis DuckDB.
        """
        try:
            row = self.con.execute(sql).fetchone()
            if not row:
                return default
            value = row[0]
            if value is None:
                return default
            return int(value)
        except Exception:
            return default

    def _safe_optional_scalar(self, sql: str) -> Any:
        """
        Lecture robuste d'un scalaire potentiellement non numérique.
        """
        try:
            row = self.con.execute(sql).fetchone()
            if not row:
                return None
            return row[0]
        except Exception:
            return None

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
        """
        Compatibilité avec plusieurs variantes de PipelineResult.
        """
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

    def _table_columns(self, table_name: str) -> set[str]:
        """
        Retourne l'ensemble des colonnes présentes dans une table.
        """
        rows = self.con.execute(f"PRAGMA table_info('{table_name}')").fetchall()
        return {str(row[1]).strip().lower() for row in rows}

    def _ensure_required_tables(self) -> None:
        """
        Vérifie que les tables minimales existent.
        """
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
        Rebuild complet de short_features_daily.

        Important
        ---------
        Le schéma réel actuel de short_features_daily ne contient pas `source_file`.
        On ne tente donc pas de l'écrire.

        Grain cible
        -----------
        1 ligne par :
        - symbol
        - as_of_date

        S'il existe plusieurs lignes daily_short_volume_history pour un symbole
        et une date (plusieurs marchés / sources), on agrège avant insertion.

        PIT logic
        ---------
        - daily short volume : disponible à `daily_short_volume_history.available_at`
        - short interest : disponible à `finra_short_interest_history.ingested_at`
          dans l'état actuel du repo

        Pour chaque (symbol, as_of_date), on choisit le short interest le plus
        récent tel que :
        - settlement_date <= as_of_date
        - ingested_at <= max_source_available_at
        """

        before_count = self._safe_scalar("SELECT COUNT(*) FROM short_features_daily", 0)

        # Rebuild complet et idempotent.
        self.con.execute("DELETE FROM short_features_daily")

        self.con.execute(
            """
            INSERT INTO short_features_daily (
                symbol,
                as_of_date,
                short_volume,
                short_exempt_volume,
                total_volume,
                short_volume_ratio,
                short_volume_ratio_20d_avg,
                short_interest,
                previous_short_interest,
                short_interest_change_pct,
                short_squeeze_score,
                short_pressure_zscore,
                days_to_cover,
                days_to_cover_zscore,
                shares_float,
                short_interest_pct_float,
                max_source_available_at
            )
            WITH daily_agg AS (
                SELECT
                    d.symbol,
                    d.trade_date AS as_of_date,

                    -- Agrégation conservative du volume short quotidien.
                    SUM(COALESCE(d.short_volume, 0.0)) AS short_volume,
                    SUM(COALESCE(d.short_exempt_volume, 0.0)) AS short_exempt_volume,
                    SUM(COALESCE(d.total_volume, 0.0)) AS total_volume,

                    CASE
                        WHEN SUM(COALESCE(d.total_volume, 0.0)) > 0
                        THEN SUM(COALESCE(d.short_volume, 0.0)) / SUM(COALESCE(d.total_volume, 0.0))
                        ELSE NULL
                    END AS short_volume_ratio,

                    MAX(d.available_at) AS max_source_available_at
                FROM daily_short_volume_history AS d
                WHERE d.symbol IS NOT NULL
                  AND TRIM(d.symbol) <> ''
                  AND d.trade_date IS NOT NULL
                GROUP BY
                    d.symbol,
                    d.trade_date
            ),
            daily_enriched AS (
                SELECT
                    a.*,
                    AVG(a.short_volume_ratio) OVER (
                        PARTITION BY a.symbol
                        ORDER BY a.as_of_date
                        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                    ) AS short_volume_ratio_20d_avg
                FROM daily_agg AS a
            ),
            short_interest_asof AS (
                SELECT
                    d.symbol,
                    d.as_of_date,
                    d.max_source_available_at,
                    s.short_interest,
                    s.previous_short_interest,
                    s.avg_daily_volume,
                    s.days_to_cover,
                    s.shares_float,
                    s.short_interest_pct_float,
                    ROW_NUMBER() OVER (
                        PARTITION BY d.symbol, d.as_of_date
                        ORDER BY s.settlement_date DESC, s.ingested_at DESC
                    ) AS rn
                FROM daily_enriched AS d
                LEFT JOIN finra_short_interest_history AS s
                    ON s.symbol = d.symbol
                   AND s.settlement_date <= d.as_of_date
                   AND s.ingested_at <= d.max_source_available_at
            ),
            joined AS (
                SELECT
                    d.symbol,
                    d.as_of_date,
                    d.short_volume,
                    d.short_exempt_volume,
                    d.total_volume,
                    d.short_volume_ratio,
                    d.short_volume_ratio_20d_avg,
                    s.short_interest,
                    s.previous_short_interest,
                    s.avg_daily_volume,
                    s.days_to_cover,
                    s.shares_float,
                    s.short_interest_pct_float,
                    d.max_source_available_at
                FROM daily_enriched AS d
                LEFT JOIN short_interest_asof AS s
                    ON s.symbol = d.symbol
                   AND s.as_of_date = d.as_of_date
                   AND s.rn = 1
            ),
            scored AS (
                SELECT
                    j.*,

                    CASE
                        WHEN j.previous_short_interest IS NOT NULL
                         AND j.previous_short_interest <> 0
                         AND j.short_interest IS NOT NULL
                        THEN (j.short_interest - j.previous_short_interest) / j.previous_short_interest
                        ELSE NULL
                    END AS short_interest_change_pct,

                    CASE
                        WHEN j.days_to_cover IS NOT NULL
                         AND j.short_volume_ratio IS NOT NULL
                        THEN j.days_to_cover * j.short_volume_ratio
                        ELSE NULL
                    END AS short_squeeze_score,

                    AVG(j.short_volume_ratio) OVER (
                        PARTITION BY j.symbol
                        ORDER BY j.as_of_date
                        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                    ) AS short_volume_ratio_avg_20d,

                    STDDEV_SAMP(j.short_volume_ratio) OVER (
                        PARTITION BY j.symbol
                        ORDER BY j.as_of_date
                        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                    ) AS short_volume_ratio_std_20d,

                    AVG(j.days_to_cover) OVER (
                        PARTITION BY j.symbol
                        ORDER BY j.as_of_date
                        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                    ) AS days_to_cover_avg_20d,

                    STDDEV_SAMP(j.days_to_cover) OVER (
                        PARTITION BY j.symbol
                        ORDER BY j.as_of_date
                        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                    ) AS days_to_cover_std_20d
                FROM joined AS j
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
                short_interest_change_pct,
                short_squeeze_score,

                CASE
                    WHEN short_volume_ratio_std_20d IS NOT NULL
                     AND short_volume_ratio_std_20d <> 0
                     AND short_volume_ratio IS NOT NULL
                     AND short_volume_ratio_avg_20d IS NOT NULL
                    THEN (short_volume_ratio - short_volume_ratio_avg_20d) / short_volume_ratio_std_20d
                    ELSE NULL
                END AS short_pressure_zscore,

                days_to_cover,

                CASE
                    WHEN days_to_cover_std_20d IS NOT NULL
                     AND days_to_cover_std_20d <> 0
                     AND days_to_cover IS NOT NULL
                     AND days_to_cover_avg_20d IS NOT NULL
                    THEN (days_to_cover - days_to_cover_avg_20d) / days_to_cover_std_20d
                    ELSE NULL
                END AS days_to_cover_zscore,

                shares_float,
                short_interest_pct_float,
                max_source_available_at
            FROM scored
            """
        )

        after_count = self._safe_scalar("SELECT COUNT(*) FROM short_features_daily", 0)

        return {
            "short_features_rows_before": before_count,
            "short_features_rows_inserted": after_count,
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
            metrics["max_short_features_as_of_date"] = self._safe_optional_scalar(
                "SELECT MAX(as_of_date) FROM short_features_daily"
            )
            metrics["final_short_features_count"] = self._safe_scalar(
                "SELECT COUNT(*) FROM short_features_daily",
                0,
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
