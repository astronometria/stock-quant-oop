from __future__ import annotations

import json
from dataclasses import asdict, is_dataclass
from datetime import datetime, timezone
from typing import Any

from stock_quant.app.dto.pipeline_result import PipelineResult


class BuildShortFeaturesPipeline:
    """
    Canonical SQL-first builder for short_features_daily.

    Design goals
    ------------
    - SQL-first: toute la logique lourde reste dans DuckDB SQL.
    - PIT-safe:
      * daily short volume utilise daily_short_volume_history.available_at
      * short interest utilise finra_short_interest_history.ingested_at
      * aucun fallback dangereux de date métier vers disponibilité
    - idempotent:
      * rebuild complet déterministe si les sources ne changent pas
    - survivor-bias safe:
      * aucun filtre par univers courant dans la reconstruction historique
    - memory-aware:
      * on casse le rebuild en batchs mensuels au lieu d'un seul gros INSERT
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

    def _table_columns(self, table_name: str) -> set[str]:
        rows = self.con.execute(f"PRAGMA table_info('{table_name}')").fetchall()
        return {str(row[1]).strip().lower() for row in rows}

    def _prepare_daily_enriched_temp(self) -> dict[str, int]:
        """
        Prépare une table temp déjà agrégée au grain (symbol, as_of_date).

        Pourquoi
        --------
        Le raw canonical `daily_short_volume_history` contient plusieurs lignes par
        symbole / date / source / marché. On réduit d'abord cette cardinalité avant
        le join PIT avec short interest.

        Cette étape reste SQL-first et diminue fortement la mémoire du join suivant.
        """
        self.con.execute("DROP TABLE IF EXISTS tmp_short_features_daily_enriched")

        self.con.execute(
            """
            CREATE TEMP TABLE tmp_short_features_daily_enriched AS
            WITH daily_agg AS (
                SELECT
                    d.symbol,
                    d.trade_date AS as_of_date,
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
            )
            SELECT
                a.symbol,
                a.as_of_date,
                a.short_volume,
                a.short_exempt_volume,
                a.total_volume,
                a.short_volume_ratio,
                AVG(a.short_volume_ratio) OVER (
                    PARTITION BY a.symbol
                    ORDER BY a.as_of_date
                    ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                ) AS short_volume_ratio_20d_avg,
                a.max_source_available_at
            FROM daily_agg AS a
            """
        )

        temp_rows = self._safe_scalar(
            "SELECT COUNT(*) FROM tmp_short_features_daily_enriched",
            0,
        )
        return {"temp_daily_enriched_rows": temp_rows}

    def _list_month_starts(self) -> list[str]:
        rows = self.con.execute(
            """
            SELECT DISTINCT CAST(date_trunc('month', as_of_date) AS DATE) AS month_start
            FROM tmp_short_features_daily_enriched
            ORDER BY month_start
            """
        ).fetchall()
        return [str(row[0]) for row in rows if row and row[0] is not None]

    def _insert_month_batch(self, month_start: str) -> int:
        """
        Insère un batch mensuel PIT-safe dans short_features_daily.

        Important
        ---------
        - le join short interest est borné au mois courant
        - la sélection PIT choisit le short interest le plus récent disponible
          au moment `max_source_available_at`
        - aucune colonne non présente dans le schéma réel n'est écrite
        """
        before_count = self._safe_scalar("SELECT COUNT(*) FROM short_features_daily", 0)

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
            WITH month_daily AS (
                SELECT *
                FROM tmp_short_features_daily_enriched
                WHERE as_of_date >= CAST(? AS DATE)
                  AND as_of_date < CAST(? AS DATE) + INTERVAL '1 month'
            ),
            short_interest_ranked AS (
                SELECT
                    d.symbol,
                    d.as_of_date,
                    d.short_volume,
                    d.short_exempt_volume,
                    d.total_volume,
                    d.short_volume_ratio,
                    d.short_volume_ratio_20d_avg,
                    d.max_source_available_at,
                    s.short_interest,
                    s.previous_short_interest,
                    s.avg_daily_volume,
                    s.days_to_cover,
                    s.shares_float,
                    s.short_interest_pct_float,
                    ROW_NUMBER() OVER (
                        PARTITION BY d.symbol, d.as_of_date
                        ORDER BY
                            s.settlement_date DESC NULLS LAST,
                            s.ingested_at DESC NULLS LAST
                    ) AS rn
                FROM month_daily AS d
                LEFT JOIN finra_short_interest_history AS s
                    ON s.symbol = d.symbol
                   AND s.settlement_date <= d.as_of_date
                   AND s.ingested_at <= d.max_source_available_at
            ),
            base AS (
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
                    CASE
                        WHEN previous_short_interest IS NOT NULL
                         AND previous_short_interest <> 0
                         AND short_interest IS NOT NULL
                        THEN (short_interest - previous_short_interest) / previous_short_interest
                        ELSE NULL
                    END AS short_interest_change_pct,
                    CAST(NULL AS DOUBLE) AS short_squeeze_score,
                    CAST(NULL AS DOUBLE) AS short_pressure_zscore,
                    days_to_cover,
                    CAST(NULL AS DOUBLE) AS days_to_cover_zscore,
                    shares_float,
                    short_interest_pct_float,
                    max_source_available_at
                FROM short_interest_ranked
                WHERE rn = 1
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
                short_pressure_zscore,
                days_to_cover,
                days_to_cover_zscore,
                shares_float,
                short_interest_pct_float,
                max_source_available_at
            FROM base
            """,
            [month_start, month_start],
        )

        after_count = self._safe_scalar("SELECT COUNT(*) FROM short_features_daily", 0)
        return max(0, after_count - before_count)

    def _apply_global_scores(self) -> dict[str, int]:
        """
        Deuxième passe SQL-first.

        Pourquoi une deuxième passe
        ---------------------------
        Les z-scores doivent voir toute la série temporelle du symbole.
        Les calculer dans chaque batch mensuel casserait la continuité.

        Ici on calcule les fenêtres sur `short_features_daily` déjà reconstruit,
        puis on met à jour les colonnes de score.
        """
        self.con.execute("DROP TABLE IF EXISTS tmp_short_feature_scores")

        self.con.execute(
            """
            CREATE TEMP TABLE tmp_short_feature_scores AS
            WITH scored AS (
                SELECT
                    f.symbol,
                    f.as_of_date,
                    f.short_volume_ratio,
                    f.days_to_cover,

                    AVG(f.short_volume_ratio) OVER (
                        PARTITION BY f.symbol
                        ORDER BY f.as_of_date
                        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                    ) AS short_volume_ratio_avg_20d,

                    STDDEV_POP(f.short_volume_ratio) OVER (
                        PARTITION BY f.symbol
                        ORDER BY f.as_of_date
                        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                    ) AS short_volume_ratio_std_20d,

                    AVG(f.days_to_cover) OVER (
                        PARTITION BY f.symbol
                        ORDER BY f.as_of_date
                        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                    ) AS days_to_cover_avg_20d,

                    STDDEV_POP(f.days_to_cover) OVER (
                        PARTITION BY f.symbol
                        ORDER BY f.as_of_date
                        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                    ) AS days_to_cover_std_20d
                FROM short_features_daily AS f
            )
            SELECT
                symbol,
                as_of_date,
                CASE
                    WHEN short_volume_ratio_std_20d IS NOT NULL
                     AND short_volume_ratio_std_20d > 0
                     AND short_volume_ratio IS NOT NULL
                     AND short_volume_ratio_avg_20d IS NOT NULL
                    THEN (short_volume_ratio - short_volume_ratio_avg_20d) / short_volume_ratio_std_20d
                    ELSE NULL
                END AS short_pressure_zscore,
                CASE
                    WHEN days_to_cover_std_20d IS NOT NULL
                     AND days_to_cover_std_20d > 0
                     AND days_to_cover IS NOT NULL
                     AND days_to_cover_avg_20d IS NOT NULL
                    THEN (days_to_cover - days_to_cover_avg_20d) / days_to_cover_std_20d
                    ELSE NULL
                END AS days_to_cover_zscore
            FROM scored
            """
        )

        self.con.execute(
            """
            UPDATE short_features_daily AS f
            SET
                short_pressure_zscore = s.short_pressure_zscore,
                days_to_cover_zscore = s.days_to_cover_zscore,
                short_squeeze_score = CASE
                    WHEN s.short_pressure_zscore IS NOT NULL
                     AND s.days_to_cover_zscore IS NOT NULL
                    THEN (s.short_pressure_zscore + s.days_to_cover_zscore) / 2.0
                    WHEN s.short_pressure_zscore IS NOT NULL
                    THEN s.short_pressure_zscore
                    WHEN s.days_to_cover_zscore IS NOT NULL
                    THEN s.days_to_cover_zscore
                    ELSE NULL
                END
            FROM tmp_short_feature_scores AS s
            WHERE f.symbol = s.symbol
              AND f.as_of_date = s.as_of_date
            """
        )

        scored_rows = self._safe_scalar(
            "SELECT COUNT(*) FROM tmp_short_feature_scores",
            0,
        )
        return {"scored_short_feature_rows": scored_rows}

    def _rebuild_short_features_daily(self) -> dict[str, int]:
        before_count = self._safe_scalar("SELECT COUNT(*) FROM short_features_daily", 0)

        # Rebuild complet, idempotent.
        self.con.execute("DELETE FROM short_features_daily")

        metrics: dict[str, int] = {}
        metrics.update(self._prepare_daily_enriched_temp())

        month_starts = self._list_month_starts()
        metrics["month_batch_count"] = len(month_starts)

        inserted_total = 0
        for idx, month_start in enumerate(month_starts, start=1):
            if idx == 1 or idx % 12 == 0 or idx == len(month_starts):
                print(
                    f"[build_short_features] month_batch {idx}/{len(month_starts)}: {month_start}",
                    flush=True,
                )
            inserted_total += self._insert_month_batch(month_start)

        metrics["short_features_rows_inserted"] = inserted_total

        metrics.update(self._apply_global_scores())

        after_count = self._safe_scalar("SELECT COUNT(*) FROM short_features_daily", 0)
        metrics["short_features_rows_before"] = before_count
        metrics["short_features_rows_after"] = after_count

        self.con.execute("DROP TABLE IF EXISTS tmp_short_feature_scores")
        self.con.execute("DROP TABLE IF EXISTS tmp_short_features_daily_enriched")

        return metrics

    def run(self) -> PipelineResult:
        started_at = self._now()
        rows_read = 0
        rows_written = 0
        metrics: dict[str, Any] = {}

        try:
            self._log_step(1, "inspect short-data source tables")
            self._ensure_required_tables()

            daily_count = self._safe_scalar(
                "SELECT COUNT(*) FROM daily_short_volume_history",
                0,
            )
            short_interest_count = self._safe_scalar(
                "SELECT COUNT(*) FROM finra_short_interest_history",
                0,
            )

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

            rows_written = int(metrics.get("short_features_rows_after", 0) or 0)

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
