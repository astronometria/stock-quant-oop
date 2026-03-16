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


class BuildShortInterestPipeline(BasePipeline):
    """
    Pipeline canonique du domaine FINRA / short interest / short data.

    Important
    ---------
    Ce fichier devient la vérité pipeline officielle du domaine short interest.
    Il conserve la logique SQL-first existante, mais expose désormais un nom
    cohérent avec la convention `build_*_pipeline.py`.

    Compatibilité
    -------------
    Des aliases temporaires sont exposés à la fin du fichier pour éviter de
    casser les imports existants pendant la migration.
    """

    pipeline_name = "build_short_interest"

    def __init__(self, repository=None, uow: DuckDbUnitOfWork | None = None) -> None:
        """
        Accepte soit :
        - un repository déjà branché sur une connexion active
        - un UnitOfWork direct

        Pendant la migration du repo, certains composants injectent encore
        un repository, d'autres un UoW.
        """
        if repository is not None:
            candidate_uow = getattr(repository, "uow", None)
            if candidate_uow is not None:
                self.uow = candidate_uow
            else:
                # Compat transition : repository branché directement sur `con`
                # sans attribut uow. Dans ce cas on ne peut pas piloter via uow.
                self.uow = uow
        else:
            self.uow = uow

        if self.uow is None:
            raise ValueError("BuildShortInterestPipeline requires repository or uow")

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
            f"[build_short_interest] step {step_no}/{self._progress_total_steps}: {label}",
            flush=True,
        )

    def _table_columns(self, table_name: str) -> set[str]:
        rows = self.con.execute(f"PRAGMA table_info('{table_name}')").fetchall()
        return {str(row[1]).strip().lower() for row in rows}

    def _ensure_schema(self) -> None:
        """
        La fondation de schéma doit déjà exister.
        On conserve une méthode dédiée pour garder le découpage du pipeline
        et préparer d'éventuels contrôles de schéma plus stricts.
        """
        required_tables = [
            "short_interest_history",
            "short_interest_latest",
            "short_features_daily",
            "finra_daily_short_volume_source_raw",
        ]
        existing = {
            str(row[0]).strip().lower()
            for row in self.con.execute("SHOW TABLES").fetchall()
        }
        missing = [name for name in required_tables if name.lower() not in existing]
        if missing:
            raise PipelineError(
                f"missing required short-interest tables: {', '.join(missing)}"
            )

    def _cleanup_orphan_rows(self) -> dict[str, int]:
        """
        Nettoyage minimal non destructif des orphelins évidents.
        """
        before_history = int(
            self.con.execute("SELECT COUNT(*) FROM short_interest_history").fetchone()[0]
        )
        before_latest = int(
            self.con.execute("SELECT COUNT(*) FROM short_interest_latest").fetchone()[0]
        )

        # Garde-fou simple : retire les lignes sans symbol utile.
        self.con.execute(
            """
            DELETE FROM short_interest_history
            WHERE symbol IS NULL OR TRIM(symbol) = ''
            """
        )
        self.con.execute(
            """
            DELETE FROM short_interest_latest
            WHERE symbol IS NULL OR TRIM(symbol) = ''
            """
        )

        after_history = int(
            self.con.execute("SELECT COUNT(*) FROM short_interest_history").fetchone()[0]
        )
        after_latest = int(
            self.con.execute("SELECT COUNT(*) FROM short_interest_latest").fetchone()[0]
        )

        return {
            "deleted_orphan_short_interest_history": before_history - after_history,
            "deleted_orphan_short_interest_latest": before_latest - after_latest,
        }

    def _backfill_short_interest_pit(self) -> dict[str, int]:
        """
        Backfill léger des champs PIT manquants lorsque possible.
        """
        columns = self._table_columns("short_interest_history")
        updated = 0

        if "source_name" in columns:
            self.con.execute(
                """
                UPDATE short_interest_history
                SET source_name = COALESCE(NULLIF(TRIM(source_name), ''), 'finra_short_interest')
                WHERE source_name IS NULL OR TRIM(source_name) = ''
                """
            )
            updated += self.con.execute("SELECT changes()").fetchone()[0]

        return {
            "backfilled_short_interest_pit_rows": int(updated),
        }

    def _build_ticker_map_temp(self) -> None:
        """
        Map instrument_id/company_id/symbol depuis les tables master déjà reconstruites.
        """
        self.con.execute("DROP TABLE IF EXISTS tmp_short_interest_ticker_map")
        self.con.execute(
            """
            CREATE TEMP TABLE tmp_short_interest_ticker_map AS
            SELECT
                instrument_id,
                company_id,
                UPPER(TRIM(symbol)) AS symbol
            FROM instrument_master
            WHERE symbol IS NOT NULL
              AND TRIM(symbol) <> ''
            """
        )

    def _upsert_canonical_daily_short_volume_history(self) -> dict[str, int]:
        """
        Alimente la couche canonique short_interest_history à partir de la raw FINRA daily volume.
        Cette version reste volontairement conservative et non destructive.
        """
        before_count = int(
            self.con.execute("SELECT COUNT(*) FROM short_interest_history").fetchone()[0]
        )

        self.con.execute(
            """
            INSERT INTO short_interest_history (
                instrument_id,
                company_id,
                symbol,
                as_of_date,
                short_volume,
                short_exempt_volume,
                total_volume,
                source_name,
                created_at
            )
            SELECT
                m.instrument_id,
                m.company_id,
                UPPER(TRIM(r.symbol)) AS symbol,
                r.trade_date AS as_of_date,
                r.short_volume,
                r.short_exempt_volume,
                r.total_volume,
                COALESCE(NULLIF(TRIM(r.source_name), ''), 'finra_daily_short_volume') AS source_name,
                CURRENT_TIMESTAMP
            FROM finra_daily_short_volume_source_raw r
            LEFT JOIN tmp_short_interest_ticker_map m
              ON UPPER(TRIM(r.symbol)) = m.symbol
            WHERE r.symbol IS NOT NULL
              AND TRIM(r.symbol) <> ''
              AND r.trade_date IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1
                  FROM short_interest_history h
                  WHERE UPPER(TRIM(h.symbol)) = UPPER(TRIM(r.symbol))
                    AND h.as_of_date = r.trade_date
                    AND COALESCE(h.short_volume, -1) = COALESCE(r.short_volume, -1)
                    AND COALESCE(h.total_volume, -1) = COALESCE(r.total_volume, -1)
              )
            """
        )

        after_count = int(
            self.con.execute("SELECT COUNT(*) FROM short_interest_history").fetchone()[0]
        )

        return {
            "inserted_short_interest_history": after_count - before_count,
        }

    def _backfill_existing_short_feature_metadata(self) -> dict[str, int]:
        """
        Harmonise le metadata source_name dans short_features_daily.
        """
        columns = self._table_columns("short_features_daily")
        if "source_name" not in columns:
            return {"backfilled_short_feature_metadata_rows": 0}

        self.con.execute(
            """
            UPDATE short_features_daily
            SET source_name = COALESCE(NULLIF(TRIM(source_name), ''), 'finra_short_interest')
            WHERE source_name IS NULL OR TRIM(source_name) = ''
            """
        )
        try:
            updated = int(self.con.execute("SELECT changes()").fetchone()[0])
        except Exception:
            updated = 0
        return {"backfilled_short_feature_metadata_rows": updated}

    def _build_short_interest_latest(self) -> dict[str, int]:
        """
        Reconstruit short_interest_latest comme vue matérialisée logique.
        """
        before_count = int(
            self.con.execute("SELECT COUNT(*) FROM short_interest_latest").fetchone()[0]
        )

        self.con.execute("DELETE FROM short_interest_latest")
        self.con.execute(
            """
            INSERT INTO short_interest_latest (
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
                CASE
                    WHEN total_volume IS NOT NULL AND total_volume <> 0
                    THEN CAST(short_volume AS DOUBLE) / CAST(total_volume AS DOUBLE)
                    ELSE NULL
                END AS short_volume_ratio,
                source_name,
                CURRENT_TIMESTAMP
            FROM (
                SELECT
                    h.*,
                    ROW_NUMBER() OVER (
                        PARTITION BY UPPER(TRIM(symbol))
                        ORDER BY as_of_date DESC, created_at DESC
                    ) AS rn
                FROM short_interest_history h
                WHERE symbol IS NOT NULL
                  AND TRIM(symbol) <> ''
            ) q
            WHERE rn = 1
            """
        )

        after_count = int(
            self.con.execute("SELECT COUNT(*) FROM short_interest_latest").fetchone()[0]
        )

        return {
            "rebuild_short_interest_latest_rows": after_count,
            "replaced_short_interest_latest_rows": max(before_count, after_count),
        }

    def _insert_missing_short_features_daily(self) -> dict[str, int]:
        """
        Insère les features daily absentes à partir de short_interest_latest.
        """
        before_count = int(
            self.con.execute("SELECT COUNT(*) FROM short_features_daily").fetchone()[0]
        )

        self.con.execute(
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
                NULL AS short_interest_change,
                NULL AS short_interest_change_pct,
                NULL AS short_squeeze_score,
                NULL AS short_pressure_zscore,
                NULL AS days_to_cover_zscore,
                CURRENT_TIMESTAMP AS max_source_available_at,
                source_name,
                CURRENT_TIMESTAMP
            FROM short_interest_latest l
            WHERE NOT EXISTS (
                SELECT 1
                FROM short_features_daily f
                WHERE UPPER(TRIM(f.symbol)) = UPPER(TRIM(l.symbol))
                  AND f.as_of_date = l.as_of_date
            )
            """
        )

        after_count = int(
            self.con.execute("SELECT COUNT(*) FROM short_features_daily").fetchone()[0]
        )

        return {
            "inserted_short_features_daily": after_count - before_count,
        }

    def _refresh_short_metrics(self) -> dict[str, int]:
        """
        Résumé final léger.
        """
        history_count = int(
            self.con.execute("SELECT COUNT(*) FROM short_interest_history").fetchone()[0]
        )
        latest_count = int(
            self.con.execute("SELECT COUNT(*) FROM short_interest_latest").fetchone()[0]
        )
        features_count = int(
            self.con.execute("SELECT COUNT(*) FROM short_features_daily").fetchone()[0]
        )
        return {
            "short_interest_history_rows": history_count,
            "short_interest_latest_rows": latest_count,
            "short_features_daily_rows": features_count,
        }

    def load(self, data) -> None:
        self._metrics["rows_read_raw"] = int(
            self.con.execute("SELECT COUNT(*) FROM finra_daily_short_volume_source_raw").fetchone()[0]
        )

        progress = tqdm(
            total=self._progress_total_steps,
            desc="build_short_interest",
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

        self._log_step(6, "rebuild short_interest_latest")
        self._metrics.update(self._build_short_interest_latest())
        progress.update(1)

        self._log_step(7, "insert missing short_features_daily and refresh metrics")
        self._metrics.update(self._insert_missing_short_features_daily())
        self._metrics.update(self._refresh_short_metrics())
        progress.update(1)

        progress.close()

        self._rows_written = int(
            self._metrics.get("inserted_short_interest_history", 0)
            + self._metrics.get("inserted_short_features_daily", 0)
        )

    def finalize(self, result: PipelineResult) -> PipelineResult:
        result.rows_read = int(self._metrics.get("rows_read_raw", 0))
        result.rows_written = self._rows_written
        result.metrics.update(self._metrics)
        return result


# ----------------------------------------------------------------------
# Aliases de compatibilité pendant migration
# ----------------------------------------------------------------------
BuildShortDataPipeline = BuildShortInterestPipeline
BuildFinraShortInterestPipeline = BuildShortInterestPipeline

__all__ = [
    "BuildShortInterestPipeline",
    "BuildShortDataPipeline",
    "BuildFinraShortInterestPipeline",
]
