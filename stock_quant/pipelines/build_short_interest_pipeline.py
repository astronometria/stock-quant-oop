from __future__ import annotations

"""
Canonical SQL-first FINRA short-interest pipeline.

Objectifs:
- garder la logique lourde côté SQL / repository
- garder le pipeline mince et lisible
- éviter les dépendances fragiles à des factories DTO optionnelles
- rester compatible avec plusieurs variantes de PipelineResult
"""

from dataclasses import fields, is_dataclass
from datetime import datetime, timezone
from typing import Any

from stock_quant.app.dto.pipeline_result import PipelineResult
from stock_quant.pipelines.base_pipeline import BasePipeline
from stock_quant.shared.exceptions import PipelineError


try:
    from tqdm import tqdm
except Exception:
    def tqdm(iterable=None, **kwargs):
        return iterable


class BuildShortInterestPipeline(BasePipeline):
    """
    Pipeline canonique FINRA short interest.

    Notes:
    - Le staging raw est déjà chargé dans `finra_short_interest_source_raw`
    - Le repository fait le travail SQL-first de transformation / upsert
    - Ce pipeline orchestre seulement les étapes et construit un résultat
    """

    pipeline_name = "build_short_interest"

    def __init__(self, service) -> None:
        """
        Injecte uniquement le service canonique.

        Important:
        - on ne supporte plus l'ancienne signature repository=...
        - le service est le contrat stable attendu par le CLI
        """
        self.service = service
        self._progress_total_steps = 5

    # ------------------------------------------------------------------
    # Helpers résultat / timestamps
    # ------------------------------------------------------------------

    def _utcnow(self) -> datetime:
        """Retourne un datetime UTC timezone-aware."""
        return datetime.now(timezone.utc)

    def _status_value(self, value: str) -> Any:
        """
        Tente de résoudre une enum de statut si le DTO en utilise une,
        sinon retourne simplement une string.
        """
        for attr_name in ("Status", "PipelineStatus"):
            enum_cls = getattr(PipelineResult, attr_name, None)
            if enum_cls is None:
                continue

            for candidate in (value, value.upper(), value.capitalize()):
                try:
                    return getattr(enum_cls, candidate)
                except Exception:
                    pass

            try:
                return enum_cls(value)
            except Exception:
                pass

        return value

    def _build_result_instance(self, **values: Any) -> PipelineResult:
        """
        Construit un PipelineResult sans présumer d'une factory classmethod.

        Stratégie:
        1. si PipelineResult est un dataclass, on renseigne seulement les champs existants
        2. sinon on tente le constructeur standard avec filtrage progressif
        """
        if is_dataclass(PipelineResult):
            valid_fields = {f.name for f in fields(PipelineResult)}
            payload = {k: v for k, v in values.items() if k in valid_fields}
            return PipelineResult(**payload)

        try:
            return PipelineResult(**values)
        except TypeError:
            reduced = dict(values)
            for optional_key in [
                "error_message",
                "metrics",
                "rows_read",
                "rows_written",
                "finished_at",
            ]:
                reduced.pop(optional_key, None)
                try:
                    return PipelineResult(**reduced)
                except TypeError:
                    continue
            raise

    def _new_started_result(self) -> PipelineResult:
        """
        Crée un résultat 'started' compatible avec plusieurs formes de DTO.
        """
        started_at = self._utcnow()
        status = self._status_value("started")

        if hasattr(PipelineResult, "started"):
            try:
                return PipelineResult.started(self.pipeline_name)
            except Exception:
                pass

        return self._build_result_instance(
            pipeline_name=self.pipeline_name,
            status=status,
            started_at=started_at,
            finished_at=None,
            rows_read=0,
            rows_written=0,
            error_message=None,
            metrics={},
        )

    def _finalize_result(
        self,
        result: PipelineResult,
        *,
        status: str,
        rows_read: int,
        rows_written: int,
        metrics: dict[str, Any],
        error_message: str | None = None,
    ) -> PipelineResult:
        """
        Retourne un PipelineResult final cohérent.

        Si le DTO expose des factories `succeeded` / `failed`, on les utilise.
        Sinon on reconstruit proprement une instance.
        """
        finished_at = self._utcnow()
        status_value = self._status_value(status)

        if status == "succeeded" and hasattr(PipelineResult, "succeeded"):
            try:
                return PipelineResult.succeeded(
                    self.pipeline_name,
                    rows_read=rows_read,
                    rows_written=rows_written,
                    metrics=metrics,
                )
            except Exception:
                pass

        if status == "failed" and hasattr(PipelineResult, "failed"):
            try:
                return PipelineResult.failed(
                    self.pipeline_name,
                    error_message=error_message or "unknown pipeline error",
                    rows_read=rows_read,
                    rows_written=rows_written,
                    metrics=metrics,
                )
            except Exception:
                pass

        started_at = getattr(result, "started_at", None) or self._utcnow()

        return self._build_result_instance(
            pipeline_name=self.pipeline_name,
            status=status_value,
            started_at=started_at,
            finished_at=finished_at,
            rows_read=rows_read,
            rows_written=rows_written,
            error_message=error_message,
            metrics=metrics,
        )

    def _noop_result(
        self,
        result: PipelineResult,
        *,
        rows_read: int,
        metrics: dict[str, Any],
    ) -> PipelineResult:
        """
        Retourne un résultat noop.
        """
        finished_at = self._utcnow()
        started_at = getattr(result, "started_at", None) or self._utcnow()

        return self._build_result_instance(
            pipeline_name=self.pipeline_name,
            status=self._status_value("noop"),
            started_at=started_at,
            finished_at=finished_at,
            rows_read=rows_read,
            rows_written=0,
            error_message=None,
            metrics=metrics,
        )

    # ------------------------------------------------------------------
    # Logging / orchestration
    # ------------------------------------------------------------------

    def _log_step(self, step_no: int, label: str) -> None:
        print(
            f"[build_short_interest] step {step_no}/{self._progress_total_steps}: {label}",
            flush=True,
        )

    def _collect_build_state(self) -> dict[str, Any]:
        """
        Lit l'état minimal nécessaire via le repository.
        """
        repo = self.service.repository
        return {
            "raw_row_count": int(repo.get_raw_row_count()),
            "source_row_count": int(repo.get_source_row_count()),
            "history_row_count": int(repo.get_history_row_count()),
            "latest_row_count": int(repo.get_latest_row_count()),
            "max_raw_source_date": repo.get_max_raw_source_date(),
            "max_history_settlement_date": repo.get_max_history_settlement_date(),
            "max_latest_settlement_date": repo.get_max_latest_settlement_date(),
        }

    def _should_noop(self, state: dict[str, Any]) -> tuple[bool, dict[str, Any]]:
        """
        Décision canonique de build.
        """
        raw_count = int(state["raw_row_count"])
        history_count = int(state["history_row_count"])
        latest_count = int(state["latest_row_count"])
        max_raw_source_date = state["max_raw_source_date"]
        max_history_settlement_date = state["max_history_settlement_date"]

        metrics = {
            "raw_row_count": raw_count,
            "source_row_count": int(state["source_row_count"]),
            "history_row_count": history_count,
            "latest_row_count": latest_count,
            "max_raw_source_date": str(max_raw_source_date),
            "max_history_settlement_date": str(max_history_settlement_date),
            "max_latest_settlement_date": str(state["max_latest_settlement_date"]),
        }

        if raw_count == 0:
            metrics["build_decision"] = "noop_no_raw_rows"
            return True, metrics

        if history_count == 0 or latest_count == 0:
            metrics["build_decision"] = "build_missing_canonical_tables"
            return False, metrics

        if max_raw_source_date is not None and max_history_settlement_date is not None:
            if max_raw_source_date > max_history_settlement_date:
                metrics["build_decision"] = "build_raw_newer_than_history"
                return False, metrics

        metrics["build_decision"] = "noop_already_up_to_date"
        return True, metrics

    # ------------------------------------------------------------------
    # Main pipeline
    # ------------------------------------------------------------------

    def run(self) -> PipelineResult:
        """
        Exécute le pipeline canonique.
        """
        result = self._new_started_result()
        rows_read = 0
        rows_written = 0
        metrics: dict[str, Any] = {}

        try:
            self._log_step(
                1,
                "detect canonical short-interest build need from table state",
            )
            state = self._collect_build_state()
            should_noop, decision_metrics = self._should_noop(state)
            metrics.update(decision_metrics)

            if should_noop:
                return self._noop_result(
                    result,
                    rows_read=rows_read,
                    metrics=metrics,
                )

            self._log_step(2, "load raw short-interest rows from staging")
            rows_read = int(self.service.load_raw())
            metrics["loaded_raw_rows"] = rows_read

            self._log_step(3, "build canonical short-interest history")
            history_result = self.service.build_history()
            history_rows = int(history_result or 0)
            metrics["history_rows_written"] = history_rows
            rows_written += history_rows

            self._log_step(4, "refresh canonical latest short-interest snapshot")
            latest_result = self.service.refresh_latest()
            latest_rows = int(latest_result or 0)
            metrics["latest_rows_written"] = latest_rows

            self._log_step(5, "finalize canonical metrics")
            post_state = self._collect_build_state()
            metrics.update({
                "post_history_row_count": int(post_state["history_row_count"]),
                "post_latest_row_count": int(post_state["latest_row_count"]),
                "post_max_history_settlement_date": str(post_state["max_history_settlement_date"]),
                "post_max_latest_settlement_date": str(post_state["max_latest_settlement_date"]),
            })

            return self._finalize_result(
                result,
                status="succeeded",
                rows_read=rows_read,
                rows_written=rows_written,
                metrics=metrics,
                error_message=None,
            )

        except Exception as exc:
            return self._finalize_result(
                result,
                status="failed",
                rows_read=rows_read,
                rows_written=rows_written,
                metrics=metrics,
                error_message=str(exc),
            )
