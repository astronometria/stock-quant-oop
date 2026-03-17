from __future__ import annotations

"""
Canonical SQL-first FINRA short-interest pipeline.

Objectif:
- partir de `finra_short_interest_source_raw`
- construire `finra_short_interest_history`
- reconstruire `finra_short_interest_latest`
- rester compatible avec des services qui retournent soit un int, soit un dict

Important:
- cette pipeline ne doit pas recaster brutalement en int un résultat dict
- le history build existe déjà côté service/repository
- on normalise ici la lecture des métriques
"""

from dataclasses import asdict, is_dataclass
from datetime import datetime, timezone
from typing import Any

from stock_quant.app.dto.pipeline_result import PipelineResult


try:
    from tqdm import tqdm
except Exception:
    def tqdm(iterable=None, **kwargs):
        return iterable


class BuildShortInterestPipeline:
    pipeline_name = "build_short_interest"

    def __init__(self, service) -> None:
        self.service = service
        self._progress_total_steps = 5

    def _now(self) -> datetime:
        return datetime.now(timezone.utc)

    def _log_step(self, step_no: int, label: str) -> None:
        print(
            f"[build_short_interest] step {step_no}/{self._progress_total_steps}: {label}",
            flush=True,
        )

    def _safe_result_payload(self, value: Any) -> dict[str, Any]:
        """
        Normalise n'importe quel retour de service en dictionnaire.
        Cas supportés:
        - dict
        - int / float / bool
        - dataclass
        - objet avec __dict__
        - fallback string
        """
        if isinstance(value, dict):
            return value

        if isinstance(value, (int, float, bool)) or value is None:
            return {"value": value}

        if is_dataclass(value):
            try:
                payload = asdict(value)
                if isinstance(payload, dict):
                    return payload
            except Exception:
                pass

        if hasattr(value, "__dict__"):
            try:
                return {
                    key: val
                    for key, val in vars(value).items()
                    if not key.startswith("_")
                }
            except Exception:
                pass

        return {"value": str(value)}

    def _safe_metric_int(self, payload: dict[str, Any], *keys: str, default: int = 0) -> int:
        """
        Cherche la première clé entière plausible dans un payload dict.
        """
        for key in keys:
            if key in payload:
                raw = payload[key]
                if raw is None:
                    continue
                if isinstance(raw, bool):
                    return int(raw)
                if isinstance(raw, (int, float)):
                    return int(raw)
                if isinstance(raw, str) and raw.strip():
                    try:
                        return int(float(raw))
                    except Exception:
                        continue
        return default

    def _read_scalar(self, repository: Any, method_name: str, default: int = 0) -> int:
        method = getattr(repository, method_name, None)
        if method is None:
            return default

        value = method()
        if value is None:
            return default
        if isinstance(value, bool):
            return int(value)
        if isinstance(value, (int, float)):
            return int(value)
        if isinstance(value, str) and value.strip():
            try:
                return int(float(value))
            except Exception:
                return default
        return default

    def _read_optional(self, repository: Any, method_name: str, default: Any = None) -> Any:
        method = getattr(repository, method_name, None)
        if method is None:
            return default
        try:
            value = method()
            return default if value is None else value
        except Exception:
            return default

    def _collect_build_state(self) -> dict[str, Any]:
        """
        Lit l'état du repository si les helpers existent.
        Sinon, reste robuste avec des défauts.
        """
        repo = self.service.repository

        return {
            "raw_row_count": self._read_scalar(repo, "get_raw_row_count", 0),
            "source_row_count": self._read_scalar(repo, "get_source_row_count", 0),
            "history_row_count": self._read_scalar(repo, "get_history_row_count", 0),
            "latest_row_count": self._read_scalar(repo, "get_latest_row_count", 0),
            "max_raw_source_date": str(self._read_optional(repo, "get_max_raw_source_date", None)),
            "max_history_settlement_date": str(self._read_optional(repo, "get_max_history_settlement_date", None)),
            "max_latest_settlement_date": str(self._read_optional(repo, "get_max_latest_settlement_date", None)),
        }

    def _should_build(self, state: dict[str, Any]) -> tuple[bool, str]:
        raw_count = int(state.get("raw_row_count", 0) or 0)
        history_count = int(state.get("history_row_count", 0) or 0)
        latest_count = int(state.get("latest_row_count", 0) or 0)

        max_raw = state.get("max_raw_source_date")
        max_history = state.get("max_history_settlement_date")

        if raw_count == 0:
            return False, "noop_no_raw_rows"

        if history_count == 0 or latest_count == 0:
            return True, "build_missing_canonical_tables"

        if str(max_raw) != str(max_history):
            return True, "build_raw_newer_than_history"

        return False, "already_up_to_date"

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
        Compat pour différentes versions de PipelineResult.
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

    def run(self) -> PipelineResult:
        started_at = self._now()
        rows_read = 0
        rows_written = 0
        metrics: dict[str, Any] = {}

        try:
            self._log_step(1, "detect canonical short-interest build need from table state")
            state = self._collect_build_state()
            metrics.update(state)

            should_build, decision = self._should_build(state)
            metrics["build_decision"] = decision

            if not should_build:
                finished_at = self._now()
                return self._build_pipeline_result(
                    status="noop",
                    started_at=started_at,
                    finished_at=finished_at,
                    rows_read=0,
                    rows_written=0,
                    error_message=None,
                    metrics=metrics,
                )

            self._log_step(2, "load raw short-interest rows from staging")
            loaded_raw_rows = self.service.load_raw()
            loaded_payload = self._safe_result_payload(loaded_raw_rows)
            metrics["loaded_raw_rows"] = self._safe_metric_int(
                loaded_payload,
                "loaded_raw_rows",
                "row_count",
                "rows",
                "value",
                default=0,
            )
            rows_read = metrics["loaded_raw_rows"]

            self._log_step(3, "build canonical short-interest history")
            history_result = self.service.build_history()
            history_payload = self._safe_result_payload(history_result)
            metrics["history_build_result"] = history_payload

            history_rows_written = self._safe_metric_int(
                history_payload,
                "rows_written",
                "inserted_rows",
                "upserted_rows",
                "history_rows_written",
                "row_count",
                "value",
                default=0,
            )
            metrics["history_rows_written"] = history_rows_written
            rows_written += history_rows_written

            self._log_step(4, "refresh canonical short-interest latest")
            latest_result = self.service.refresh_latest()
            latest_payload = self._safe_result_payload(latest_result)
            metrics["latest_refresh_result"] = latest_payload

            latest_rows_written = self._safe_metric_int(
                latest_payload,
                "rows_written",
                "inserted_rows",
                "replaced_rows",
                "latest_rows_written",
                "row_count",
                "value",
                default=0,
            )
            metrics["latest_rows_written"] = latest_rows_written
            rows_written += latest_rows_written

            self._log_step(5, "collect final canonical short-interest state")
            final_state = self._collect_build_state()
            metrics.update({
                "final_raw_row_count": final_state.get("raw_row_count"),
                "final_source_row_count": final_state.get("source_row_count"),
                "final_history_row_count": final_state.get("history_row_count"),
                "final_latest_row_count": final_state.get("latest_row_count"),
                "final_max_raw_source_date": final_state.get("max_raw_source_date"),
                "final_max_history_settlement_date": final_state.get("max_history_settlement_date"),
                "final_max_latest_settlement_date": final_state.get("max_latest_settlement_date"),
            })

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
