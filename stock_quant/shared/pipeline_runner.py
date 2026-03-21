"""
Runner canonique des pipelines.

Ce module est la SEULE porte d'entrée autorisée pour envelopper une exécution
pipeline et produire un PipelineResult normalisé.

Invariants:
- Une seule vérité pour PipelineResult:
  stock_quant.app.dto.pipeline_result.PipelineResult
- Une seule vérité pour PipelineStatus:
  stock_quant.shared.enums.PipelineStatus
- Les pipelines concrets ne doivent PAS construire PipelineResult eux-mêmes.
- Les pipelines concrets doivent renvoyer un dict avec:
    rows_read
    rows_written
    rows_skipped
    warnings
    metrics
  et peuvent optionnellement renvoyer:
    status
    error_message

Important:
Ce module ne doit pas contenir de logique métier PIT/survivorship.
Ces garanties doivent rester dans les services, policies, repositories et SQL.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from datetime import datetime
from typing import Any

from stock_quant.app.dto.pipeline_result import PipelineResult
from stock_quant.shared.enums import PipelineStatus


def _coerce_int(value: Any, default: int = 0) -> int:
    """
    Convertit proprement une valeur arbitraire en int.
    """
    if value is None:
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _coerce_warnings(value: Any) -> list[str]:
    """
    Normalise les warnings en liste[str].
    """
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item) for item in value]
    if isinstance(value, tuple):
        return [str(item) for item in value]
    return [str(value)]


def _coerce_metrics(value: Any) -> dict[str, Any]:
    """
    Normalise les métriques en dict.
    """
    if value is None:
        return {}
    if isinstance(value, Mapping):
        return dict(value)
    return {"raw_metrics": value}


def _coerce_error_message(value: Any) -> str | None:
    """
    Normalise le message d'erreur.
    """
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _coerce_status(value: Any) -> PipelineStatus:
    """
    Convertit un statut brut en PipelineStatus canonique.

    Conventions supportées:
    - PipelineStatus.*                    -> inchangé
    - "success", "ok"                    -> SUCCESS
    - "failed", "error"                  -> FAILED
    - "skipped", "skip", "noop", "no-op" -> SKIPPED

    Toute valeur inconnue est ramenée à SUCCESS pour éviter les faux FAILED
    silencieux quand un pipeline renvoie un payload partiel.
    """
    if isinstance(value, PipelineStatus):
        return value

    if value is None:
        return PipelineStatus.SUCCESS

    raw = str(value).strip().lower()

    if raw in {"success", "ok", "done", "completed"}:
        return PipelineStatus.SUCCESS
    if raw in {"failed", "error", "exception"}:
        return PipelineStatus.FAILED
    if raw in {"skipped", "skip", "noop", "no-op", "no_op"}:
        return PipelineStatus.SKIPPED

    return PipelineStatus.SUCCESS


def _normalize_payload(payload: Any) -> dict[str, Any]:
    """
    Normalise la sortie métier retournée par un pipeline concret.

    Contrat attendu:
    {
        "status": "success|failed|skipped|noop" | PipelineStatus,
        "rows_read": int,
        "rows_written": int,
        "rows_skipped": int,
        "warnings": list[str],
        "metrics": dict[str, Any],
        "error_message": str | None,
    }

    Compatibilité volontairement minimale:
    - None => structure vide SUCCESS
    - dict partiel => champs manquants complétés
    - toute autre valeur => placée dans metrics.raw_payload
    """
    if payload is None:
        return {
            "status": PipelineStatus.SUCCESS,
            "rows_read": 0,
            "rows_written": 0,
            "rows_skipped": 0,
            "warnings": [],
            "metrics": {},
            "error_message": None,
        }

    if isinstance(payload, Mapping):
        return {
            "status": _coerce_status(payload.get("status")),
            "rows_read": _coerce_int(payload.get("rows_read", 0)),
            "rows_written": _coerce_int(payload.get("rows_written", 0)),
            "rows_skipped": _coerce_int(payload.get("rows_skipped", 0)),
            "warnings": _coerce_warnings(payload.get("warnings", [])),
            "metrics": _coerce_metrics(payload.get("metrics", {})),
            "error_message": _coerce_error_message(payload.get("error_message")),
        }

    return {
        "status": PipelineStatus.SUCCESS,
        "rows_read": 0,
        "rows_written": 0,
        "rows_skipped": 0,
        "warnings": [],
        "metrics": {"raw_payload": payload},
        "error_message": None,
    }


def run_pipeline(
    pipeline_name: str,
    fn: Callable[..., Any],
    *args: Any,
    **kwargs: Any,
) -> PipelineResult:
    """
    Exécute une unité pipeline métier et retourne un PipelineResult canonique.

    Notes:
    - Aucun pipeline concret ne doit instancier PipelineResult directement.
    - Toute exception non gérée devient un status FAILED.
    - Aucune logique PIT métier n'est implémentée ici.
    """
    started_at = datetime.utcnow()

    try:
        raw_payload = fn(*args, **kwargs)
        payload = _normalize_payload(raw_payload)
        finished_at = datetime.utcnow()

        return PipelineResult(
            pipeline_name=pipeline_name,
            status=payload["status"],
            started_at=started_at,
            finished_at=finished_at,
            rows_read=payload["rows_read"],
            rows_written=payload["rows_written"],
            rows_skipped=payload["rows_skipped"],
            warnings=payload["warnings"],
            metrics=payload["metrics"],
            error_message=payload["error_message"],
        )

    except Exception as exc:
        finished_at = datetime.utcnow()

        return PipelineResult(
            pipeline_name=pipeline_name,
            status=PipelineStatus.FAILED,
            started_at=started_at,
            finished_at=finished_at,
            rows_read=0,
            rows_written=0,
            rows_skipped=0,
            warnings=[],
            metrics={},
            error_message=str(exc),
        )
