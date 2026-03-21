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

    On reste volontairement strict mais robuste:
    - None => default
    - bool => 0/1 via int(bool)
    - int/float/str numérique => int(...)
    - sinon => default
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


def _normalize_payload(payload: Any) -> dict[str, Any]:
    """
    Normalise la sortie métier retournée par un pipeline concret.

    Contrat attendu:
    {
        "rows_read": int,
        "rows_written": int,
        "rows_skipped": int,
        "warnings": list[str],
        "metrics": dict[str, Any],
    }

    Compatibilité volontairement minimale:
    - None => structure vide
    - dict partiel => champs manquants complétés
    - toute autre valeur => placée dans metrics.raw_payload
    """
    if payload is None:
        return {
            "rows_read": 0,
            "rows_written": 0,
            "rows_skipped": 0,
            "warnings": [],
            "metrics": {},
        }

    if isinstance(payload, Mapping):
        return {
            "rows_read": _coerce_int(payload.get("rows_read", 0)),
            "rows_written": _coerce_int(payload.get("rows_written", 0)),
            "rows_skipped": _coerce_int(payload.get("rows_skipped", 0)),
            "warnings": _coerce_warnings(payload.get("warnings", [])),
            "metrics": _coerce_metrics(payload.get("metrics", {})),
        }

    return {
        "rows_read": 0,
        "rows_written": 0,
        "rows_skipped": 0,
        "warnings": [],
        "metrics": {"raw_payload": payload},
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
            status=PipelineStatus.SUCCESS,
            started_at=started_at,
            finished_at=finished_at,
            rows_read=payload["rows_read"],
            rows_written=payload["rows_written"],
            rows_skipped=payload["rows_skipped"],
            warnings=payload["warnings"],
            metrics=payload["metrics"],
            error_message=None,
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
