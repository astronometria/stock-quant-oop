"""
pipeline_runner.py

Runner standard pour exécuter les pipelines.

Ce module centralise :

- le timing
- la gestion d'erreur
- la construction du PipelineResult

Les pipelines peuvent ainsi rester très simples.
"""

from __future__ import annotations

from typing import Callable

from stock_quant.shared.pipeline_result import PipelineResult
from stock_quant.shared.pipeline_status import PipelineStatus
from stock_quant.shared.time_utils import utcnow, isoformat_utc


def run_pipeline(
    pipeline_name: str,
    func: Callable[[], dict],
) -> PipelineResult:
    """
    Exécute une fonction pipeline avec gestion standardisée.

    La fonction fournie doit retourner un dict contenant :

        rows_read
        rows_written
        rows_skipped
        metrics

    Exemple :

        run_pipeline(
            "build_prices",
            lambda: service.build()
        )
    """

    started = utcnow()

    try:
        result = func()

        finished = utcnow()
        duration = (finished - started).total_seconds()

        return PipelineResult(
            pipeline_name=pipeline_name,
            status=PipelineStatus.SUCCESS,
            started_at=isoformat_utc(started),
            finished_at=isoformat_utc(finished),
            duration_seconds=duration,
            rows_read=result.get("rows_read", 0),
            rows_written=result.get("rows_written", 0),
            rows_skipped=result.get("rows_skipped", 0),
            error_message=None,
            metrics=result.get("metrics", {}),
        )

    except Exception as exc:
        finished = utcnow()
        duration = (finished - started).total_seconds()

        return PipelineResult(
            pipeline_name=pipeline_name,
            status=PipelineStatus.FAILED,
            started_at=isoformat_utc(started),
            finished_at=isoformat_utc(finished),
            duration_seconds=duration,
            rows_read=0,
            rows_written=0,
            rows_skipped=0,
            error_message=str(exc),
            metrics={},
        )
