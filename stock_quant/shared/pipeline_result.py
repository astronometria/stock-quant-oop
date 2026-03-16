"""
pipeline_result.py

Objet standard retourné par tous les pipelines du projet.

Objectif
--------

Uniformiser la sortie des pipelines afin de :

- simplifier le logging
- simplifier l'orchestration
- permettre un monitoring uniforme
- éviter les formats divergents

Tous les pipelines doivent retourner cet objet.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class PipelineResult:
    """
    Résultat standard d'un pipeline.
    """

    pipeline_name: str
    status: str
    started_at: str
    finished_at: str
    duration_seconds: float
    rows_read: int
    rows_written: int
    rows_skipped: int
    error_message: str | None
    metrics: dict

    def summary_dict(self) -> dict:
        """
        Convertit le résultat en dictionnaire JSON-friendly.
        """
        return {
            "pipeline_name": self.pipeline_name,
            "status": self.status,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "duration_seconds": self.duration_seconds,
            "rows_read": self.rows_read,
            "rows_written": self.rows_written,
            "rows_skipped": self.rows_skipped,
            "error_message": self.error_message,
            "metrics": self.metrics,
        }
