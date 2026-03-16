"""
pipeline_status.py

Définit les statuts standard utilisés par tous les pipelines.

Pourquoi ce fichier existe
--------------------------

Actuellement dans beaucoup de projets data, chaque pipeline invente
ses propres statuts ("OK", "SUCCESS", "DONE", etc.).

Cela rend l'orchestration difficile.

Ce module définit un ensemble minimal et stable.
"""

from __future__ import annotations


class PipelineStatus:
    """
    Statuts possibles d'un pipeline.
    """

    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    PARTIAL = "PARTIAL"
