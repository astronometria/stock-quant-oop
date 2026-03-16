"""
time_utils.py

Helpers temporels utilisés dans tout le projet.

Pourquoi ce module
------------------

Dans les pipelines data, il est très fréquent de manipuler des timestamps.

On centralise ici quelques helpers simples pour éviter :
- duplication
- erreurs de timezone
- incohérences entre pipelines
"""

from __future__ import annotations

from datetime import datetime, timezone


def utcnow() -> datetime:
    """
    Retourne un datetime UTC avec timezone.
    """
    return datetime.now(timezone.utc)


def utcnow_naive() -> datetime:
    """
    Retourne un datetime UTC naïf.

    Beaucoup de systèmes analytiques utilisent des timestamps naïfs
    stockés implicitement en UTC.
    """
    return datetime.now(timezone.utc).replace(tzinfo=None)


def isoformat_utc(dt: datetime) -> str:
    """
    Convertit un datetime en ISO string sans timezone.

    Utile pour les résultats de pipeline.
    """
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt.isoformat()
