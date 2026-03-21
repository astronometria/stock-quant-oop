#!/usr/bin/env bash
set -euo pipefail

# -----------------------------------------------------------------------------
# Refactor brutal du coeur pipeline
# Objectif:
# - garder UNE seule vérité pour PipelineResult
# - garder UNE seule vérité pour PipelineStatus
# - garder UN seul runner standard
#
# Ligne rouge absolue:
# - ne toucher à aucune logique PIT / survivorship / reproductibilité métier
# - on casse seulement l'orchestration pipeline dupliquée
# -----------------------------------------------------------------------------

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${PROJECT_ROOT}"

mkdir -p logs

timestamp="$(date +%Y%m%d_%H%M%S)"
log_file="logs/refactor_pipeline_core_${timestamp}.log"

exec > >(tee "${log_file}") 2>&1

echo "===== DATE ====="
date
echo "===== PROJECT_ROOT ====="
pwd

# -----------------------------------------------------------------------------
# Fonction utilitaire: backup versionné en .bak avant toute réécriture/suppression
# -----------------------------------------------------------------------------
backup_if_exists() {
  local path="$1"
  if [ -f "${path}" ]; then
    cp "${path}" "${path}.bak"
    echo "BACKUP: ${path} -> ${path}.bak"
  else
    echo "SKIP BACKUP (missing): ${path}"
  fi
}

# -----------------------------------------------------------------------------
# 1) Backups des fichiers critiques avant casse
# -----------------------------------------------------------------------------
backup_if_exists "stock_quant/app/dto/pipeline_result.py"
backup_if_exists "stock_quant/shared/enums.py"
backup_if_exists "stock_quant/shared/pipeline_runner.py"
backup_if_exists "stock_quant/shared/pipeline_result.py"
backup_if_exists "stock_quant/shared/pipeline_status.py"
backup_if_exists "stock_quant/pipelines/base_pipeline.py"

# -----------------------------------------------------------------------------
# 2) Réécriture agressive du runner canonique
#
# Règles:
# - le runner dépend UNIQUEMENT du DTO canonique et du Enum canonique
# - le runner est le seul endroit qui fabrique un PipelineResult
# - les pipelines concrets doivent renvoyer un dict normalisé
# -----------------------------------------------------------------------------
cat > stock_quant/shared/pipeline_runner.py <<'PYEOF'
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
PYEOF

echo "REWROTE: stock_quant/shared/pipeline_runner.py"

# -----------------------------------------------------------------------------
# 3) Suppression brutale des sources de vérité concurrentes
# -----------------------------------------------------------------------------
for f in \
  stock_quant/shared/pipeline_result.py \
  stock_quant/shared/pipeline_status.py \
  stock_quant/pipelines/base_pipeline.py
do
  if [ -f "${f}" ]; then
    rm -f "${f}"
    echo "REMOVED: ${f}"
  else
    echo "ALREADY MISSING: ${f}"
  fi
done

# -----------------------------------------------------------------------------
# 4) Remplacements d'imports agressifs
# -----------------------------------------------------------------------------
python3 <<'PYEOF'
from pathlib import Path

ROOTS = [Path("stock_quant"), Path("cli"), Path("tests")]

REPLACEMENTS = [
    (
        "from stock_quant.shared.pipeline_result import PipelineResult",
        "from stock_quant.app.dto.pipeline_result import PipelineResult",
    ),
    (
        "from stock_quant.shared.pipeline_status import PipelineStatus",
        "from stock_quant.shared.enums import PipelineStatus",
    ),
    (
        "import stock_quant.shared.pipeline_result",
        "import stock_quant.app.dto.pipeline_result",
    ),
    (
        "import stock_quant.shared.pipeline_status",
        "import stock_quant.shared.enums",
    ),
]

# Remplacement simple et volontairement brutal.
# On préfère casser tôt plutôt que de laisser des ambiguïtés silencieuses.
for root in ROOTS:
    if not root.exists():
        continue

    for path in root.rglob("*.py"):
        text = path.read_text(encoding="utf-8")
        original = text

        for old, new in REPLACEMENTS:
            text = text.replace(old, new)

        # On neutralise les imports directs de BasePipeline.
        # On ne tente PAS de compatibilité douce.
        text = text.replace(
            "from stock_quant.pipelines.base_pipeline import BasePipeline",
            "# REMOVED: BasePipeline supprimé pendant la consolidation brutale",
        )
        text = text.replace(
            "import stock_quant.pipelines.base_pipeline",
            "# REMOVED: base_pipeline supprimé pendant la consolidation brutale",
        )

        if text != original:
            path.write_text(text, encoding="utf-8")
            print(f"UPDATED IMPORTS: {path}")
PYEOF

# -----------------------------------------------------------------------------
# 5) Test de garde pour empêcher le retour des doublons
# -----------------------------------------------------------------------------
cat > tests/unit/shared/test_pipeline_contract_guard.py <<'PYEOF'
from pathlib import Path

from stock_quant.app.dto.pipeline_result import PipelineResult
from stock_quant.shared.enums import PipelineStatus
from stock_quant.shared.pipeline_runner import run_pipeline


def test_pipeline_contract_duplicates_removed() -> None:
    assert not Path("stock_quant/shared/pipeline_result.py").exists()
    assert not Path("stock_quant/shared/pipeline_status.py").exists()
    assert not Path("stock_quant/pipelines/base_pipeline.py").exists()


def test_pipeline_runner_uses_canonical_contract() -> None:
    def _fake_pipeline() -> dict:
        return {
            "rows_read": 10,
            "rows_written": 7,
            "rows_skipped": 3,
            "warnings": ["demo-warning"],
            "metrics": {"demo_metric": 1},
        }

    result = run_pipeline("demo_pipeline", _fake_pipeline)

    assert isinstance(result, PipelineResult)
    assert result.status == PipelineStatus.SUCCESS
    assert result.rows_read == 10
    assert result.rows_written == 7
    assert result.rows_skipped == 3
    assert result.warnings == ["demo-warning"]
    assert result.metrics["demo_metric"] == 1
    assert result.error_message is None
PYEOF

echo "WROTE: tests/unit/shared/test_pipeline_contract_guard.py"

# -----------------------------------------------------------------------------
# 6) Rapport final rapide
# -----------------------------------------------------------------------------
echo "===== POST-REFACTOR TARGET FILES ====="
for f in \
  stock_quant/app/dto/pipeline_result.py \
  stock_quant/shared/enums.py \
  stock_quant/shared/pipeline_runner.py \
  stock_quant/shared/pipeline_result.py \
  stock_quant/shared/pipeline_status.py \
  stock_quant/pipelines/base_pipeline.py \
  tests/unit/shared/test_pipeline_contract_guard.py
do
  if [ -f "${f}" ]; then
    echo "FOUND: ${f}"
  else
    echo "MISSING: ${f}"
  fi
done

echo "===== GREP FOR FORBIDDEN IMPORTS ====="
grep -RIn --exclude-dir=.git --exclude-dir=.venv --exclude-dir=venv --exclude-dir=data \
  -E "shared\.pipeline_result|shared\.pipeline_status|pipelines\.base_pipeline" \
  stock_quant cli tests || true

echo "===== SCRIPT DONE ====="
