#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${PROJECT_ROOT}"

mkdir -p logs
timestamp="$(date +%Y%m%d_%H%M%S)"
log_file="logs/write_final_pipeline_convergence_patch_${timestamp}.log"
exec > >(tee "${log_file}") 2>&1

echo "===== DATE ====="
date
echo "===== PROJECT_ROOT ====="
pwd

# -----------------------------------------------------------------------------
# 1) Réécriture complète du runner canonique
#
# Ajouts:
# - support des statuts payload "success" / "failed" / "skipped" / "noop"
# - support d'un error_message retourné par payload
# - conservation du contrat PipelineResult canonique
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
PYEOF

echo "REWROTE: stock_quant/shared/pipeline_runner.py"

# -----------------------------------------------------------------------------
# 2) Script Python de patch ciblé pour les pipelines short restants
#
# Objectif:
# - éliminer les constructions locales de PipelineResult
# - faire converger vers run_pipeline(...)
# - conserver la logique SQL / PIT locale
# -----------------------------------------------------------------------------
cat > scripts/patch_remaining_pipeline_results.py <<'PYEOF'
from __future__ import annotations

from pathlib import Path
import re


TARGETS = [
    Path("stock_quant/pipelines/build_short_features_pipeline.py"),
    Path("stock_quant/pipelines/build_short_interest_pipeline.py"),
    Path("stock_quant/pipelines/build_daily_short_volume_pipeline.py"),
]

BUILD_PAYLOAD_TEMPLATE = '''
    def _build_payload(
        self,
        *,
        status: str,
        started_at: datetime,
        finished_at: datetime,
        rows_read: int,
        rows_written: int,
        error_message: str | None,
        metrics: dict[str, Any],
    ) -> dict[str, Any]:
        """
        Construit un payload canonique pour run_pipeline(...).

        IMPORTANT:
        - aucune construction directe de PipelineResult ici
        - le runner partagé convertit ce payload en PipelineResult
        - les statuts "noop" / "skipped" sont conservés puis normalisés
        """
        return {
            "status": status,
            "rows_read": rows_read,
            "rows_written": rows_written,
            "rows_skipped": 0,
            "warnings": [],
            "metrics": metrics,
            "error_message": error_message,
        }
'''

BUILD_RESULT_TEMPLATE = '''
    def _build_payload(
        self,
        *,
        status: str,
        started_at: datetime,
        finished_at: datetime,
        rows_read: int,
        rows_written: int,
        metrics: dict[str, Any],
        error_message: str | None,
    ) -> dict[str, Any]:
        """
        Construit un payload canonique pour run_pipeline(...).

        IMPORTANT:
        - aucune construction directe de PipelineResult ici
        - le runner partagé convertit ce payload en PipelineResult
        - les statuts "noop" / "skipped" sont conservés puis normalisés
        """
        return {
            "status": status,
            "rows_read": rows_read,
            "rows_written": rows_written,
            "rows_skipped": 0,
            "warnings": [],
            "metrics": metrics,
            "error_message": error_message,
        }
'''

RUN_WRAPPER = '''

    def run(self) -> PipelineResult:
        """
        Point d'entrée canonique.

        Toute l'exécution métier reste dans _run_impl() pour préserver
        la logique SQL/PIT déjà présente dans le module.
        """
        return run_pipeline(self.pipeline_name, self._run_impl)
'''

def ensure_import(text: str, line: str) -> str:
    if line in text:
        return text

    lines = text.splitlines()
    insert_at = 0
    for i, raw in enumerate(lines):
        if raw.startswith("from ") or raw.startswith("import "):
            insert_at = i + 1
    lines.insert(insert_at, line)
    return "\n".join(lines) + ("\n" if text.endswith("\n") else "")


def patch_file(path: Path) -> None:
    text = path.read_text(encoding="utf-8")
    original = text

    bak4 = path.with_suffix(path.suffix + ".bak4")
    bak4.write_text(text, encoding="utf-8")

    # injecter run_pipeline import
    text = ensure_import(text, "from stock_quant.shared.pipeline_runner import run_pipeline")

    # 1) short_features / short_interest: _build_pipeline_result -> _build_payload
    if "def _build_pipeline_result(" in text:
        text = re.sub(
            r"""
            \n    def\ _build_pipeline_result\(
            .*?
            \n    def\ 
            """,
            "\n" + BUILD_PAYLOAD_TEMPLATE + "\n    def ",
            text,
            flags=re.DOTALL | re.VERBOSE,
            count=1,
        )

        text = text.replace("self._build_pipeline_result(", "self._build_payload(")

    # 2) daily_short_volume: _build_result -> _build_payload
    if "def _build_result(" in text:
        text = re.sub(
            r"""
            \n    def\ _build_result\(
            .*?
            \n    def\ run\(self\)\ \-\>\ PipelineResult:
            """,
            "\n" + BUILD_RESULT_TEMPLATE + "\n    def _run_impl(self) -> dict[str, Any]:",
            text,
            flags=re.DOTALL | re.VERBOSE,
            count=1,
        )
        text = text.replace("self._build_result(", "self._build_payload(")

    # 3) short_features / short_interest: renommer run -> _run_impl
    text = text.replace("def run(self) -> PipelineResult:", "def _run_impl(self) -> dict[str, Any]:", 1)

    # 4) ajouter le run canonique si absent
    if "return run_pipeline(self.pipeline_name, self._run_impl)" not in text:
        text = text.rstrip() + RUN_WRAPPER + "\n"

    # sécurité: aucune construction locale PipelineResult ne doit rester
    if "return PipelineResult(" in text:
        raise RuntimeError(f"Local PipelineResult construction still present in {path}")

    path.write_text(text, encoding="utf-8")
    print(f"PATCHED: {path}")


for target in TARGETS:
    if not target.exists():
        raise SystemExit(f"Missing target: {target}")
    patch_file(target)
PYEOF

chmod +x scripts/patch_remaining_pipeline_results.py
echo "WROTE: scripts/patch_remaining_pipeline_results.py"

# -----------------------------------------------------------------------------
# 3) Test de garde du runner canonique
# -----------------------------------------------------------------------------
cat > tests/unit/shared/test_pipeline_runner_status_mapping.py <<'PYEOF'
from stock_quant.shared.enums import PipelineStatus
from stock_quant.shared.pipeline_runner import run_pipeline


def test_runner_maps_noop_to_skipped() -> None:
    def _fake():
        return {
            "status": "noop",
            "rows_read": 0,
            "rows_written": 0,
            "rows_skipped": 0,
            "metrics": {"build_decision": "noop_no_raw_rows"},
            "error_message": None,
        }

    result = run_pipeline("demo_noop", _fake)
    assert result.status == PipelineStatus.SKIPPED
    assert result.metrics["build_decision"] == "noop_no_raw_rows"


def test_runner_maps_success_payload() -> None:
    def _fake():
        return {
            "status": "success",
            "rows_read": 10,
            "rows_written": 7,
            "rows_skipped": 3,
            "metrics": {"demo_metric": 1},
            "warnings": ["ok"],
            "error_message": None,
        }

    result = run_pipeline("demo_success", _fake)
    assert result.status == PipelineStatus.SUCCESS
    assert result.rows_read == 10
    assert result.rows_written == 7
    assert result.rows_skipped == 3
    assert result.warnings == ["ok"]
PYEOF

echo "WROTE: tests/unit/shared/test_pipeline_runner_status_mapping.py"

# -----------------------------------------------------------------------------
# 4) Test de garde des pipelines short convergés
# -----------------------------------------------------------------------------
cat > tests/unit/pipelines/test_remaining_short_pipelines_use_runner.py <<'PYEOF'
from pathlib import Path

TARGETS = [
    Path("stock_quant/pipelines/build_short_features_pipeline.py"),
    Path("stock_quant/pipelines/build_short_interest_pipeline.py"),
    Path("stock_quant/pipelines/build_daily_short_volume_pipeline.py"),
]


def test_remaining_short_pipelines_no_longer_construct_pipeline_result_locally() -> None:
    for path in TARGETS:
        text = path.read_text(encoding="utf-8")
        assert "return PipelineResult(" not in text, f"local PipelineResult remains in {path}"
        assert "return run_pipeline(self.pipeline_name, self._run_impl)" in text, f"runner not wired in {path}"
PYEOF

echo "WROTE: tests/unit/pipelines/test_remaining_short_pipelines_use_runner.py"

echo "===== DONE ====="
