#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${PROJECT_ROOT}"

mkdir -p logs
timestamp="$(date +%Y%m%d_%H%M%S)"
log_file="logs/converge_build_prices_pipeline_${timestamp}.log"
exec > >(tee "${log_file}") 2>&1

echo "===== DATE ====="
date
echo "===== PROJECT_ROOT ====="
pwd

# ---------------------------------------------------------------------------
# Backups locaux avant réécriture
# ---------------------------------------------------------------------------
for f in \
  stock_quant/pipelines/build_prices_pipeline.py \
  stock_quant/pipelines/prices_pipeline.py
do
  if [ -f "$f" ]; then
    cp "$f" "$f.bak9"
    echo "BACKUP: $f -> $f.bak9"
  fi
done

# ---------------------------------------------------------------------------
# build_prices_pipeline.py
# ---------------------------------------------------------------------------
cat > stock_quant/pipelines/build_prices_pipeline.py <<'PYEOF'
from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from stock_quant.app.services.price_ingestion_service import (
    PriceIngestionResult,
    PriceIngestionService,
)
from stock_quant.app.dto.pipeline_result import PipelineResult
from stock_quant.shared.exceptions import PipelineError
from stock_quant.shared.pipeline_runner import run_pipeline


class BuildPricesPipeline:
    """
    Pipeline canonique du domaine prix.

    Responsabilités
    ----------------
    - déléguer l'ingestion incrémentale à PriceIngestionService
    - converger vers le contrat pipeline canonique du repo
    - exposer un point d'entrée stable au CLI / orchestrateur

    Notes anti-biais
    ----------------
    - `price_history` est la seule table normalized canonique
    - `price_latest` est une table de service / serving seulement
    - `price_latest` ne doit jamais servir aux backtests, features ou labels
    - la recherche PIT doit toujours lire depuis `price_history`
    """

    pipeline_name = "build_prices"

    def __init__(self, price_ingestion_service: PriceIngestionService) -> None:
        # Service métier mince:
        # toute la logique de validation / ingestion / upsert reste là-bas.
        self.price_ingestion_service = price_ingestion_service

    def _to_payload(self, result: PriceIngestionResult) -> dict[str, Any]:
        """
        Convertit le résultat métier prix en payload canonique pour run_pipeline(...).

        Convention retenue:
        - rows_read     = nombre de symboles demandés
        - rows_written  = nombre de lignes écrites dans price_history
        - rows_skipped  = symboles demandés mais non récupérés
        - metrics       = détails métier prix auditables

        On conserve les informations spécifiques prix dans metrics
        plutôt que dans un DTO pipeline concurrent.
        """
        requested_symbols = int(result.requested_symbols)
        fetched_symbols = int(result.fetched_symbols)
        written_rows = int(result.written_price_history_rows)

        return {
            "status": "success",
            "rows_read": requested_symbols,
            "rows_written": written_rows,
            "rows_skipped": max(0, requested_symbols - fetched_symbols),
            "warnings": [],
            "metrics": {
                "requested_symbols": requested_symbols,
                "fetched_symbols": fetched_symbols,
                "written_price_history_rows": written_rows,
                "price_latest_rows_after_refresh": int(result.price_latest_rows_after_refresh),
                "start_date": result.start_date,
                "end_date": result.end_date,
            },
            "error_message": None,
        }

    def _run_impl(
        self,
        symbols: Iterable[str] | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> dict[str, Any]:
        """
        Exécution métier locale.

        Important:
        - aucune construction directe de PipelineResult ici
        - aucune logique PIT additionnelle n'est déplacée ici
        - le runner partagé reste la seule fabrique du PipelineResult canonique
        """
        try:
            result = self.price_ingestion_service.ingest_incremental(
                symbols=symbols,
                start_date=start_date,
                end_date=end_date,
            )
            return self._to_payload(result)
        except Exception as exc:
            if isinstance(exc, PipelineError):
                raise
            raise PipelineError(f"failed to run build_prices pipeline: {exc}") from exc

    def run(
        self,
        symbols: Iterable[str] | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> PipelineResult:
        """
        Point d'entrée canonique du pipeline prix.
        """
        return run_pipeline(
            self.pipeline_name,
            self._run_impl,
            symbols=symbols,
            start_date=start_date,
            end_date=end_date,
        )


# ----------------------------------------------------------------------
# Alias de compatibilité
# ----------------------------------------------------------------------
PricesPipeline = BuildPricesPipeline

__all__ = [
    "BuildPricesPipeline",
    "PricesPipeline",
]
PYEOF

echo "REWROTE: stock_quant/pipelines/build_prices_pipeline.py"

# ---------------------------------------------------------------------------
# prices_pipeline.py
# ---------------------------------------------------------------------------
cat > stock_quant/pipelines/prices_pipeline.py <<'PYEOF'
"""
Compatibility wrapper for the prices pipeline.

Historique
----------
Le pipeline canonique a été renommé pour suivre la convention :
build_*_pipeline.py

Ancien fichier :
prices_pipeline.py

Nouveau fichier :
build_prices_pipeline.py

Ce wrapper reste présent temporairement pour éviter de casser
les imports existants dans l'orchestrateur prix et dans les CLI.
"""

from __future__ import annotations

from stock_quant.pipelines.build_prices_pipeline import (
    BuildPricesPipeline,
    PricesPipeline,
)

__all__ = [
    "BuildPricesPipeline",
    "PricesPipeline",
]
PYEOF

echo "REWROTE: stock_quant/pipelines/prices_pipeline.py"

# ---------------------------------------------------------------------------
# Test de garde
# ---------------------------------------------------------------------------
cat > tests/unit/pipelines/test_build_prices_pipeline_converged.py <<'PYEOF'
from pathlib import Path


def test_build_prices_pipeline_no_longer_uses_special_result_contract() -> None:
    text = Path("stock_quant/pipelines/build_prices_pipeline.py").read_text(encoding="utf-8")

    assert "BuildPricesPipelineResult" not in text
    assert "PricesPipelineResult" not in text
    assert "return run_pipeline(" in text
    assert "def _to_payload(" in text
    assert "\"written_price_history_rows\"" in text
    assert "\"price_latest_rows_after_refresh\"" in text


def test_prices_pipeline_wrapper_only_reexports_pipeline_classes() -> None:
    text = Path("stock_quant/pipelines/prices_pipeline.py").read_text(encoding="utf-8")

    assert "BuildPricesPipelineResult" not in text
    assert "PricesPipelineResult" not in text
    assert "BuildPricesPipeline" in text
    assert "PricesPipeline" in text
PYEOF

echo "WROTE: tests/unit/pipelines/test_build_prices_pipeline_converged.py"

echo "===== DONE ====="
