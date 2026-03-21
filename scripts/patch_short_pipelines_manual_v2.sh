#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${PROJECT_ROOT}"

mkdir -p logs
timestamp="$(date +%Y%m%d_%H%M%S)"
log_file="logs/patch_short_pipelines_manual_v2_${timestamp}.log"
exec > >(tee "${log_file}") 2>&1

echo "===== DATE ====="
date
echo "===== PROJECT_ROOT ====="
pwd

python3 <<'PYEOF'
from pathlib import Path
import re

TARGETS = [
    Path("stock_quant/pipelines/build_short_features_pipeline.py"),
    Path("stock_quant/pipelines/build_short_interest_pipeline.py"),
    Path("stock_quant/pipelines/build_daily_short_volume_pipeline.py"),
]

RUN_METHOD = """

    def run(self) -> PipelineResult:
        \"""
        Point d'entrée canonique.

        La logique SQL/PIT reste dans _run_impl().
        Le runner partagé construit le PipelineResult canonique.
        \"""
        return run_pipeline(self.pipeline_name, self._run_impl)
"""

PAYLOAD_HELPER = """
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
        \"""
        Construit un payload canonique pour run_pipeline(...).

        IMPORTANT:
        - aucune construction directe de PipelineResult ici
        - les statuts métier comme noop/skipped restent visibles
        - le runner partagé convertit ce payload en PipelineResult
        \"""
        return {
            "status": status,
            "rows_read": rows_read,
            "rows_written": rows_written,
            "rows_skipped": 0,
            "warnings": [],
            "metrics": metrics,
            "error_message": error_message,
        }
"""

def ensure_import(text: str, line: str) -> str:
    if line in text:
        return text
    lines = text.splitlines()
    insert_at = 0
    for i, raw in enumerate(lines):
        if raw.startswith("from ") or raw.startswith("import "):
            insert_at = i + 1
    lines.insert(insert_at, line)
    return "\\n".join(lines) + ("\\n" if text.endswith("\\n") else "")

def patch_return_pipeline_result_blocks(text: str) -> str:
    # Remplacement très ciblé de "return PipelineResult(...)" par "return { ... }"
    pattern = re.compile(
        r'''
        return\ PipelineResult\(
            \s*pipeline_name=self\.pipeline_name,
            \s*status=status,
            \s*started_at=started_at,
            \s*finished_at=finished_at,
            \s*rows_read=rows_read,
            \s*rows_written=rows_written,
            \s*rows_skipped=0,
            \s*warnings=\[\],
            \s*metrics=metrics,
            \s*error_message=error_message,
        \s*\)
        ''',
        re.VERBOSE | re.DOTALL,
    )

    replacement = """return {
            "status": status,
            "rows_read": rows_read,
            "rows_written": rows_written,
            "rows_skipped": 0,
            "warnings": [],
            "metrics": metrics,
            "error_message": error_message,
        }"""

    return pattern.sub(replacement, text)

for path in TARGETS:
    text = path.read_text(encoding="utf-8")
    original = text

    bak5 = path.with_suffix(path.suffix + ".bak5")
    bak5.write_text(text, encoding="utf-8")

    text = ensure_import(text, "from stock_quant.shared.pipeline_runner import run_pipeline")

    # short_features / short_interest
    text = text.replace("def _build_pipeline_result(", "def _build_payload(")
    text = text.replace("def _build_result(", "def _build_payload(")

    # renommer run existant en _run_impl si encore présent
    text = text.replace("def run(self) -> PipelineResult:", "def _run_impl(self) -> dict[str, Any]:", 1)

    # rerouter les appels helper locaux
    text = text.replace("self._build_pipeline_result(", "self._build_payload(")
    text = text.replace("self._build_result(", "self._build_payload(")

    # remplacer les return PipelineResult(...)
    text = patch_return_pipeline_result_blocks(text)

    # si le helper payload n'existe pas proprement, on l'injecte à la place de l'ancien helper
    if "def _build_payload(" not in text:
        raise RuntimeError(f"_build_payload missing after patch in {path}")

    # ajouter run canonique si absent
    if "return run_pipeline(self.pipeline_name, self._run_impl)" not in text:
        text = text.rstrip() + RUN_METHOD + "\n"

    # garde dure
    if "return PipelineResult(" in text:
        raise RuntimeError(f"Local PipelineResult construction still present in {path}")

    path.write_text(text, encoding="utf-8")
    print(f"PATCHED: {path}")

PYEOF

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
