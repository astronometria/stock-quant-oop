#!/usr/bin/env bash
set -euo pipefail

# -----------------------------------------------------------------------------
# Patch chirurgical des 3 pipelines short restants.
#
# Objectif:
# - supprimer les constructions locales de PipelineResult
# - conserver toute la logique SQL / PIT / overlap / as-of join existante
# - faire converger vers run_pipeline(...)
#
# IMPORTANT:
# - on ne touche PAS à build_prices_pipeline.py dans cette passe
# - on ne modifie PAS les requêtes métier
# - on ne déplace PAS les protections PIT / anti-survivorship
# -----------------------------------------------------------------------------

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${PROJECT_ROOT}"

mkdir -p logs
timestamp="$(date +%Y%m%d_%H%M%S)"
log_file="logs/patch_short_pipelines_exact_v3_${timestamp}.log"
exec > >(tee "${log_file}") 2>&1

echo "===== DATE ====="
date
echo "===== PROJECT_ROOT ====="
pwd

python3 <<'PYEOF'
from pathlib import Path
import re

SHORT_FEATURES = Path("stock_quant/pipelines/build_short_features_pipeline.py")
SHORT_INTEREST = Path("stock_quant/pipelines/build_short_interest_pipeline.py")
DAILY_SHORT_VOL = Path("stock_quant/pipelines/build_daily_short_volume_pipeline.py")


def backup(path: Path, suffix: str) -> None:
    backup_path = path.with_suffix(path.suffix + suffix)
    backup_path.write_text(path.read_text(encoding="utf-8"), encoding="utf-8")
    print(f"BACKUP: {path} -> {backup_path}")


def ensure_runner_import(text: str) -> str:
    line = "from stock_quant.shared.pipeline_runner import run_pipeline"
    if line in text:
        return text

    lines = text.splitlines()
    insert_at = 0
    for i, raw in enumerate(lines):
        if raw.startswith("from ") or raw.startswith("import "):
            insert_at = i + 1
    lines.insert(insert_at, line)
    return "\n".join(lines) + ("\n" if text.endswith("\n") else "")


def replace_helper_block(
    text: str,
    old_name: str,
    next_def_name: str,
) -> str:
    pattern = re.compile(
        rf"""
(?P<indent>    )def\ {old_name}\(
.*?
(?=^\s{{4}}def\ {next_def_name}\()
""",
        re.DOTALL | re.MULTILINE | re.VERBOSE,
    )

    replacement = f'''    def _build_payload(
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
        - les statuts métier comme noop/skipped restent visibles
        - le runner partagé convertit ensuite ce payload en PipelineResult
        """
        return {{
            "status": status,
            "rows_read": rows_read,
            "rows_written": rows_written,
            "rows_skipped": 0,
            "warnings": [],
            "metrics": metrics,
            "error_message": error_message,
        }}

'''

    new_text, count = pattern.subn(replacement, text, count=1)
    if count != 1:
        raise RuntimeError(f"Impossible de remplacer le helper {old_name} avant {next_def_name}")
    return new_text


def patch_short_features_or_interest(path: Path) -> None:
    text = path.read_text(encoding="utf-8")
    original = text

    backup(path, ".bak6")

    text = ensure_runner_import(text)

    # 1) remplacer le helper local PipelineResult -> payload dict
    text = replace_helper_block(
        text=text,
        old_name="_build_pipeline_result",
        next_def_name="_table_columns" if "build_short_features" in path.name else "run",
    )

    # 2) renommer les appels au helper
    text = text.replace("self._build_pipeline_result(", "self._build_payload(")

    # 3) renommer l'ancien run métier en _run_impl
    text, run_count = re.subn(
        r"^    def run\(self\) -> PipelineResult:$",
        "    def _run_impl(self) -> dict[str, Any]:",
        text,
        count=1,
        flags=re.MULTILINE,
    )
    if run_count != 1:
        raise RuntimeError(f"Impossible de renommer run() dans {path}")

    # 4) ajouter le wrapper canonique run() si absent
    wrapper = '''

    def run(self) -> PipelineResult:
        """
        Point d'entrée canonique.

        La logique SQL/PIT reste dans _run_impl().
        Le runner partagé construit le PipelineResult canonique.
        """
        return run_pipeline(self.pipeline_name, self._run_impl)
'''
    if "return run_pipeline(self.pipeline_name, self._run_impl)" not in text:
        text = text.rstrip() + wrapper + "\n"

    # 5) garde dure
    if "return PipelineResult(" in text:
        raise RuntimeError(f"Local PipelineResult construction still present in {path}")

    path.write_text(text, encoding="utf-8")
    print(f"PATCHED: {path}")


def patch_daily_short_volume(path: Path) -> None:
    text = path.read_text(encoding="utf-8")
    original = text

    backup(path, ".bak6")

    text = ensure_runner_import(text)

    # 1) remplacer le helper local PipelineResult -> payload dict
    text = replace_helper_block(
        text=text,
        old_name="_build_result",
        next_def_name="run",
    )

    # 2) renommer les appels au helper
    text = text.replace("self._build_result(", "self._build_payload(")

    # 3) renommer l'ancien run métier en _run_impl
    text, run_count = re.subn(
        r"^    def run\(self\) -> PipelineResult:$",
        "    def _run_impl(self) -> dict[str, Any]:",
        text,
        count=1,
        flags=re.MULTILINE,
    )
    if run_count != 1:
        raise RuntimeError(f"Impossible de renommer run() dans {path}")

    # 4) ajouter le wrapper canonique run() si absent
    wrapper = '''

    def run(self) -> PipelineResult:
        """
        Point d'entrée canonique.

        La logique SQL/PIT reste dans _run_impl().
        Le runner partagé construit le PipelineResult canonique.
        """
        return run_pipeline(self.pipeline_name, self._run_impl)
'''
    if "return run_pipeline(self.pipeline_name, self._run_impl)" not in text:
        text = text.rstrip() + wrapper + "\n"

    # 5) garde dure
    if "return PipelineResult(" in text:
        raise RuntimeError(f"Local PipelineResult construction still present in {path}")

    path.write_text(text, encoding="utf-8")
    print(f"PATCHED: {path}")


patch_short_features_or_interest(SHORT_FEATURES)
patch_short_features_or_interest(SHORT_INTEREST)
patch_daily_short_volume(DAILY_SHORT_VOL)

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
        assert "def _build_pipeline_result(" not in text, f"legacy helper remains in {path}"
        assert "def _build_result(" not in text, f"legacy helper remains in {path}"
        assert "return run_pipeline(self.pipeline_name, self._run_impl)" in text, f"runner not wired in {path}"
PYEOF

echo "WROTE: tests/unit/pipelines/test_remaining_short_pipelines_use_runner.py"
echo "===== DONE ====="
