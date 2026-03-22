#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${PROJECT_ROOT}"

mkdir -p logs
timestamp="$(date +%Y%m%d_%H%M%S)"
log_file="logs/fix_short_pipeline_indent_and_cleanup_${timestamp}.log"
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

HELPER = '''    def _build_payload(
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

RUN_WRAPPER = '''    def run(self) -> PipelineResult:
        """
        Point d'entrée canonique.
        """
        return run_pipeline(self.pipeline_name, self._run_impl)
'''

for path in TARGETS:
    text = path.read_text(encoding="utf-8")
    bak = path.with_suffix(path.suffix + ".bak7")
    bak.write_text(text, encoding="utf-8")

    # Remplace tout helper cassé entre _build_payload(...) et la def suivante.
    text = re.sub(
        r"^    def _build_payload\([\s\S]*?(?=^    def )",
        HELPER,
        text,
        count=1,
        flags=re.MULTILINE,
    )

    # Si run wrapper absent, l'ajoute à la fin.
    if "return run_pipeline(self.pipeline_name, self._run_impl)" not in text:
        text = text.rstrip() + "\n\n" + RUN_WRAPPER + "\n"

    # Garde dure
    if "return PipelineResult(" in text:
        raise RuntimeError(f"local PipelineResult remains in {path}")

    compile(text, str(path), "exec")
    path.write_text(text, encoding="utf-8")
    print(f"FIXED: {path}")

PYEOF

# durcir .gitignore pour tous les backups générés pendant le refactor
python3 <<'PYEOF'
from pathlib import Path

path = Path(".gitignore")
text = path.read_text(encoding="utf-8") if path.exists() else ""

lines_to_add = [
    "*.bak",
    "*.bak2",
    "*.bak3",
    "*.bak4",
    "*.bak5",
    "*.bak6",
    "*.bak7",
]

changed = False
for line in lines_to_add:
    if line not in text.splitlines():
        text = text.rstrip() + ("\n" if text and not text.endswith("\n") else "") + line + "\n"
        changed = True

if changed:
    path.write_text(text, encoding="utf-8")
    print("UPDATED: .gitignore")
else:
    print("UNCHANGED: .gitignore")
PYEOF

echo "===== DONE ====="
