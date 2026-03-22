#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "${PROJECT_ROOT}"

mkdir -p logs
timestamp="$(date +%Y%m%d_%H%M%S)"
log_file="logs/fix_short_payload_indent_v2_${timestamp}.log"
exec > >(tee "${log_file}") 2>&1

echo "===== DATE ====="
date
echo "===== PROJECT_ROOT ====="
pwd

python3 <<'PYEOF'
from pathlib import Path

TARGETS = [
    Path("stock_quant/pipelines/build_short_features_pipeline.py"),
    Path("stock_quant/pipelines/build_short_interest_pipeline.py"),
    Path("stock_quant/pipelines/build_daily_short_volume_pipeline.py"),
]

HELPER_START = "def _build_payload("
RUN_WRAPPER_SENTINEL = "return run_pipeline(self.pipeline_name, self._run_impl)"


def fix_file(path: Path) -> None:
    text = path.read_text(encoding="utf-8")
    bak = path.with_suffix(path.suffix + ".bak8")
    bak.write_text(text, encoding="utf-8")

    lines = text.splitlines()
    out = []

    i = 0
    while i < len(lines):
        line = lines[i]
        stripped = line.lstrip()

        # détecte un helper mal indenté à l'intérieur d'une méthode
        if stripped.startswith(HELPER_START):
            # force le début du helper à 4 espaces
            out.append("    " + stripped)
            i += 1

            # les lignes du bloc helper restent à +8 espaces jusqu'au prochain def de classe
            while i < len(lines):
                current = lines[i]
                current_stripped = current.lstrip()

                # prochain def de classe => on sort sans consommer la ligne
                if current.startswith("    def ") and not current_stripped.startswith(HELPER_START):
                    break

                # ligne vide
                if current_stripped == "":
                    out.append("")
                    i += 1
                    continue

                # docstring / corps du helper
                out.append("        " + current_stripped)
                i += 1

            continue

        out.append(line)
        i += 1

    fixed = "\n".join(out) + ("\n" if text.endswith("\n") else "")

    # garde dure
    compile(fixed, str(path), "exec")

    path.write_text(fixed, encoding="utf-8")
    print(f"FIXED: {path}")


for target in TARGETS:
    fix_file(target)
PYEOF

python3 <<'PYEOF'
from pathlib import Path

path = Path(".gitignore")
text = path.read_text(encoding="utf-8") if path.exists() else ""

for pattern in ["*.bak", "*.bak2", "*.bak3", "*.bak4", "*.bak5", "*.bak6", "*.bak7", "*.bak8"]:
    if pattern not in text.splitlines():
        text = text.rstrip() + ("\n" if text and not text.endswith("\n") else "") + pattern + "\n"

path.write_text(text, encoding="utf-8")
print("UPDATED: .gitignore")
PYEOF

echo "===== DONE ====="
