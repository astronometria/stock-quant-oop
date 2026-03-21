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
