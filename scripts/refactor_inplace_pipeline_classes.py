from __future__ import annotations

from pathlib import Path
import re

TARGETS = [
    Path("stock_quant/pipelines/build_dataset_pipeline.py"),
    Path("stock_quant/pipelines/build_feature_engine_pipeline.py"),
    Path("stock_quant/pipelines/build_label_engine_pipeline.py"),
    Path("stock_quant/pipelines/build_market_universe_pipeline.py"),
    Path("stock_quant/pipelines/build_news_pipeline.py"),
    Path("stock_quant/pipelines/build_sec_filings_pipeline.py"),
    Path("stock_quant/pipelines/build_symbol_reference_pipeline.py"),
    Path("stock_quant/pipelines/master_data_pipeline.py"),
    Path("stock_quant/pipelines/news_raw_pipeline.py"),
    Path("stock_quant/pipelines/news_symbol_candidates_pipeline.py"),
    Path("stock_quant/pipelines/research_universe_pipeline.py"),
    Path("stock_quant/pipelines/build_daily_short_volume_pipeline.py"),
    Path("stock_quant/pipelines/build_short_interest_pipeline.py"),
    Path("stock_quant/pipelines/build_short_features_pipeline.py"),
]

RUN_METHOD = '''

    def _execute_pipeline_steps(self) -> dict:
        """
        Exécute le cycle pipeline historique sans BasePipeline.

        Important:
        - la logique PIT / anti-survivorship reste dans validate/load/finalize
        - cette méthode ne doit pas déplacer la logique métier hors du module
        """
        data = self.extract()
        data = self.transform(data)
        self.validate(data)
        self.load(data)

        return {
            "rows_read": getattr(self, "_rows_read", 0),
            "rows_written": getattr(self, "_rows_written", 0),
            "rows_skipped": getattr(self, "_rows_skipped", 0),
            "warnings": [],
            "metrics": getattr(self, "_metrics", {}),
        }

    def run(self):
        """
        Point d'entrée canonique local.

        Le runner partagé construit PipelineResult.
        La logique métier locale peut ensuite enrichir le résultat
        via _finalize_result(...) si le module le définit.
        """
        result = run_pipeline(self.pipeline_name, self._execute_pipeline_steps)
        finalizer = getattr(self, "_finalize_result", None)
        if callable(finalizer):
            return finalizer(result)
        return result
'''

for path in TARGETS:
    if not path.exists():
        print(f"SKIP MISSING: {path}")
        continue

    text = path.read_text(encoding="utf-8")
    original = text

    # backup local
    bak3 = path.with_suffix(path.suffix + ".bak3")
    bak3.write_text(text, encoding="utf-8")

    # 1) retirer l'import BasePipeline résiduel commenté/cassé
    text = text.replace(
        "# REMOVED: BasePipeline supprimé pendant la consolidation brutale\n",
        "",
    )

    # 2) injecter run_pipeline si absent
    if "from stock_quant.shared.pipeline_runner import run_pipeline" not in text:
        lines = text.splitlines()
        insert_idx = 0
        for i, line in enumerate(lines):
            if line.startswith("from ") or line.startswith("import "):
                insert_idx = i + 1
        lines.insert(insert_idx, "from stock_quant.shared.pipeline_runner import run_pipeline")
        text = "\n".join(lines) + ("\n" if text.endswith("\n") else "")

    # 3) remplacer héritage BasePipeline
    text = re.sub(
        r"class (\w+)\(BasePipeline\):",
        r"class \1:",
        text,
    )

    # 4) renommer finalize -> _finalize_result
    text = re.sub(
        r"def finalize\(self, result: PipelineResult\)",
        "def _finalize_result(self, result: PipelineResult)",
        text,
    )

    # 5) ajouter run() si absent
    if re.search(r"\n\s+def run\(self[,\)]", text) is None:
        class_match = re.search(r"class\s+\w+:\n", text)
        if class_match:
            insert_pos = len(text)
            text = text.rstrip() + RUN_METHOD + "\n"

    # 6) écrire seulement si changement
    if text != original:
        path.write_text(text, encoding="utf-8")
        print(f"PATCHED: {path}")
    else:
        print(f"NO CHANGE: {path}")
