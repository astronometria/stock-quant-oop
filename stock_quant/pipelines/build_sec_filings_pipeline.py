from __future__ import annotations

# =============================================================================
# build_sec_filings_pipeline.py
# -----------------------------------------------------------------------------
# Pipeline de construction de la table sec_filing à partir de sec_filing_raw_index.
#
# Correctifs importants:
# - alignement avec le repository réel
# - appel à upsert_sec_filings(...) au pluriel
# - instanciation interne du service pour éviter les divergences d'interface
# - métriques plus robustes
# =============================================================================

from stock_quant.app.dto.pipeline_result import PipelineResult
from stock_quant.app.services.sec_filings_service import SecFilingsService
from stock_quant.infrastructure.repositories.duckdb_sec_repository import DuckDbSecRepository
from stock_quant.pipelines.base_pipeline import BasePipeline
from stock_quant.shared.exceptions import PipelineError


class BuildSecFilingsPipeline(BasePipeline):
    pipeline_name = "build_sec_filings"

    def __init__(self, repository: DuckDbSecRepository) -> None:
        self.repository = repository
        self.service = SecFilingsService()
        self._metrics: dict[str, int] = {}
        self._rows_written = 0

    def extract(self):
        return {
            "raw_index_rows": self.repository.load_sec_filing_raw_index_rows(),
            "cik_company_map": self.repository.load_cik_company_map(),
        }

    def transform(self, data):
        filings, metrics = self.service.build_sec_filings(
            raw_index_rows=data["raw_index_rows"],
            cik_company_map=data["cik_company_map"],
        )
        return filings, metrics

    def validate(self, data) -> None:
        filings, metrics = data

        if not isinstance(filings, list):
            raise PipelineError("filings must be a list")

        if not isinstance(metrics, dict):
            raise PipelineError("metrics must be a dict")

        if metrics.get("raw_index_rows", 0) == 0:
            raise PipelineError("no sec_filing_raw_index rows available")

    def load(self, data) -> None:
        filings, metrics = data

        # ---------------------------------------------------------------------
        # Alignement avec le repository actuel:
        # - la méthode exposée est upsert_sec_filings(...)
        # - au pluriel
        # ---------------------------------------------------------------------
        self._rows_written = self.repository.upsert_sec_filings(filings)
        self._metrics = dict(metrics)
        self._metrics["written_sec_filing"] = self._rows_written

    def finalize(self, result: PipelineResult) -> PipelineResult:
        result.rows_read = int(self._metrics.get("raw_index_rows", 0))
        result.rows_written = self._rows_written
        result.metrics.update(self._metrics)
        return result
