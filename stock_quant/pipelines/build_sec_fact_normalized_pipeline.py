from __future__ import annotations

from stock_quant.app.dto.pipeline_result import PipelineResult
from stock_quant.infrastructure.repositories.duckdb_sec_repository import DuckDbSecRepository
from stock_quant.pipelines.base_pipeline import BasePipeline
from stock_quant.shared.exceptions import PipelineError


class BuildSecFactNormalizedPipeline(BasePipeline):
    """
    Transforme sec_xbrl_fact_raw -> sec_fact_normalized.

    Cette couche constitue la normalized layer canonique pour les facts SEC.
    """

    pipeline_name = "build_sec_fact_normalized"

    def __init__(self, repository: DuckDbSecRepository) -> None:
        self.repository = repository
        self._rows_written = 0
        self._metrics: dict[str, int] = {}

    def extract(self):
        return self.repository.load_sec_xbrl_fact_raw_rows()

    def validate(self, rows) -> None:
        if not isinstance(rows, list):
            raise PipelineError("sec_xbrl_fact_raw rows must be a list")
        if not rows:
            raise PipelineError("no sec_xbrl_fact_raw rows available")

    def transform(self, rows):
        normalized_rows = []

        for row in rows:
            normalized_rows.append(
                {
                    "company_id": row.get("company_id"),
                    "cik": row.get("cik"),
                    "concept": row.get("concept"),
                    "unit": row.get("unit"),
                    "value": row.get("value"),
                    "period_start_date": row.get("period_start_date"),
                    "period_end_date": row.get("period_end_date"),
                    "fiscal_year": row.get("fiscal_year"),
                    "fiscal_period": row.get("fiscal_period"),
                    "available_at": row.get("available_at"),
                }
            )

        self._metrics["raw_fact_rows"] = len(rows)
        return normalized_rows

    def load(self, rows) -> None:
        self._rows_written = self.repository.insert_sec_fact_normalized(rows)

    def finalize(self, result: PipelineResult) -> PipelineResult:
        result.rows_read = int(self._metrics.get("raw_fact_rows", 0))
        result.rows_written = self._rows_written
        result.metrics.update(self._metrics)
        return result
