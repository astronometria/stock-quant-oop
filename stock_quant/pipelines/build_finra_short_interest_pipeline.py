from __future__ import annotations

from dataclasses import dataclass

from stock_quant.app.services.short_interest_service import ShortInterestService
from stock_quant.shared.exceptions import PipelineError


@dataclass(slots=True)
class BuildFinraShortInterestResult:

    raw_rows: int
    written_history_rows: int
    latest_rows: int


class BuildFinraShortInterestPipeline:

    pipeline_name = "build_finra_short_interest"

    def __init__(self, service: ShortInterestService):

        self.service = service

    def run(self):

        raw_rows = self.service.load_raw()

        if raw_rows == 0:
            raise PipelineError("no FINRA raw rows available")

        written = self.service.build_history()

        latest = self.service.refresh_latest()

        return BuildFinraShortInterestResult(
            raw_rows=raw_rows,
            written_history_rows=written,
            latest_rows=latest,
        )
