from __future__ import annotations

from dataclasses import dataclass

from stock_quant.app.services.fundamentals_service import FundamentalsService
from stock_quant.shared.exceptions import PipelineError


@dataclass(slots=True)
class BuildFundamentalsResult:

    sec_fact_rows: int
    snapshot_rows: int
    feature_rows: int


class BuildFundamentalsPipeline:

    pipeline_name = "build_fundamentals"

    def __init__(self, service: FundamentalsService):

        self.service = service


    def run(self):

        facts = self.service.load_sec_facts()

        if not facts:
            raise PipelineError("no sec_fact_normalized rows available")

        snapshot_rows = self.service.build_snapshots()

        feature_rows = self.service.build_features()

        return BuildFundamentalsResult(
            sec_fact_rows=len(facts),
            snapshot_rows=snapshot_rows,
            feature_rows=feature_rows,
        )
