from __future__ import annotations

from abc import ABC
from datetime import datetime

from stock_quant.app.dto.pipeline_result import PipelineResult
from stock_quant.shared.enums import PipelineStatus


class BasePipeline(ABC):
    pipeline_name = "base_pipeline"

    def prepare(self) -> None:
        return None

    def extract(self):
        return None

    def transform(self, data):
        return data

    def validate(self, data) -> None:
        return None

    def load(self, data) -> None:
        return None

    def finalize(self, result: PipelineResult) -> PipelineResult:
        return result

    def run(self) -> PipelineResult:
        started_at = datetime.utcnow()
        try:
            self.prepare()
            data = self.extract()
            data = self.transform(data)
            self.validate(data)
            self.load(data)
            result = PipelineResult(
                pipeline_name=self.pipeline_name,
                status=PipelineStatus.SUCCESS,
                started_at=started_at,
                finished_at=datetime.utcnow(),
            )
            return self.finalize(result)
        except Exception as exc:
            result = PipelineResult(
                pipeline_name=self.pipeline_name,
                status=PipelineStatus.FAILED,
                started_at=started_at,
                finished_at=datetime.utcnow(),
                error_message=str(exc),
            )
            return self.finalize(result)
