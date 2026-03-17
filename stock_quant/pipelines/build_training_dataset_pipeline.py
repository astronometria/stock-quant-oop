from __future__ import annotations

from dataclasses import dataclass

from stock_quant.shared.exceptions import PipelineError


@dataclass(slots=True)
class BuildTrainingDatasetResult:

    rows_written: int


class BuildTrainingDatasetPipeline:

    pipeline_name = "build_training_dataset"

    def __init__(self, repository):

        self.repository = repository


    def run(self):

        rows = self.repository.build_dataset()

        if rows == 0:
            raise PipelineError("training dataset empty")

        return BuildTrainingDatasetResult(rows_written=rows)
