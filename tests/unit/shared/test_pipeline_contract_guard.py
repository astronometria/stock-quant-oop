from pathlib import Path

from stock_quant.app.dto.pipeline_result import PipelineResult
from stock_quant.shared.enums import PipelineStatus
from stock_quant.shared.pipeline_runner import run_pipeline


def test_pipeline_contract_duplicates_removed() -> None:
    assert not Path("stock_quant/shared/pipeline_result.py").exists()
    assert not Path("stock_quant/shared/pipeline_status.py").exists()
    assert not Path("stock_quant/pipelines/base_pipeline.py").exists()


def test_pipeline_runner_uses_canonical_contract() -> None:
    def _fake_pipeline() -> dict:
        return {
            "rows_read": 10,
            "rows_written": 7,
            "rows_skipped": 3,
            "warnings": ["demo-warning"],
            "metrics": {"demo_metric": 1},
        }

    result = run_pipeline("demo_pipeline", _fake_pipeline)

    assert isinstance(result, PipelineResult)
    assert result.status == PipelineStatus.SUCCESS
    assert result.rows_read == 10
    assert result.rows_written == 7
    assert result.rows_skipped == 3
    assert result.warnings == ["demo-warning"]
    assert result.metrics["demo_metric"] == 1
    assert result.error_message is None
