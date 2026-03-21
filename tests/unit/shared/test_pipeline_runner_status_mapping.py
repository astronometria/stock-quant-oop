from stock_quant.shared.enums import PipelineStatus
from stock_quant.shared.pipeline_runner import run_pipeline


def test_runner_maps_noop_to_skipped() -> None:
    def _fake():
        return {
            "status": "noop",
            "rows_read": 0,
            "rows_written": 0,
            "rows_skipped": 0,
            "metrics": {"build_decision": "noop_no_raw_rows"},
            "error_message": None,
        }

    result = run_pipeline("demo_noop", _fake)
    assert result.status == PipelineStatus.SKIPPED
    assert result.metrics["build_decision"] == "noop_no_raw_rows"


def test_runner_maps_success_payload() -> None:
    def _fake():
        return {
            "status": "success",
            "rows_read": 10,
            "rows_written": 7,
            "rows_skipped": 3,
            "metrics": {"demo_metric": 1},
            "warnings": ["ok"],
            "error_message": None,
        }

    result = run_pipeline("demo_success", _fake)
    assert result.status == PipelineStatus.SUCCESS
    assert result.rows_read == 10
    assert result.rows_written == 7
    assert result.rows_skipped == 3
    assert result.warnings == ["ok"]
