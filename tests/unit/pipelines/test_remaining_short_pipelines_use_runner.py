from pathlib import Path

TARGETS = [
    Path("stock_quant/pipelines/build_short_features_pipeline.py"),
    Path("stock_quant/pipelines/build_short_interest_pipeline.py"),
    Path("stock_quant/pipelines/build_daily_short_volume_pipeline.py"),
]


def test_remaining_short_pipelines_no_longer_construct_pipeline_result_locally() -> None:
    for path in TARGETS:
        text = path.read_text(encoding="utf-8")
        assert "return PipelineResult(" not in text, f"local PipelineResult remains in {path}"
        assert "def _build_pipeline_result(" not in text, f"legacy helper remains in {path}"
        assert "def _build_result(" not in text, f"legacy helper remains in {path}"
        assert "return run_pipeline(self.pipeline_name, self._run_impl)" in text, f"runner not wired in {path}"
