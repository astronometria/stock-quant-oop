from pathlib import Path


def test_build_prices_pipeline_no_longer_uses_special_result_contract() -> None:
    text = Path("stock_quant/pipelines/build_prices_pipeline.py").read_text(encoding="utf-8")

    assert "BuildPricesPipelineResult" not in text
    assert "PricesPipelineResult" not in text
    assert "return run_pipeline(" in text
    assert "def _to_payload(" in text
    assert "\"written_price_history_rows\"" in text
    assert "\"price_latest_rows_after_refresh\"" in text


def test_prices_pipeline_wrapper_only_reexports_pipeline_classes() -> None:
    text = Path("stock_quant/pipelines/prices_pipeline.py").read_text(encoding="utf-8")

    assert "BuildPricesPipelineResult" not in text
    assert "PricesPipelineResult" not in text
    assert "BuildPricesPipeline" in text
    assert "PricesPipeline" in text
