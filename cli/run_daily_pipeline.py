#!/usr/bin/env python3
from __future__ import annotations

import argparse
from datetime import datetime

from stock_quant.infrastructure.db.unit_of_work import DuckDbUnitOfWork
from stock_quant.app.services.pipeline_run_service import PipelineRunService

from stock_quant.pipelines.research_universe_pipeline import BuildResearchUniversePipeline
from stock_quant.pipelines.short_data_pipeline import BuildShortDataPipeline
from stock_quant.pipelines.dataset_builder_pipeline import BuildDatasetBuilderPipeline


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Run full daily stock-quant pipeline.")
    p.add_argument("--db-path", default="~/stock-quant-oop/market.duckdb")
    return p.parse_args()


def run_pipeline(pipeline_cls, name: str, uow):
    run_service = PipelineRunService(uow)

    run_id = run_service.start_run(name)

    pipeline = pipeline_cls(uow=uow)

    try:
        result = pipeline.run()

        run_service.finish_run(
            run_id,
            "SUCCESS",
            rows_read=result.rows_read,
            rows_written=result.rows_written,
        )

        return result

    except Exception as e:
        run_service.finish_run(run_id, "FAILED")
        raise


def main():
    args = parse_args()

    db_path = args.db_path
    uow = DuckDbUnitOfWork(db_path)

    print("===== DAILY PIPELINE START =====")
    print(datetime.utcnow())
    print()

    print("STEP 1 — SHORT DATA")
    run_pipeline(BuildShortDataPipeline, "short_data_pipeline", uow)

    print("STEP 2 — RESEARCH UNIVERSE")
    run_pipeline(BuildResearchUniversePipeline, "research_universe_pipeline", uow)

    print("STEP 3 — DATASET BUILDER")
    run_pipeline(BuildDatasetBuilderPipeline, "dataset_builder_pipeline", uow)

    print()
    print("===== DAILY PIPELINE COMPLETE =====")


if __name__ == "__main__":
    main()
