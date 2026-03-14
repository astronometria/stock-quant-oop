#!/usr/bin/env python3
from __future__ import annotations

import argparse

from stock_quant.app.services.llm_run_tracking_service import LlmRunTrackingService
from stock_quant.infrastructure.config.settings_loader import build_app_config
from stock_quant.infrastructure.db.duckdb_session_factory import DuckDbSessionFactory
from stock_quant.infrastructure.db.research_schema import ResearchSchemaManager
from stock_quant.infrastructure.db.unit_of_work import DuckDbUnitOfWork
from stock_quant.infrastructure.repositories.duckdb_llm_run_repository import DuckDbLlmRunRepository


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Register an LLM enrichment run in llm_runs.")
    parser.add_argument("--db-path", default=None, help="Path to DuckDB database file.")
    parser.add_argument("--run-name", default="news_llm_enrichment_v1", help="Logical run name.")
    parser.add_argument("--model-name", default="placeholder", help="Model name.")
    parser.add_argument("--prompt-version", default="v1", help="Prompt version.")
    parser.add_argument("--input-table", default="news_articles_normalized", help="Input table name.")
    parser.add_argument("--output-table", default="news_llm_enrichment", help="Output table name.")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose output.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    config = build_app_config(db_path=args.db_path)
    config.ensure_directories()

    if args.verbose:
        print(f"[register_llm_run] project_root={config.project_root}")
        print(f"[register_llm_run] db_path={config.db_path}")
        print(f"[register_llm_run] run_name={args.run_name}")

    session_factory = DuckDbSessionFactory(config.db_path)
    with DuckDbUnitOfWork(session_factory) as uow:
        ResearchSchemaManager(uow).initialize()

        repository = DuckDbLlmRunRepository(uow)
        service = LlmRunTrackingService()

        row_count = repository.count_news_llm_enrichment_rows()
        payload = service.build_llm_run_payload(
            run_name=args.run_name,
            model_name=args.model_name,
            prompt_version=args.prompt_version,
            input_table=args.input_table,
            output_table=args.output_table,
            row_count=row_count,
        )
        written = repository.insert_llm_run(payload)

    print(f"llm_runs inserted: {written}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
