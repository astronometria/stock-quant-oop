#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

from stock_quant.app.orchestrators.core_pipeline_orchestrator import (
    CorePipelineOrchestrator,
    CorePipelineStep,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the core stock-quant-oop pipeline in sequence."
    )
    parser.add_argument(
        "--project-root",
        default=str(Path(__file__).resolve().parents[2]),
        help="Project root directory.",
    )
    parser.add_argument(
        "--db-path",
        default=None,
        help="Optional DuckDB path passed to each CLI step.",
    )
    parser.add_argument(
        "--drop-existing",
        action="store_true",
        help="Drop and recreate database objects at init step.",
    )
    parser.add_argument(
        "--source-market",
        default="regular",
        choices=["regular", "otc", "both"],
        help="Source market selection passed to supported steps.",
    )
    parser.add_argument(
        "--skip-symbol-load",
        action="store_true",
        help="Skip loading symbol reference raw staging data.",
    )
    parser.add_argument(
        "--skip-universe",
        action="store_true",
        help="Skip market universe build step.",
    )
    parser.add_argument(
        "--skip-symbol-reference",
        action="store_true",
        help="Skip symbol reference build step.",
    )
    parser.add_argument(
        "--skip-price-load",
        action="store_true",
        help="Skip loading price raw staging data.",
    )
    parser.add_argument(
        "--skip-prices",
        action="store_true",
        help="Skip price build step.",
    )
    parser.add_argument(
        "--skip-finra-load",
        action="store_true",
        help="Skip loading FINRA raw staging data.",
    )
    parser.add_argument(
        "--skip-finra",
        action="store_true",
        help="Skip FINRA short interest build step.",
    )
    parser.add_argument(
        "--skip-news-load",
        action="store_true",
        help="Skip loading news raw staging data.",
    )
    parser.add_argument(
        "--skip-news-raw",
        action="store_true",
        help="Skip normalized news build step.",
    )
    parser.add_argument(
        "--skip-news-candidates",
        action="store_true",
        help="Skip news symbol candidates step.",
    )
    parser.add_argument(
        "--truncate-raw",
        action="store_true",
        help="Pass --truncate to raw loader steps when supported.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose output for child commands when supported.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    project_root = Path(args.project_root).expanduser().resolve()

    orchestrator = CorePipelineOrchestrator(
        project_root=project_root,
        db_path=args.db_path,
        verbose=args.verbose,
    )

    steps: list[CorePipelineStep] = []

    steps.append(
        CorePipelineStep(
            name="init_market_db",
            relative_script="cli/init_market_db.py",
            extra_args=["--drop-existing"] if args.drop_existing else [],
        )
    )

    if not args.skip_symbol_load and orchestrator.script_exists("cli/load_symbol_reference_source_raw.py"):
        symbol_load_args: list[str] = []
        if args.truncate_raw:
            symbol_load_args.append("--truncate")
        steps.append(
            CorePipelineStep(
                name="load_symbol_reference_source_raw",
                relative_script="cli/load_symbol_reference_source_raw.py",
                extra_args=symbol_load_args,
            )
        )

    if not args.skip_universe:
        steps.append(
            CorePipelineStep(
                name="build_market_universe",
                relative_script="cli/build_market_universe.py",
            )
        )

    if not args.skip_symbol_reference:
        steps.append(
            CorePipelineStep(
                name="build_symbol_reference",
                relative_script="cli/build_symbol_reference.py",
            )
        )

    if not args.skip_price_load:
        if orchestrator.script_exists("cli/load_price_source_daily_raw_all_from_stooq_zip.py"):
            raw_all_args: list[str] = []
            if args.truncate_raw:
                raw_all_args.append("--truncate")
            steps.append(
                CorePipelineStep(
                    name="load_price_source_daily_raw_all_from_stooq_zip",
                    relative_script="cli/load_price_source_daily_raw_all_from_stooq_zip.py",
                    extra_args=raw_all_args,
                )
            )

        if orchestrator.script_exists("cli/fetch_price_source_daily_raw_yahoo.py"):
            yahoo_args: list[str] = []
            if args.truncate_raw:
                yahoo_args.append("--truncate")
            steps.append(
                CorePipelineStep(
                    name="fetch_price_source_daily_raw_yahoo",
                    relative_script="cli/fetch_price_source_daily_raw_yahoo.py",
                    extra_args=yahoo_args,
                )
            )

        if orchestrator.script_exists("cli/filter_price_source_daily_raw_from_all.py"):
            filter_args: list[str] = []
            if args.truncate_raw:
                filter_args.append("--truncate")
            steps.append(
                CorePipelineStep(
                    name="filter_price_source_daily_raw_from_all",
                    relative_script="cli/filter_price_source_daily_raw_from_all.py",
                    extra_args=filter_args,
                )
            )

    if not args.skip_prices:
        steps.append(
            CorePipelineStep(
                name="build_prices",
                relative_script="cli/build_prices.py",
            )
        )

    if not args.skip_finra_load and orchestrator.script_exists("cli/load_finra_short_interest_source_raw.py"):
        finra_load_args: list[str] = []
        if args.truncate_raw:
            finra_load_args.append("--truncate")
        steps.append(
            CorePipelineStep(
                name="load_finra_short_interest_source_raw",
                relative_script="cli/load_finra_short_interest_source_raw.py",
                extra_args=finra_load_args,
            )
        )

    if not args.skip_finra:
        steps.append(
            CorePipelineStep(
                name="build_finra_short_interest",
                relative_script="cli/build_finra_short_interest.py",
                extra_args=["--source-market", args.source_market],
            )
        )

    if not args.skip_news_load and orchestrator.script_exists("cli/load_news_source_raw.py"):
        news_load_args: list[str] = []
        if args.truncate_raw:
            news_load_args.append("--truncate")
        steps.append(
            CorePipelineStep(
                name="load_news_source_raw",
                relative_script="cli/load_news_source_raw.py",
                extra_args=news_load_args,
            )
        )

    if not args.skip_news_raw:
        steps.append(
            CorePipelineStep(
                name="build_news_raw",
                relative_script="cli/build_news_raw.py",
            )
        )

    if not args.skip_news_candidates:
        steps.append(
            CorePipelineStep(
                name="build_news_symbol_candidates",
                relative_script="cli/build_news_symbol_candidates.py",
            )
        )

    return orchestrator.run_steps(steps)


if __name__ == "__main__":
    raise SystemExit(main())
