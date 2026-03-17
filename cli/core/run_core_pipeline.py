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
        "--symbol-source",
        action="append",
        dest="symbol_sources",
        default=[],
        help="Optional symbol reference raw source file. Repeat this flag for multiple inputs.",
    )
    parser.add_argument(
        "--finra-source",
        action="append",
        dest="finra_sources",
        default=[],
        help="Optional FINRA raw source path/file/dir/glob. Repeat this flag for multiple inputs.",
    )
    parser.add_argument(
        "--news-source",
        action="append",
        dest="news_sources",
        default=[],
        help="Optional news raw source file. Repeat this flag for multiple inputs.",
    )
    parser.add_argument("--skip-symbol-load", action="store_true")
    parser.add_argument("--skip-universe", action="store_true")
    parser.add_argument("--skip-symbol-reference", action="store_true")
    parser.add_argument("--skip-price-load", action="store_true")
    parser.add_argument("--skip-prices", action="store_true")
    parser.add_argument("--skip-finra-load", action="store_true")
    parser.add_argument("--skip-finra", action="store_true")
    parser.add_argument("--skip-news-load", action="store_true")
    parser.add_argument("--skip-news-raw", action="store_true")
    parser.add_argument("--skip-news-candidates", action="store_true")
    parser.add_argument("--truncate-raw", action="store_true")
    parser.add_argument("--verbose", action="store_true")
    return parser.parse_args()


def _append_if(args: list[str], flag: str, condition: bool) -> None:
    if condition:
        args.append(flag)


def _extend_repeatable(args: list[str], flag: str, values: list[str]) -> None:
    for value in values:
        args.extend([flag, value])


def _auto_disable_missing_source_chains(args: argparse.Namespace) -> None:
    """
    Make the core pipeline safe for lightweight test runs and empty-db smoke runs.

    Dependency rules:
    - no symbol source -> skip symbol load + universe + symbol reference
    - no FINRA source  -> skip FINRA load + FINRA build
    - no news source   -> skip news load + news raw + news candidates
    """
    if not args.symbol_sources:
        args.skip_symbol_load = True
        args.skip_universe = True
        args.skip_symbol_reference = True

    if not args.finra_sources:
        args.skip_finra_load = True
        args.skip_finra = True

    if not args.news_sources:
        args.skip_news_load = True
        args.skip_news_raw = True
        args.skip_news_candidates = True


def main() -> int:
    args = parse_args()
    _auto_disable_missing_source_chains(args)

    project_root = Path(args.project_root).expanduser().resolve()
    orchestrator = CorePipelineOrchestrator(
        project_root=project_root,
        db_path=args.db_path,
        verbose=args.verbose,
    )

    steps: list[CorePipelineStep] = [
        CorePipelineStep(
            name="init_market_db",
            relative_script="cli/core/init_market_db.py",
            extra_args=["--drop-existing"] if args.drop_existing else [],
        )
    ]

    if not args.skip_symbol_load:
        symbol_args: list[str] = []
        _extend_repeatable(symbol_args, "--source", args.symbol_sources)
        _append_if(symbol_args, "--truncate", args.truncate_raw)
        steps.append(
            CorePipelineStep(
                name="load_symbol_reference_source_raw",
                relative_script="cli/raw/load_symbol_reference_source_raw.py",
                extra_args=symbol_args,
            )
        )

    if not args.skip_universe:
        steps.append(
            CorePipelineStep(
                name="build_market_universe",
                relative_script="cli/core/build_market_universe.py",
            )
        )

    if not args.skip_symbol_reference:
        steps.append(
            CorePipelineStep(
                name="build_symbol_reference",
                relative_script="cli/core/build_symbol_reference.py",
            )
        )

    if not args.skip_price_load:
        price_args: list[str] = []
        _append_if(price_args, "--truncate", args.truncate_raw)
        steps.append(
            CorePipelineStep(
                name="load_price_source_daily_raw",
                relative_script="cli/raw/load_price_source_daily_raw.py",
                extra_args=price_args,
            )
        )

    if not args.skip_prices:
        steps.append(
            CorePipelineStep(
                name="build_prices",
                relative_script="cli/core/build_prices.py",
            )
        )

    if not args.skip_finra_load:
        finra_args: list[str] = []
        _extend_repeatable(finra_args, "--source", args.finra_sources)
        _append_if(finra_args, "--truncate", args.truncate_raw)
        steps.append(
            CorePipelineStep(
                name="load_finra_short_interest_source_raw",
                relative_script="cli/raw/load_finra_short_interest_source_raw.py",
                extra_args=finra_args,
            )
        )

    if not args.skip_finra:
        steps.append(
            CorePipelineStep(
                name="build_finra_short_interest",
                relative_script="cli/core/build_finra_short_interest.py",
                extra_args=["--source-market", args.source_market],
            )
        )

    if not args.skip_news_load:
        news_args: list[str] = []
        _extend_repeatable(news_args, "--source", args.news_sources)
        _append_if(news_args, "--truncate", args.truncate_raw)
        steps.append(
            CorePipelineStep(
                name="load_news_source_raw",
                relative_script="cli/raw/load_news_source_raw.py",
                extra_args=news_args,
            )
        )

    if not args.skip_news_raw:
        steps.append(
            CorePipelineStep(
                name="build_news_raw",
                relative_script="cli/core/build_news_raw.py",
            )
        )

    if not args.skip_news_candidates:
        steps.append(
            CorePipelineStep(
                name="build_news_symbol_candidates",
                relative_script="cli/core/build_news_symbol_candidates.py",
            )
        )

    return orchestrator.run_steps(steps)


if __name__ == "__main__":
    raise SystemExit(main())
