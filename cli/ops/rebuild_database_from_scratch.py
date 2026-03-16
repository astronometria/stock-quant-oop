#!/usr/bin/env python3
from __future__ import annotations

import argparse
import shlex
import subprocess
import sys
from pathlib import Path

from stock_quant.infrastructure.config.settings_loader import build_app_config


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Rebuild the DuckDB database from scratch using real raw symbol sources, "
            "core SEC normalization, fundamentals construction, mandatory Stooq price backfill, "
            "optional Yahoo daily refresh, and future extension points such as FINRA."
        )
    )
    parser.add_argument(
        "--db-path",
        default=None,
        help="Path to DuckDB database file.",
    )

    # ------------------------------------------------------------------
    # Core rebuild switches
    # ------------------------------------------------------------------
    parser.add_argument("--skip-init", action="store_true", help="Skip schema initialization step.")
    parser.add_argument("--skip-sec-fetch", action="store_true", help="Skip SEC company tickers raw download.")
    parser.add_argument("--skip-nasdaq-fetch", action="store_true", help="Skip NASDAQ symbol directory raw download.")
    parser.add_argument("--skip-raw-load", action="store_true", help="Skip loading symbol_reference_source_raw.")
    parser.add_argument("--skip-market-universe", action="store_true", help="Skip build_market_universe.")
    parser.add_argument("--skip-symbol-reference", action="store_true", help="Skip build_symbol_reference.")
    parser.add_argument("--skip-sec-filings", action="store_true", help="Skip build_sec_filings.")
    parser.add_argument("--skip-fundamentals", action="store_true", help="Skip build_fundamentals.")

    # ------------------------------------------------------------------
    # Price-specific switches
    # ------------------------------------------------------------------
    parser.add_argument(
        "--skip-price-backfill",
        action="store_true",
        help="Skip mandatory historical Stooq-style price backfill step.",
    )
    parser.add_argument(
        "--skip-price-daily",
        action="store_true",
        help="Skip Yahoo daily price refresh after historical backfill.",
    )
    parser.add_argument(
        "--stooq-source",
        action="append",
        dest="stooq_sources",
        default=[],
        help="Historical Stooq source path. Repeat for multiple inputs.",
    )
    parser.add_argument(
        "--price-symbol",
        action="append",
        dest="price_symbols",
        default=[],
        help="Optional symbol filter for prices. Repeat for multiple symbols.",
    )
    parser.add_argument(
        "--price-start-date",
        default=None,
        help="Optional start date for prices (YYYY-MM-DD). Applies to daily and can be used for explicit backfill windows.",
    )
    parser.add_argument(
        "--price-end-date",
        default=None,
        help="Optional end date for prices (YYYY-MM-DD). Applies to daily and can be used for explicit backfill windows.",
    )
    parser.add_argument(
        "--price-as-of",
        default=None,
        help="Optional single date for daily price refresh (YYYY-MM-DD).",
    )

    # ------------------------------------------------------------------
    # Universe behavior
    # ------------------------------------------------------------------
    parser.add_argument(
        "--allow-adr",
        action="store_true",
        help="Allow ADR in build_market_universe when supported by the CLI.",
    )

    parser.add_argument("--verbose", action="store_true", help="Enable verbose child command output.")
    return parser.parse_args()


def _run_step(step_name: str, command: list[str]) -> None:
    print(f"===== STEP: {step_name} =====", flush=True)
    print("COMMAND:", " ".join(shlex.quote(part) for part in command), flush=True)

    completed = subprocess.run(command, text=True)

    if completed.returncode != 0:
        raise SystemExit(
            f"step failed: {step_name} (exit_code={completed.returncode})"
        )


def _latest_file(directory: Path, pattern: str) -> Path | None:
    matches = sorted(directory.glob(pattern))
    if not matches:
        return None
    return matches[-1]


def _extend_with_optional_price_filters(command: list[str], args: argparse.Namespace) -> list[str]:
    for source in args.stooq_sources:
        command.extend(["--historical-source", str(source)])

    for symbol in args.price_symbols:
        command.extend(["--symbol", str(symbol)])

    if args.price_start_date:
        command.extend(["--start-date", str(args.price_start_date)])
    if args.price_end_date:
        command.extend(["--end-date", str(args.price_end_date)])
    if args.price_as_of:
        command.extend(["--as-of", str(args.price_as_of)])

    return command


def main() -> int:
    args = parse_args()

    config = build_app_config(db_path=args.db_path)
    config.ensure_directories()

    project_root = Path(config.project_root).expanduser().resolve()
    db_path = Path(config.db_path).expanduser().resolve()
    python_bin = sys.executable

    sec_dir = project_root / "data" / "symbol_sources" / "sec"
    nasdaq_dir = project_root / "data" / "symbol_sources" / "nasdaq"

    print("===== REBUILD DATABASE FROM SCRATCH =====", flush=True)
    print(f"project_root={project_root}", flush=True)
    print(f"db_path={db_path}", flush=True)

    # ------------------------------------------------------------------
    # 1) Init DB schema
    # ------------------------------------------------------------------
    if not args.skip_init:
        command = [
            python_bin,
            str(project_root / "cli" / "core" / "init_market_db.py"),
            "--db-path",
            str(db_path),
        ]
        _run_step("INIT MARKET DB", command)

    # ------------------------------------------------------------------
    # 2) Fetch raw symbol sources to local disk
    # ------------------------------------------------------------------
    if not args.skip_sec_fetch:
        command = [
            python_bin,
            str(project_root / "cli" / "raw" / "fetch_sec_company_tickers_raw.py"),
            "--verbose",
        ]
        _run_step("FETCH SEC COMPANY TICKERS RAW", command)

    if not args.skip_nasdaq_fetch:
        command = [
            python_bin,
            str(project_root / "cli" / "raw" / "fetch_nasdaq_symbol_directory_raw.py"),
            "--verbose",
        ]
        _run_step("FETCH NASDAQ SYMBOL DIRECTORY RAW", command)

    # ------------------------------------------------------------------
    # 3) Resolve latest local raw symbol files
    # ------------------------------------------------------------------
    sec_file = _latest_file(sec_dir, "sec_company_tickers_*.csv")
    nasdaqlisted_file = _latest_file(nasdaq_dir, "nasdaqlisted_*.csv")
    otherlisted_file = _latest_file(nasdaq_dir, "otherlisted_*.csv")

    print("===== RESOLVED RAW FILES =====", flush=True)
    print(f"sec_file={sec_file}", flush=True)
    print(f"nasdaqlisted_file={nasdaqlisted_file}", flush=True)
    print(f"otherlisted_file={otherlisted_file}", flush=True)

    # ------------------------------------------------------------------
    # 4) Load symbol_reference_source_raw from real local raw files
    # ------------------------------------------------------------------
    if not args.skip_raw_load:
        missing_files: list[str] = []
        if sec_file is None:
            missing_files.append("SEC company tickers file")
        if nasdaqlisted_file is None:
            missing_files.append("NASDAQ listed file")
        if otherlisted_file is None:
            missing_files.append("NASDAQ other listed file")

        if missing_files:
            raise SystemExit(
                "cannot load symbol_reference_source_raw; missing files: "
                + ", ".join(missing_files)
            )

        command = [
            python_bin,
            str(project_root / "cli" / "raw" / "load_symbol_reference_source_raw.py"),
            "--db-path",
            str(db_path),
            "--truncate",
            "--source",
            str(nasdaqlisted_file),
            "--source",
            str(otherlisted_file),
            "--source",
            str(sec_file),
            "--verbose",
        ]
        _run_step("LOAD SYMBOL_REFERENCE_SOURCE_RAW", command)

    # ------------------------------------------------------------------
    # 5) Build market_universe
    # ------------------------------------------------------------------
    if not args.skip_market_universe:
        command = [
            python_bin,
            str(project_root / "cli" / "core" / "build_market_universe.py"),
            "--db-path",
            str(db_path),
            "--verbose",
        ]
        if not args.allow_adr:
            # Current CLI remains conservative by default.
            pass
        _run_step("BUILD MARKET UNIVERSE", command)

    # ------------------------------------------------------------------
    # 6) Build symbol_reference
    # ------------------------------------------------------------------
    if not args.skip_symbol_reference:
        command = [
            python_bin,
            str(project_root / "cli" / "core" / "build_symbol_reference.py"),
            "--db-path",
            str(db_path),
            "--verbose",
        ]
        _run_step("BUILD SYMBOL REFERENCE", command)

    # ------------------------------------------------------------------
    # 7) Build SEC filings
    # ------------------------------------------------------------------
    if not args.skip_sec_filings:
        command = [
            python_bin,
            str(project_root / "cli" / "core" / "build_sec_filings.py"),
            "--db-path",
            str(db_path),
            "--verbose",
        ]
        _run_step("BUILD SEC FILINGS", command)

    # ------------------------------------------------------------------
    # 8) Build fundamentals
    # ------------------------------------------------------------------
    if not args.skip_fundamentals:
        command = [
            python_bin,
            str(project_root / "cli" / "core" / "build_fundamentals.py"),
            "--db-path",
            str(db_path),
            "--verbose",
        ]
        _run_step("BUILD FUNDAMENTALS", command)

    # ------------------------------------------------------------------
    # 9) Historical price backfill is part of the rebuild.
    #
    # This is essential for a real from-scratch rebuild because daily Yahoo
    # refresh alone is not sufficient to reconstruct long history.
    # ------------------------------------------------------------------
    if not args.skip_price_backfill:
        if not args.stooq_sources:
            raise SystemExit(
                "historical Stooq price sources are required for rebuild; "
                "pass at least one --stooq-source or explicitly use --skip-price-backfill"
            )

        command = [
            python_bin,
            str(project_root / "cli" / "core" / "build_prices.py"),
            "--db-path",
            str(db_path),
            "--mode",
            "backfill",
            "--verbose",
        ]
        command = _extend_with_optional_price_filters(command, args)
        _run_step("BUILD PRICES BACKFILL", command)

    # ------------------------------------------------------------------
    # 10) Daily price refresh after historical backfill
    # ------------------------------------------------------------------
    if not args.skip_price_daily:
        command = [
            python_bin,
            str(project_root / "cli" / "core" / "build_prices.py"),
            "--db-path",
            str(db_path),
            "--mode",
            "daily",
            "--verbose",
        ]

        for symbol in args.price_symbols:
            command.extend(["--symbol", str(symbol)])

        if args.price_start_date:
            command.extend(["--start-date", str(args.price_start_date)])
        if args.price_end_date:
            command.extend(["--end-date", str(args.price_end_date)])
        if args.price_as_of:
            command.extend(["--as-of", str(args.price_as_of)])

        _run_step("BUILD PRICES DAILY", command)

    print("===== REBUILD DATABASE COMPLETE =====", flush=True)
    print(f"db_path={db_path}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
