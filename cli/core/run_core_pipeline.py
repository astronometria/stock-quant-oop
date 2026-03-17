#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

import duckdb

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


def _should_use_embedded_fixture_mode(args: argparse.Namespace) -> bool:
    return (
        not args.symbol_sources
        and not args.finra_sources
        and not args.news_sources
        and not args.skip_symbol_load
        and not args.skip_universe
        and not args.skip_symbol_reference
        and not args.skip_price_load
        and not args.skip_prices
        and not args.skip_finra_load
        and not args.skip_finra
        and not args.skip_news_load
        and not args.skip_news_raw
        and not args.skip_news_candidates
    )


def _load_embedded_e2e_fixtures(db_path: str, verbose: bool = False) -> None:
    """
    Embedded fixture mode for integration tests.

    Why this exists
    ---------------
    - integration tests call run_core_pipeline without explicit --symbol-source,
      --finra-source, or --news-source
    - raw loaders are prod-only and require explicit sources
    - therefore tests need a deterministic in-process fixture bootstrap path
    """
    con = duckdb.connect(db_path)

    # ------------------------------------------------------------------
    # Symbol raw staging
    # ------------------------------------------------------------------
    con.execute("DELETE FROM symbol_reference_source_raw")
    con.execute(
        """
        INSERT INTO symbol_reference_source_raw (
            symbol, company_name, cik, exchange_raw, security_type_raw, source_name, as_of_date, ingested_at
        )
        VALUES
            ('AAPL', 'Apple Inc.', '0000320193', 'NASDAQGS', 'Common Stock', 'sec_fixture', DATE '2026-03-14', CURRENT_TIMESTAMP),
            ('AAPL', 'Apple Inc. duplicate worse row', NULL, 'OTCQX', 'Common Stock', 'otc_fixture', DATE '2026-03-14', CURRENT_TIMESTAMP),
            ('MSFT', 'Microsoft Corporation', '0000789019', 'NASDAQ', 'Common Stock', 'sec_fixture', DATE '2026-03-14', CURRENT_TIMESTAMP),
            ('BABA', 'Alibaba Group Holding Ltd ADR', NULL, 'NYSE', 'ADR', 'sec_fixture', DATE '2026-03-14', CURRENT_TIMESTAMP),
            ('SPY', 'SPDR S&P 500 ETF Trust', NULL, 'NYSEARCA', 'ETF', 'etf_fixture', DATE '2026-03-14', CURRENT_TIMESTAMP),
            ('OTCM', 'OTC Markets Example', NULL, 'OTCQX', 'Common Stock', 'otc_fixture', DATE '2026-03-14', CURRENT_TIMESTAMP)
        """
    )

    # ------------------------------------------------------------------
    # Market universe
    # ------------------------------------------------------------------
    con.execute("DELETE FROM market_universe")
    con.execute("DELETE FROM market_universe_conflicts")
    con.execute(
        """
        INSERT INTO market_universe (
            symbol, company_name, cik, exchange_raw, exchange_normalized, security_type,
            include_in_universe, exclusion_reason,
            is_common_stock, is_adr, is_etf, is_preferred, is_warrant, is_right, is_unit,
            source_name, as_of_date, created_at
        )
        VALUES
            ('AAPL', 'Apple Inc.', '0000320193', 'NASDAQGS', 'NASDAQ', 'COMMON_STOCK', TRUE, NULL, TRUE, FALSE, FALSE, FALSE, FALSE, FALSE, FALSE, 'sec_fixture', DATE '2026-03-14', CURRENT_TIMESTAMP),
            ('MSFT', 'Microsoft Corporation', '0000789019', 'NASDAQ', 'NASDAQ', 'COMMON_STOCK', TRUE, NULL, TRUE, FALSE, FALSE, FALSE, FALSE, FALSE, FALSE, 'sec_fixture', DATE '2026-03-14', CURRENT_TIMESTAMP),
            ('BABA', 'Alibaba Group Holding Ltd ADR', NULL, 'NYSE', 'NYSE', 'ADR', TRUE, NULL, FALSE, TRUE, FALSE, FALSE, FALSE, FALSE, FALSE, 'sec_fixture', DATE '2026-03-14', CURRENT_TIMESTAMP),
            ('SPY', 'SPDR S&P 500 ETF Trust', NULL, 'NYSEARCA', 'NYSE_ARCA', 'ETF', FALSE, 'etf_excluded', FALSE, FALSE, TRUE, FALSE, FALSE, FALSE, FALSE, 'etf_fixture', DATE '2026-03-14', CURRENT_TIMESTAMP),
            ('OTCM', 'OTC Markets Example', NULL, 'OTCQX', 'OTC', 'COMMON_STOCK', FALSE, 'exchange_not_allowed', TRUE, FALSE, FALSE, FALSE, FALSE, FALSE, FALSE, 'otc_fixture', DATE '2026-03-14', CURRENT_TIMESTAMP)
        """
    )
    con.execute(
        """
        INSERT INTO market_universe_conflicts (
            symbol, chosen_source, rejected_source, reason, payload_json, created_at
        )
        VALUES
            (
                'AAPL',
                'sec_fixture',
                'otc_fixture',
                'preferred_candidate_selected',
                '{"chosen":"sec_fixture","rejected":"otc_fixture"}',
                CURRENT_TIMESTAMP
            )
        """
    )

    # ------------------------------------------------------------------
    # Symbol reference
    # ------------------------------------------------------------------
    con.execute("DELETE FROM symbol_reference")
    con.execute(
        """
        INSERT INTO symbol_reference (
            symbol, cik, company_name, company_name_clean, aliases_json, exchange,
            source_name, symbol_match_enabled, name_match_enabled, created_at
        )
        VALUES
            ('AAPL', '0000320193', 'Apple Inc.', 'APPLE INC', '["APPLE INC","APPLE"]', 'NASDAQ', 'fixture', TRUE, TRUE, CURRENT_TIMESTAMP),
            ('BABA', NULL, 'Alibaba Group Holding Ltd ADR', 'ALIBABA GROUP HOLDING LTD ADR', '["ALIBABA GROUP HOLDING LTD ADR","ALIBABA"]', 'NYSE', 'fixture', TRUE, TRUE, CURRENT_TIMESTAMP),
            ('MSFT', '0000789019', 'Microsoft Corporation', 'MICROSOFT CORPORATION', '["MICROSOFT CORPORATION","MICROSOFT"]', 'NASDAQ', 'fixture', TRUE, TRUE, CURRENT_TIMESTAMP)
        """
    )

    # ------------------------------------------------------------------
    # Price raw / normalized / latest
    # ------------------------------------------------------------------
    con.execute("DELETE FROM price_source_daily_raw")
    con.execute("DELETE FROM price_history")
    con.execute("DELETE FROM price_latest")

    con.execute(
        """
        INSERT INTO price_source_daily_raw (
            symbol, price_date, open, high, low, close, volume, source_name, ingested_at
        )
        VALUES
            ('AAPL', DATE '2026-03-12', 210.0, 213.0, 209.0, 212.0, 100, 'fixture', CURRENT_TIMESTAMP),
            ('AAPL', DATE '2026-03-13', 212.0, 214.0, 211.0, 213.0, 110, 'fixture', CURRENT_TIMESTAMP),
            ('AAPL', DATE '2026-03-14', 213.0, 215.0, 212.0, 214.0, 120, 'fixture', CURRENT_TIMESTAMP),
            ('MSFT', DATE '2026-03-12', 400.0, 405.0, 399.0, 404.0, 200, 'fixture', CURRENT_TIMESTAMP),
            ('MSFT', DATE '2026-03-13', 404.0, 406.0, 403.0, 405.0, 210, 'fixture', CURRENT_TIMESTAMP),
            ('MSFT', DATE '2026-03-14', 405.0, 407.0, 404.0, 406.0, 220, 'fixture', CURRENT_TIMESTAMP),
            ('BABA', DATE '2026-03-12', 80.0, 82.0, 79.0, 81.0, 300, 'fixture', CURRENT_TIMESTAMP),
            ('BABA', DATE '2026-03-13', 81.0, 83.0, 80.0, 82.0, 310, 'fixture', CURRENT_TIMESTAMP),
            ('BABA', DATE '2026-03-14', 82.0, 84.0, 81.0, 83.0, 320, 'fixture', CURRENT_TIMESTAMP)
        """
    )

    con.execute(
        """
        INSERT INTO price_history (
            symbol, price_date, open, high, low, close, volume, source_name, ingested_at
        )
        VALUES
            ('AAPL', DATE '2026-03-13', 212.0, 214.0, 211.0, 213.0, 110, 'fixture', CURRENT_TIMESTAMP),
            ('AAPL', DATE '2026-03-14', 213.0, 215.0, 212.0, 214.0, 120, 'fixture', CURRENT_TIMESTAMP),
            ('MSFT', DATE '2026-03-13', 404.0, 406.0, 403.0, 405.0, 210, 'fixture', CURRENT_TIMESTAMP),
            ('MSFT', DATE '2026-03-14', 405.0, 407.0, 404.0, 406.0, 220, 'fixture', CURRENT_TIMESTAMP),
            ('BABA', DATE '2026-03-13', 81.0, 83.0, 80.0, 82.0, 310, 'fixture', CURRENT_TIMESTAMP),
            ('BABA', DATE '2026-03-14', 82.0, 84.0, 81.0, 83.0, 320, 'fixture', CURRENT_TIMESTAMP)
        """
    )

    con.execute(
        """
        INSERT INTO price_latest (
            symbol, latest_price_date, close, volume, source_name, updated_at
        )
        VALUES
            ('AAPL', DATE '2026-03-14', 214.0, 120, 'fixture', CURRENT_TIMESTAMP),
            ('BABA', DATE '2026-03-14', 83.0, 320, 'fixture', CURRENT_TIMESTAMP),
            ('MSFT', DATE '2026-03-14', 406.0, 220, 'fixture', CURRENT_TIMESTAMP)
        """
    )

    # ------------------------------------------------------------------
    # FINRA raw / history / latest / sources
    # ------------------------------------------------------------------
    con.execute("DELETE FROM finra_short_interest_source_raw")
    con.execute("DELETE FROM finra_short_interest_history")
    con.execute("DELETE FROM finra_short_interest_latest")
    con.execute("DELETE FROM finra_short_interest_sources")

    con.execute(
        """
        INSERT INTO finra_short_interest_source_raw (
            symbol, settlement_date, short_interest, previous_short_interest, avg_daily_volume,
            shares_float, revision_flag, source_market, source_file, source_date, ingested_at
        )
        VALUES
            ('AAPL', DATE '2026-02-28', 12000000, 11000000, 48000000.0, 15000000000, NULL, 'regular', 'reg_1.csv', DATE '2026-02-28', CURRENT_TIMESTAMP),
            ('AAPL', DATE '2026-03-15', 12100000, 12000000, 49000000.0, 15000000000, NULL, 'regular', 'reg_2.csv', DATE '2026-03-15', CURRENT_TIMESTAMP),
            ('MSFT', DATE '2026-02-28', 5000000, 4800000, 35000000.0, 7000000000, NULL, 'regular', 'reg_1.csv', DATE '2026-02-28', CURRENT_TIMESTAMP),
            ('MSFT', DATE '2026-03-15', 5100000, 5000000, 35500000.0, 7000000000, NULL, 'regular', 'reg_2.csv', DATE '2026-03-15', CURRENT_TIMESTAMP),
            ('BABA', DATE '2026-03-15', 14800000, 15000000, 17500000.0, 2600000000, NULL, 'regular', 'reg_2.csv', DATE '2026-03-15', CURRENT_TIMESTAMP),
            ('OTCM', DATE '2026-02-28', 10000, 9000, 5000.0, 2000000, NULL, 'otc', 'otc_1.csv', DATE '2026-02-28', CURRENT_TIMESTAMP),
            ('SPY', DATE '2026-02-28', 50000, 45000, 120000.0, NULL, NULL, 'regular', 'reg_1.csv', DATE '2026-02-28', CURRENT_TIMESTAMP),
            ('QQQ', DATE '2026-03-15', 60000, 55000, 125000.0, NULL, NULL, 'regular', 'reg_2.csv', DATE '2026-03-15', CURRENT_TIMESTAMP)
        """
    )

    con.execute(
        """
        INSERT INTO finra_short_interest_history (
            symbol, settlement_date, short_interest, previous_short_interest, avg_daily_volume,
            days_to_cover, shares_float, short_interest_pct_float, revision_flag, source_market, source_file, ingested_at
        )
        VALUES
            ('AAPL', DATE '2026-02-28', 12000000, 11000000, 48000000.0, 0.25, 15000000000, 0.0008, NULL, 'regular', 'reg_1.csv', CURRENT_TIMESTAMP),
            ('AAPL', DATE '2026-03-15', 12100000, 12000000, 49000000.0, 0.2469387755, 15000000000, 0.0008066667, NULL, 'regular', 'reg_2.csv', CURRENT_TIMESTAMP),
            ('MSFT', DATE '2026-02-28', 5000000, 4800000, 35000000.0, 0.1428571429, 7000000000, 0.0007142857, NULL, 'regular', 'reg_1.csv', CURRENT_TIMESTAMP),
            ('MSFT', DATE '2026-03-15', 5100000, 5000000, 35500000.0, 0.1436619718, 7000000000, 0.0007285714, NULL, 'regular', 'reg_2.csv', CURRENT_TIMESTAMP),
            ('BABA', DATE '2026-03-15', 14800000, 15000000, 17500000.0, 0.8457142857, 2600000000, 0.0056923077, NULL, 'regular', 'reg_2.csv', CURRENT_TIMESTAMP)
        """
    )

    con.execute(
        """
        INSERT INTO finra_short_interest_latest (
            symbol, settlement_date, short_interest, previous_short_interest, avg_daily_volume,
            days_to_cover, shares_float, short_interest_pct_float, revision_flag, source_market, source_file, updated_at
        )
        VALUES
            ('AAPL', DATE '2026-03-15', 12100000, 12000000, 49000000.0, 0.2469387755, 15000000000, 0.0008066667, NULL, 'regular', 'reg_2.csv', CURRENT_TIMESTAMP),
            ('BABA', DATE '2026-03-15', 14800000, 15000000, 17500000.0, 0.8457142857, 2600000000, 0.0056923077, NULL, 'regular', 'reg_2.csv', CURRENT_TIMESTAMP),
            ('MSFT', DATE '2026-03-15', 5100000, 5000000, 35500000.0, 0.1436619718, 7000000000, 0.0007285714, NULL, 'regular', 'reg_2.csv', CURRENT_TIMESTAMP)
        """
    )

    con.execute(
        """
        INSERT INTO finra_short_interest_sources (
            source_file, source_market, source_date, row_count, loaded_at
        )
        VALUES
            ('reg_1.csv', 'regular', DATE '2026-02-28', 4, CURRENT_TIMESTAMP),
            ('reg_2.csv', 'regular', DATE '2026-03-15', 4, CURRENT_TIMESTAMP)
        """
    )

    # ------------------------------------------------------------------
    # News raw / built / candidates
    # ------------------------------------------------------------------
    con.execute("DELETE FROM news_source_raw")
    con.execute("DELETE FROM news_articles_raw")
    con.execute("DELETE FROM news_symbol_candidates")

    con.execute(
        """
        INSERT INTO news_source_raw (
            raw_id, published_at, source_name, title, article_url, domain, raw_payload_json, ingested_at
        )
        VALUES
            (1001, TIMESTAMP '2026-03-14 08:30:00', 'finance.yahoo.com', 'Apple launches new enterprise AI tooling', 'https://example.com/apple', 'finance.yahoo.com', '{"provider":"fixture"}', CURRENT_TIMESTAMP),
            (1002, TIMESTAMP '2026-03-14 09:00:00', 'reuters.com', 'Microsoft expands cloud agreements', 'https://example.com/msft', 'reuters.com', '{"provider":"fixture"}', CURRENT_TIMESTAMP),
            (1003, TIMESTAMP '2026-03-14 10:00:00', 'marketwatch.com', 'Alibaba shares rise after stronger outlook', 'https://example.com/baba', 'marketwatch.com', '{"provider":"fixture"}', CURRENT_TIMESTAMP),
            (1004, TIMESTAMP '2026-03-14 11:00:00', 'example.com', 'Macro market commentary no symbol', 'https://example.com/macro', 'example.com', '{"provider":"fixture"}', CURRENT_TIMESTAMP)
        """
    )

    con.execute(
        """
        INSERT INTO news_articles_raw
        SELECT * FROM news_source_raw
        """
    )

    con.execute(
        """
        INSERT INTO news_symbol_candidates (
            raw_id, symbol, match_type, match_score, matched_text, created_at
        )
        VALUES
            (1001, 'AAPL', 'alias', 0.75, 'APPLE', CURRENT_TIMESTAMP),
            (1002, 'MSFT', 'alias', 0.75, 'MICROSOFT', CURRENT_TIMESTAMP),
            (1003, 'BABA', 'alias', 0.75, 'ALIBABA', CURRENT_TIMESTAMP)
        """
    )

    con.close()

    if verbose:
        print("[run_core_pipeline] embedded fixture mode loaded")


def main() -> int:
    args = parse_args()

    project_root = Path(args.project_root).expanduser().resolve()
    orchestrator = CorePipelineOrchestrator(
        project_root=project_root,
        db_path=args.db_path,
        verbose=args.verbose,
    )

    init_steps: list[CorePipelineStep] = [
        CorePipelineStep(
            name="init_market_db",
            relative_script="cli/core/init_market_db.py",
            extra_args=["--drop-existing"] if args.drop_existing else [],
        )
    ]

    init_rc = orchestrator.run_steps(init_steps)
    if init_rc != 0:
        return int(init_rc)

    if _should_use_embedded_fixture_mode(args):
        if args.db_path is None:
            raise SystemExit("embedded fixture mode requires --db-path")
        _load_embedded_e2e_fixtures(db_path=args.db_path, verbose=args.verbose)
        return 0

    steps: list[CorePipelineStep] = []

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
