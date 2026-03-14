# Pipeline Usage

## Core pipeline order

The core pipeline runs the following steps in order:

1. init_market_db.py
2. build_market_universe.py
3. build_symbol_reference.py
4. build_stooq_prices.py
5. build_finra_short_interest.py
6. build_news_raw.py
7. build_news_symbol_candidates.py

## Run the full core pipeline

python cli/run_core_pipeline.py --drop-existing

## Run with a custom database path

python cli/run_core_pipeline.py --db-path /path/to/market.duckdb

## Skip optional steps

python cli/run_core_pipeline.py --skip-prices --skip-finra --skip-news-raw --skip-news-candidates

## Suggested validation

python -m compileall stock_quant
pytest -ra
python cli/run_core_pipeline.py --drop-existing
