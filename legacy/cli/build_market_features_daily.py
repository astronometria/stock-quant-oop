#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import time

import duckdb


def parse_args():
    p = argparse.ArgumentParser(description="Build market_features_daily dataset.")
    p.add_argument("--db-path", required=True)
    p.add_argument("--truncate", action="store_true")
    p.add_argument("--verbose", action="store_true")
    return p.parse_args()


def create_table(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("""
    CREATE TABLE IF NOT EXISTS market_features_daily (
        symbol VARCHAR,
        feature_date DATE,

        close DOUBLE,
        volume BIGINT,

        return_1d DOUBLE,
        return_5d DOUBLE,

        fwd_return_1d DOUBLE,
        fwd_return_5d DOUBLE,
        fwd_return_10d DOUBLE,

        news_count BIGINT,
        avg_match_score DOUBLE,
        distinct_sources BIGINT,
        distinct_domains BIGINT,

        llm_news_count BIGINT,
        positive_news_count BIGINT,
        neutral_news_count BIGINT,
        negative_news_count BIGINT,

        avg_sentiment_score DOUBLE,
        avg_relevance_score DOUBLE,
        avg_materiality_score DOUBLE,
        avg_confidence DOUBLE,

        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)


def main():
    args = parse_args()
    con = duckdb.connect(args.db_path)

    started = time.time()

    if args.truncate:
        con.execute("DROP TABLE IF EXISTS market_features_daily")

    create_table(con)

    before = con.execute("SELECT COUNT(*) FROM market_features_daily").fetchone()[0]

    con.execute("""
    INSERT INTO market_features_daily (
        symbol,
        feature_date,

        close,
        volume,

        return_1d,
        return_5d,

        fwd_return_1d,
        fwd_return_5d,
        fwd_return_10d,

        news_count,
        avg_match_score,
        distinct_sources,
        distinct_domains,

        llm_news_count,
        positive_news_count,
        neutral_news_count,
        negative_news_count,

        avg_sentiment_score,
        avg_relevance_score,
        avg_materiality_score,
        avg_confidence
    )
    WITH price_base AS (
        SELECT
            p.symbol,
            p.price_date,
            p.close,
            p.volume,

            (p.close / LAG(p.close) OVER w - 1) AS return_1d,
            (p.close / LAG(p.close, 5) OVER w - 1) AS return_5d,

            (LEAD(p.close) OVER w / p.close - 1) AS fwd_return_1d,
            (LEAD(p.close, 5) OVER w / p.close - 1) AS fwd_return_5d,
            (LEAD(p.close, 10) OVER w / p.close - 1) AS fwd_return_10d

        FROM price_history p
        WINDOW w AS (PARTITION BY p.symbol ORDER BY p.price_date)
    ),
    news_classic AS (
        SELECT
            symbol,
            feature_date,
            news_count,
            avg_match_score,
            distinct_sources,
            distinct_domains
        FROM news_features_daily
    ),
    news_llm AS (
        SELECT
            symbol,
            feature_date,
            news_count AS llm_news_count,
            positive_news_count,
            neutral_news_count,
            negative_news_count,
            avg_sentiment_score,
            avg_relevance_score,
            avg_materiality_score,
            avg_confidence
        FROM news_features_daily_llm
    )
    SELECT
        pb.symbol,
        pb.price_date AS feature_date,

        pb.close,
        pb.volume,

        pb.return_1d,
        pb.return_5d,

        pb.fwd_return_1d,
        pb.fwd_return_5d,
        pb.fwd_return_10d,

        nc.news_count,
        nc.avg_match_score,
        nc.distinct_sources,
        nc.distinct_domains,

        nl.llm_news_count,
        nl.positive_news_count,
        nl.neutral_news_count,
        nl.negative_news_count,

        nl.avg_sentiment_score,
        nl.avg_relevance_score,
        nl.avg_materiality_score,
        nl.avg_confidence

    FROM price_base pb
    LEFT JOIN news_classic nc
      ON pb.symbol = nc.symbol
     AND pb.price_date = nc.feature_date
    LEFT JOIN news_llm nl
      ON pb.symbol = nl.symbol
     AND pb.price_date = nl.feature_date
    """)

    after = con.execute("SELECT COUNT(*) FROM market_features_daily").fetchone()[0]

    duration = round(time.time() - started, 6)

    result = {
        "pipeline_name": "build_market_features_daily",
        "duration_seconds": duration,
        "rows_written": after - before,
        "total_rows": after,
        "status": "SUCCESS",
    }

    if args.verbose:
        print(f"[build_market_features_daily] db_path={args.db_path}")
        print(f"[build_market_features_daily] rows_written={after - before}")

    print(json.dumps(result, indent=2))
    con.close()


if __name__ == "__main__":
    main()
