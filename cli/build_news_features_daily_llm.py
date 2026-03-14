#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import time

import duckdb


def parse_args():
    p = argparse.ArgumentParser(description="Build daily LLM news features.")
    p.add_argument("--db-path", required=True)
    p.add_argument("--truncate", action="store_true")
    p.add_argument("--verbose", action="store_true")
    return p.parse_args()


def main():
    args = parse_args()
    con = duckdb.connect(args.db_path)

    started = time.time()

    con.execute("""
    CREATE TABLE IF NOT EXISTS news_features_daily_llm (
        symbol VARCHAR,
        feature_date DATE,

        news_count BIGINT,

        positive_news_count BIGINT,
        neutral_news_count BIGINT,
        negative_news_count BIGINT,

        avg_sentiment_score DOUBLE,
        max_sentiment_score DOUBLE,
        min_sentiment_score DOUBLE,

        avg_relevance_score DOUBLE,
        avg_materiality_score DOUBLE,
        avg_confidence DOUBLE,

        distinct_event_types BIGINT,

        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)

    if args.truncate:
        con.execute("DELETE FROM news_features_daily_llm")

    before = con.execute(
        "SELECT COUNT(*) FROM news_features_daily_llm"
    ).fetchone()[0]

    con.execute("""
    INSERT INTO news_features_daily_llm (
        symbol,
        feature_date,
        news_count,
        positive_news_count,
        neutral_news_count,
        negative_news_count,
        avg_sentiment_score,
        max_sentiment_score,
        min_sentiment_score,
        avg_relevance_score,
        avg_materiality_score,
        avg_confidence,
        distinct_event_types
    )
    SELECT
        symbol,
        CAST(date_trunc('day', published_at) AS DATE) AS feature_date,

        COUNT(*) AS news_count,

        SUM(CASE WHEN sentiment_label = 'positive' THEN 1 ELSE 0 END) AS positive_news_count,
        SUM(CASE WHEN sentiment_label = 'neutral' THEN 1 ELSE 0 END) AS neutral_news_count,
        SUM(CASE WHEN sentiment_label = 'negative' THEN 1 ELSE 0 END) AS negative_news_count,

        AVG(sentiment_score) AS avg_sentiment_score,
        MAX(sentiment_score) AS max_sentiment_score,
        MIN(sentiment_score) AS min_sentiment_score,

        AVG(relevance_score) AS avg_relevance_score,
        AVG(materiality_score) AS avg_materiality_score,
        AVG(confidence) AS avg_confidence,

        COUNT(DISTINCT event_type) AS distinct_event_types
    FROM news_llm_enrichment
    GROUP BY 1, 2
    """)

    after = con.execute(
        "SELECT COUNT(*) FROM news_features_daily_llm"
    ).fetchone()[0]

    duration = round(time.time() - started, 6)

    result = {
        "pipeline_name": "build_news_features_daily_llm",
        "duration_seconds": duration,
        "rows_written": after - before,
        "total_rows": after,
        "status": "SUCCESS",
    }

    if args.verbose:
        print(f"[build_news_features_daily_llm] db_path={args.db_path}")
        print(f"[build_news_features_daily_llm] rows_written={after - before}")

    print(json.dumps(result, indent=2))
    con.close()


if __name__ == "__main__":
    main()
