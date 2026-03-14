#!/usr/bin/env python3
from __future__ import annotations

import argparse
import duckdb


def parse_args():
    p = argparse.ArgumentParser(description="Initialize LLM enrichment tables.")
    p.add_argument("--db-path", required=True)
    return p.parse_args()


def main():
    args = parse_args()

    con = duckdb.connect(args.db_path)

    con.execute("""
    CREATE TABLE IF NOT EXISTS news_llm_enrichment (
        raw_id BIGINT,
        symbol VARCHAR,
        published_at TIMESTAMP,

        model_name VARCHAR,
        prompt_version VARCHAR,

        sentiment_label VARCHAR,
        sentiment_score DOUBLE,

        relevance_score DOUBLE,
        event_type VARCHAR,
        materiality_score DOUBLE,
        horizon_label VARCHAR,

        summary VARCHAR,
        confidence DOUBLE,

        inference_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

        PRIMARY KEY (raw_id, symbol, model_name, prompt_version)
    )
    """)

    print("news_llm_enrichment table ready")

    con.close()


if __name__ == "__main__":
    main()
