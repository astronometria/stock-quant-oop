#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import time

from stock_quant.infrastructure.config.settings_loader import build_app_config
from stock_quant.infrastructure.db.duckdb_session_factory import DuckDbSessionFactory
from stock_quant.infrastructure.db.schema_manager import SchemaManager
from stock_quant.infrastructure.db.unit_of_work import DuckDbUnitOfWork


def parse_args():
    p = argparse.ArgumentParser(description="Build daily news features from news_articles_effective.")
    p.add_argument("--db-path", default=None)
    p.add_argument("--truncate", action="store_true")
    p.add_argument("--verbose", action="store_true")
    return p.parse_args()


def main():
    args = parse_args()
    config = build_app_config(db_path=args.db_path)
    config.ensure_directories()

    started = time.time()
    session_factory = DuckDbSessionFactory(config.db_path)

    with DuckDbUnitOfWork(session_factory) as uow:
        schema_manager = SchemaManager(uow)
        schema_manager.validate()

        con = uow.connection
        if con is None:
            raise RuntimeError("missing active connection")

        con.execute("""
        CREATE TABLE IF NOT EXISTS news_features_daily (
            symbol VARCHAR,
            feature_date DATE,
            news_count BIGINT,
            avg_match_score DOUBLE,
            max_match_score DOUBLE,
            min_match_score DOUBLE,
            distinct_sources BIGINT,
            distinct_domains BIGINT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)

        if args.truncate:
            con.execute("DELETE FROM news_features_daily")

        before = con.execute("SELECT COUNT(*) FROM news_features_daily").fetchone()[0]

        con.execute("""
        INSERT INTO news_features_daily (
            symbol,
            feature_date,
            news_count,
            avg_match_score,
            max_match_score,
            min_match_score,
            distinct_sources,
            distinct_domains
        )
        SELECT
            symbol,
            CAST(date_trunc('day', published_at) AS DATE) AS feature_date,
            COUNT(*) AS news_count,
            AVG(match_score) AS avg_match_score,
            MAX(match_score) AS max_match_score,
            MIN(match_score) AS min_match_score,
            COUNT(DISTINCT source_name) AS distinct_sources,
            COUNT(DISTINCT domain) AS distinct_domains
        FROM news_articles_effective
        GROUP BY 1, 2
        """)

        after = con.execute("SELECT COUNT(*) FROM news_features_daily").fetchone()[0]

    duration = round(time.time() - started, 6)

    result = {
        "pipeline_name": "build_news_features_daily",
        "duration_seconds": duration,
        "rows_written": after - before,
        "total_rows": after,
        "status": "SUCCESS",
    }

    if args.verbose:
        print(f"[build_news_features_daily] db_path={config.db_path}")
        print(f"[build_news_features_daily] rows_written={after - before}")

    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
