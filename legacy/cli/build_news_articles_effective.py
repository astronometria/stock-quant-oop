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
    p = argparse.ArgumentParser(description="Build news_articles_effective from raw + symbol candidates.")
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
        CREATE TABLE IF NOT EXISTS news_articles_effective (
            raw_id BIGINT,
            symbol VARCHAR,
            published_at TIMESTAMP,
            source_name VARCHAR,
            domain VARCHAR,
            title VARCHAR,
            article_url VARCHAR,
            match_type VARCHAR,
            match_score DOUBLE,
            matched_text VARCHAR,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)

        if args.truncate:
            con.execute("DELETE FROM news_articles_effective")

        before = con.execute("SELECT COUNT(*) FROM news_articles_effective").fetchone()[0]

        con.execute("""
        INSERT INTO news_articles_effective (
            raw_id,
            symbol,
            published_at,
            source_name,
            domain,
            title,
            article_url,
            match_type,
            match_score,
            matched_text
        )
        SELECT
            a.raw_id,
            c.symbol,
            a.published_at,
            a.source_name,
            a.domain,
            a.title,
            a.article_url,
            c.match_type,
            c.match_score,
            c.matched_text
        FROM news_articles_raw a
        JOIN news_symbol_candidates c
            ON a.raw_id = c.raw_id
        LEFT JOIN news_articles_effective e
            ON e.raw_id = a.raw_id
           AND e.symbol = c.symbol
        WHERE e.raw_id IS NULL
        """)

        after = con.execute("SELECT COUNT(*) FROM news_articles_effective").fetchone()[0]

    duration = round(time.time() - started, 6)

    result = {
        "pipeline_name": "build_news_articles_effective",
        "duration_seconds": duration,
        "rows_written": after - before,
        "total_rows": after,
        "status": "SUCCESS",
    }

    if args.verbose:
        print(f"[build_news_articles_effective] db_path={config.db_path}")
        print(f"[build_news_articles_effective] rows_written={after-before}")

    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
