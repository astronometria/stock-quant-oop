#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from datetime import datetime

from stock_quant.infrastructure.config.settings_loader import build_app_config
from stock_quant.infrastructure.db.duckdb_session_factory import DuckDbSessionFactory
from stock_quant.infrastructure.db.schema_manager import SchemaManager
from stock_quant.infrastructure.db.table_names import NEWS_SOURCE_RAW
from stock_quant.infrastructure.db.unit_of_work import DuckDbUnitOfWork


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load fixture rows into news_source_raw.")
    parser.add_argument("--db-path", default=None, help="Path to DuckDB database file.")
    parser.add_argument("--truncate", action="store_true", help="Delete existing staging rows before insert.")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose output.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    config = build_app_config(db_path=args.db_path)
    config.ensure_directories()

    now = datetime.utcnow()

    rows = [
        (
            1001,
            "2026-03-14 08:30:00",
            "finance.yahoo.com",
            "Apple launches new enterprise AI tooling for developers",
            "https://finance.yahoo.com/news/apple-launches-enterprise-ai-tooling-1001.html",
            "finance.yahoo.com",
            json.dumps({"provider": "fixture", "topic": "apple"}, sort_keys=True),
            now,
        ),
        (
            1002,
            "2026-03-14 09:00:00",
            "reuters.com",
            "Microsoft expands cloud agreements with major banking clients",
            "https://www.reuters.com/world/us/microsoft-cloud-banking-1002/",
            "reuters.com",
            json.dumps({"provider": "fixture", "topic": "microsoft"}, sort_keys=True),
            now,
        ),
        (
            1003,
            "2026-03-14 09:20:00",
            "bloomberg.com",
            "Alibaba shares rise after stronger commerce outlook",
            "https://www.bloomberg.com/news/articles/2026-03-14/alibaba-outlook-1003",
            "bloomberg.com",
            json.dumps({"provider": "fixture", "topic": "alibaba"}, sort_keys=True),
            now,
        ),
        (
            1004,
            "2026-03-14 09:45:00",
            "marketwatch.com",
            "ETF flows accelerate as broad market rally continues",
            "https://www.marketwatch.com/story/etf-flows-rally-1004",
            "marketwatch.com",
            json.dumps({"provider": "fixture", "topic": "etf"}, sort_keys=True),
            now,
        ),
    ]

    session_factory = DuckDbSessionFactory(config.db_path)

    with DuckDbUnitOfWork(session_factory) as uow:
        schema_manager = SchemaManager(uow)
        schema_manager.validate()

        con = uow.connection
        if con is None:
            raise RuntimeError("missing active connection")

        if args.truncate:
            con.execute(f"DELETE FROM {NEWS_SOURCE_RAW}")

        con.executemany(
            f"""
            INSERT INTO {NEWS_SOURCE_RAW} (
                raw_id,
                published_at,
                source_name,
                title,
                article_url,
                domain,
                raw_payload_json,
                ingested_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )

        count = con.execute(f"SELECT COUNT(*) FROM {NEWS_SOURCE_RAW}").fetchone()[0]

    if args.verbose:
        print(f"[load_news_source_raw] db_path={config.db_path}")
        print(f"[load_news_source_raw] inserted_rows={len(rows)}")
        print(f"[load_news_source_raw] total_rows={count}")

    print(f"Loaded news_source_raw rows: total_rows={count}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
