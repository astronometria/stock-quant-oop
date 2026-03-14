#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

from stock_quant.infrastructure.config.settings_loader import build_app_config
from stock_quant.infrastructure.db.duckdb_session_factory import DuckDbSessionFactory
from stock_quant.infrastructure.db.schema_manager import SchemaManager
from stock_quant.infrastructure.db.table_names import NEWS_SOURCE_RAW
from stock_quant.infrastructure.db.unit_of_work import DuckDbUnitOfWork


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Load news CSV into news_source_raw."
    )
    parser.add_argument("--db-path", default=None, help="Path to DuckDB database file.")
    parser.add_argument("--csv-path", required=True, help="Path to input CSV.")
    parser.add_argument("--truncate", action="store_true", help="Delete existing rows before load.")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose output.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    config = build_app_config(db_path=args.db_path)
    config.ensure_directories()

    csv_path = Path(args.csv_path).expanduser().resolve()
    if not csv_path.exists():
        raise SystemExit(f"csv file not found: {csv_path}")

    session_factory = DuckDbSessionFactory(config.db_path)

    with DuckDbUnitOfWork(session_factory) as uow:
        schema_manager = SchemaManager(uow)
        schema_manager.validate()

        con = uow.connection
        if con is None:
            raise RuntimeError("missing active connection")

        if args.truncate:
            con.execute(f"DELETE FROM {NEWS_SOURCE_RAW}")

        con.execute(
            """
            DROP TABLE IF EXISTS news_csv_stage
            """
        )

        con.execute(
            f"""
            CREATE TEMP TABLE news_csv_stage AS
            SELECT
                CAST(raw_id AS BIGINT) AS raw_id,
                CAST(published_at AS TIMESTAMP) AS published_at,
                CAST(source_name AS VARCHAR) AS source_name,
                CAST(title AS VARCHAR) AS title,
                CAST(article_url AS VARCHAR) AS article_url,
                CAST(domain AS VARCHAR) AS domain,
                CAST(raw_payload_json AS VARCHAR) AS raw_payload_json
            FROM read_csv_auto(
                '{csv_path}',
                header = true
            )
            """
        )

        con.execute(
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
            SELECT
                raw_id,
                published_at,
                COALESCE(source_name, 'unknown_source'),
                COALESCE(title, ''),
                COALESCE(article_url, ''),
                domain,
                raw_payload_json,
                CURRENT_TIMESTAMP
            FROM news_csv_stage
            """
        )

        total_rows = con.execute(f"SELECT COUNT(*) FROM {NEWS_SOURCE_RAW}").fetchone()[0]

    if args.verbose:
        print(f"[load_news_source_raw_csv] db_path={config.db_path}")
        print(f"[load_news_source_raw_csv] csv_path={csv_path}")
        print(f"[load_news_source_raw_csv] total_rows={total_rows}")

    print(
        "Loaded news CSV into news_source_raw: "
        f"total_rows={total_rows} csv_path={csv_path}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
