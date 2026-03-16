#!/usr/bin/env python3
from __future__ import annotations

import argparse
from datetime import date, datetime

import pandas as pd

from stock_quant.infrastructure.config.settings_loader import build_app_config
from stock_quant.infrastructure.db.duckdb_session_factory import DuckDbSessionFactory
from stock_quant.infrastructure.db.schema_manager import SchemaManager
from stock_quant.infrastructure.db.table_names import NEWS_SOURCE_RAW
from stock_quant.infrastructure.db.unit_of_work import DuckDbUnitOfWork
from stock_quant.infrastructure.providers.news.news_source_loader import NewsSourceLoader


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Load real news raw rows into DuckDB from provided sources. Prod-only: no fixture fallback."
    )
    parser.add_argument("--db-path", default=None, help="Path to DuckDB database file.")
    parser.add_argument(
        "--source",
        action="append",
        dest="sources",
        default=[],
        help="Source file path. Repeat this flag for multiple inputs.",
    )
    parser.add_argument("--start-date", default=None, help="Optional start date YYYY-MM-DD.")
    parser.add_argument("--end-date", default=None, help="Optional end date YYYY-MM-DD.")
    parser.add_argument("--limit", type=int, default=None, help="Optional row limit.")
    parser.add_argument("--truncate", action="store_true", help="Delete existing staging rows before insert.")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose output.")
    return parser.parse_args()


def parse_date(value: str | None):
    if value is None:
        return None
    return date.fromisoformat(value)


def normalize_source_frame(frame: pd.DataFrame) -> pd.DataFrame:
    frame = frame.copy()
    frame.columns = [str(col).strip() for col in frame.columns]

    if "raw_id" not in frame.columns:
        raise SystemExit("load_news_source_raw requires a raw_id column in real-source mode.")

    if "ingested_at" not in frame.columns:
        frame["ingested_at"] = datetime.utcnow()

    if "raw_payload_json" not in frame.columns:
        frame["raw_payload_json"] = "{}"

    required = [
        "raw_id",
        "published_at",
        "source_name",
        "title",
        "article_url",
        "domain",
        "raw_payload_json",
        "ingested_at",
    ]
    for col in required:
        if col not in frame.columns:
            frame[col] = None

    return frame[required].reset_index(drop=True)


def load_source_frame(args: argparse.Namespace) -> pd.DataFrame:
    """
    Real-source loader only.

    Prod rule:
    - no --source => hard failure
    - provider empty => hard failure
    """
    if not args.sources:
        raise SystemExit(
            "load_news_source_raw requires at least one --source in prod mode."
        )

    loader = NewsSourceLoader(source_paths=args.sources)
    frame = loader.fetch_news_frame(
        start_date=parse_date(args.start_date),
        end_date=parse_date(args.end_date),
        limit=args.limit,
    )

    if frame is None or frame.empty:
        raise SystemExit(
            "load_news_source_raw received no rows from the provided news source files."
        )

    return normalize_source_frame(frame)


def main() -> int:
    args = parse_args()
    config = build_app_config(db_path=args.db_path)
    config.ensure_directories()

    frame = load_source_frame(args)

    session_factory = DuckDbSessionFactory(config.db_path)
    with DuckDbUnitOfWork(session_factory) as uow:
        schema_manager = SchemaManager(uow)
        schema_manager.validate()

        con = uow.connection
        if con is None:
            raise RuntimeError("missing active DB connection")

        if args.truncate:
            con.execute(f"DELETE FROM {NEWS_SOURCE_RAW}")

        rows_before = int(con.execute(f"SELECT COUNT(*) FROM {NEWS_SOURCE_RAW}").fetchone()[0])

        rows_written = 0
        con.register("tmp_news_source_raw_frame", frame)
        try:
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
                    CAST(raw_id AS BIGINT) AS raw_id,
                    CAST(published_at AS TIMESTAMP) AS published_at,
                    CAST(source_name AS VARCHAR) AS source_name,
                    CAST(title AS VARCHAR) AS title,
                    CAST(article_url AS VARCHAR) AS article_url,
                    CAST(domain AS VARCHAR) AS domain,
                    CAST(raw_payload_json AS VARCHAR) AS raw_payload_json,
                    CAST(ingested_at AS TIMESTAMP) AS ingested_at
                FROM tmp_news_source_raw_frame
                WHERE raw_id IS NOT NULL
                  AND published_at IS NOT NULL
                """
            )
            rows_written = int(len(frame))
        finally:
            try:
                con.unregister("tmp_news_source_raw_frame")
            except Exception:
                pass

        rows_after = int(con.execute(f"SELECT COUNT(*) FROM {NEWS_SOURCE_RAW}").fetchone()[0])

    if args.verbose:
        print(f"[load_news_source_raw] db_path={config.db_path}")
        print("[load_news_source_raw] source_mode=real_sources_only")
        print(f"[load_news_source_raw] source_count={len(args.sources)}")
        print(f"[load_news_source_raw] rows_before={rows_before}")
        print(f"[load_news_source_raw] rows_written={rows_written}")
        print(f"[load_news_source_raw] rows_after={rows_after}")

    print(f"Loaded news_source_raw rows: total_rows={rows_after}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
