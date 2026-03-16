from __future__ import annotations

from typing import Any

from stock_quant.domain.entities.news import NewsArticleRaw, NewsSymbolCandidate, RawNewsSourceRecord
from stock_quant.domain.entities.symbol_reference import SymbolReferenceEntry
from stock_quant.domain.ports.repositories import NewsRepositoryPort
from stock_quant.infrastructure.db.table_names import NEWS_ARTICLES_RAW, NEWS_SOURCE_RAW, NEWS_SYMBOL_CANDIDATES, SYMBOL_REFERENCE
from stock_quant.shared.exceptions import RepositoryError


class DuckDbNewsRepository(NewsRepositoryPort):
    def __init__(self, con: Any) -> None:
        self.con = con

    def _require_connection(self):
        if self.con is None:
            raise RepositoryError("active DB connection is required")
        return self.con

    def con(self):
        if self.uow.connection is None:
            raise RepositoryError("active DB connection is required")
        return self.uow.connection

    def load_raw_news_source_records(self) -> list[RawNewsSourceRecord]:
        try:
            rows = self.con.execute(
                f"""
                SELECT
                    raw_id,
                    published_at,
                    source_name,
                    title,
                    article_url,
                    domain,
                    raw_payload_json
                FROM {NEWS_SOURCE_RAW}
                ORDER BY published_at, raw_id
                """
            ).fetchall()
            return [
                RawNewsSourceRecord(
                    raw_id=row[0],
                    published_at=row[1],
                    source_name=row[2] or "unknown_source",
                    title=row[3] or "",
                    article_url=row[4] or "",
                    domain=row[5],
                    raw_payload_json=row[6],
                )
                for row in rows
            ]
        except Exception as exc:
            raise RepositoryError(f"failed to load raw news source records: {exc}") from exc

    def replace_news_articles_raw(self, entries: list[NewsArticleRaw]) -> int:
        try:
            self.con.execute(f"DELETE FROM {NEWS_ARTICLES_RAW}")
            if not entries:
                return 0

            rows = [
                (
                    e.raw_id,
                    e.published_at,
                    e.source_name,
                    e.title,
                    e.article_url,
                    e.domain,
                    e.raw_payload_json,
                    e.ingested_at,
                )
                for e in entries
            ]

            self.con.executemany(
                f"""
                INSERT INTO {NEWS_ARTICLES_RAW} (
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
            return len(rows)
        except Exception as exc:
            raise RepositoryError(f"failed to replace news_articles_raw: {exc}") from exc

    def load_news_articles_raw(self) -> list[NewsArticleRaw]:
        try:
            rows = self.con.execute(
                f"""
                SELECT
                    raw_id,
                    published_at,
                    source_name,
                    title,
                    article_url,
                    domain,
                    raw_payload_json,
                    ingested_at
                FROM {NEWS_ARTICLES_RAW}
                ORDER BY published_at, raw_id
                """
            ).fetchall()
            return [
                NewsArticleRaw(
                    raw_id=row[0],
                    published_at=row[1],
                    source_name=row[2] or "unknown_source",
                    title=row[3] or "",
                    article_url=row[4] or "",
                    domain=row[5],
                    raw_payload_json=row[6],
                    ingested_at=row[7],
                )
                for row in rows
            ]
        except Exception as exc:
            raise RepositoryError(f"failed to load news_articles_raw: {exc}") from exc

    def load_symbol_reference_entries(self) -> list[SymbolReferenceEntry]:
        try:
            rows = self.con.execute(
                f"""
                SELECT
                    symbol,
                    cik,
                    company_name,
                    company_name_clean,
                    aliases_json,
                    exchange,
                    source_name,
                    symbol_match_enabled,
                    name_match_enabled,
                    created_at
                FROM {SYMBOL_REFERENCE}
                ORDER BY symbol
                """
            ).fetchall()
            return [
                SymbolReferenceEntry(
                    symbol=row[0],
                    cik=row[1],
                    company_name=row[2],
                    company_name_clean=row[3],
                    aliases_json=row[4],
                    exchange=row[5],
                    source_name=row[6],
                    symbol_match_enabled=row[7],
                    name_match_enabled=row[8],
                    created_at=row[9],
                )
                for row in rows
            ]
        except Exception as exc:
            raise RepositoryError(f"failed to load symbol_reference entries: {exc}") from exc

    def replace_news_symbol_candidates(self, entries: list[NewsSymbolCandidate]) -> int:
        try:
            self.con.execute(f"DELETE FROM {NEWS_SYMBOL_CANDIDATES}")
            if not entries:
                return 0

            rows = [
                (
                    e.raw_id,
                    e.symbol,
                    e.match_type,
                    e.match_score,
                    e.matched_text,
                    e.created_at,
                )
                for e in entries
            ]

            self.con.executemany(
                f"""
                INSERT INTO {NEWS_SYMBOL_CANDIDATES} (
                    raw_id,
                    symbol,
                    match_type,
                    match_score,
                    matched_text,
                    created_at
                )
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                rows,
            )
            return len(rows)
        except Exception as exc:
            raise RepositoryError(f"failed to replace news_symbol_candidates: {exc}") from exc
