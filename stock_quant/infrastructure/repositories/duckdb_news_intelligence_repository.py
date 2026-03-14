from __future__ import annotations

from datetime import datetime
from typing import Any

from stock_quant.domain.entities.news_intelligence import (
    NewsArticleNormalized,
    NewsEventWindow,
    NewsFeatureDaily,
    NewsLlmEnrichment,
    NewsSymbolLink,
)
from stock_quant.infrastructure.db.unit_of_work import DuckDbUnitOfWork
from stock_quant.shared.exceptions import RepositoryError


class DuckDbNewsIntelligenceRepository:
    def __init__(self, uow: DuckDbUnitOfWork) -> None:
        self.uow = uow

    @property
    def con(self):
        if self.uow.connection is None:
            raise RepositoryError("active DB connection is required")
        return self.uow.connection

    def load_news_articles_raw_rows(self) -> list[dict[str, Any]]:
        try:
            rows = self.con.execute(
                """
                SELECT
                    raw_id,
                    published_at,
                    source_name,
                    title,
                    article_url
                FROM news_articles_raw
                ORDER BY raw_id
                """
            ).fetchall()

            return [
                {
                    "raw_id": row[0],
                    "published_at": row[1],
                    "source_name": row[2],
                    "title": row[3],
                    "article_url": row[4],
                }
                for row in rows
            ]
        except Exception as exc:
            raise RepositoryError(f"failed to load news_articles_raw rows: {exc}") from exc

    def load_news_symbol_candidate_rows(self) -> list[dict[str, Any]]:
        try:
            rows = self.con.execute(
                """
                SELECT
                    raw_id,
                    symbol
                FROM news_symbol_candidates
                ORDER BY raw_id, symbol
                """
            ).fetchall()

            return [
                {
                    "raw_id": row[0],
                    "symbol": row[1],
                }
                for row in rows
            ]
        except Exception as exc:
            raise RepositoryError(f"failed to load news_symbol_candidates rows: {exc}") from exc

    def load_symbol_map(self) -> dict[str, dict[str, str | None]]:
        try:
            rows = self.con.execute(
                """
                SELECT symbol, instrument_id, company_id
                FROM instrument_master
                ORDER BY symbol
                """
            ).fetchall()

            return {
                str(row[0]).strip().upper(): {
                    "instrument_id": row[1],
                    "company_id": row[2],
                }
                for row in rows
            }
        except Exception as exc:
            raise RepositoryError(f"failed to load symbol map: {exc}") from exc

    def replace_news_articles_normalized(self, rows: list[NewsArticleNormalized]) -> int:
        try:
            self.con.execute("DELETE FROM news_articles_normalized")
            self.con.execute("DELETE FROM news_clusters")
            if not rows:
                return 0

            article_payload = [
                (
                    row.article_id,
                    row.published_at,
                    row.available_at,
                    row.source_name,
                    row.title,
                    row.article_url,
                    row.article_hash,
                    row.cluster_id,
                    row.created_at or datetime.utcnow(),
                )
                for row in rows
            ]
            cluster_payload = [
                (
                    row.cluster_id,
                    row.article_id,
                    row.created_at or datetime.utcnow(),
                )
                for row in rows
            ]

            self.con.executemany(
                """
                INSERT INTO news_articles_normalized (
                    article_id,
                    published_at,
                    available_at,
                    source_name,
                    title,
                    article_url,
                    article_hash,
                    cluster_id,
                    created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                article_payload,
            )

            self.con.executemany(
                """
                INSERT INTO news_clusters (
                    cluster_id,
                    article_id,
                    created_at
                )
                VALUES (?, ?, ?)
                """,
                cluster_payload,
            )

            return len(article_payload)
        except Exception as exc:
            raise RepositoryError(f"failed to replace news_articles_normalized/news_clusters: {exc}") from exc

    def replace_news_symbol_links(self, rows: list[NewsSymbolLink]) -> int:
        try:
            self.con.execute("DELETE FROM news_symbol_links")
            if not rows:
                return 0

            payload = [
                (
                    row.article_id,
                    row.instrument_id,
                    row.company_id,
                    row.symbol,
                    row.link_confidence,
                    row.linking_method,
                    row.created_at or datetime.utcnow(),
                )
                for row in rows
            ]
            self.con.executemany(
                """
                INSERT INTO news_symbol_links (
                    article_id,
                    instrument_id,
                    company_id,
                    symbol,
                    link_confidence,
                    linking_method,
                    created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                payload,
            )
            return len(payload)
        except Exception as exc:
            raise RepositoryError(f"failed to replace news_symbol_links: {exc}") from exc

    def replace_news_event_windows(self, rows: list[NewsEventWindow]) -> int:
        try:
            self.con.execute("DELETE FROM news_event_windows")
            if not rows:
                return 0

            payload = [
                (
                    row.article_id,
                    row.instrument_id,
                    row.symbol,
                    row.event_date,
                    row.window_start_date,
                    row.window_end_date,
                    row.created_at or datetime.utcnow(),
                )
                for row in rows
            ]
            self.con.executemany(
                """
                INSERT INTO news_event_windows (
                    article_id,
                    instrument_id,
                    symbol,
                    event_date,
                    window_start_date,
                    window_end_date,
                    created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                payload,
            )
            return len(payload)
        except Exception as exc:
            raise RepositoryError(f"failed to replace news_event_windows: {exc}") from exc

    def replace_news_llm_enrichment(self, rows: list[NewsLlmEnrichment]) -> int:
        try:
            self.con.execute("DELETE FROM news_llm_enrichment")
            if not rows:
                return 0

            payload = [
                (
                    row.article_id,
                    row.model_name,
                    row.prompt_version,
                    row.event_type,
                    row.sentiment_label,
                    row.causality_score,
                    row.novelty_score,
                    row.summary_text,
                    row.created_at or datetime.utcnow(),
                )
                for row in rows
            ]
            self.con.executemany(
                """
                INSERT INTO news_llm_enrichment (
                    article_id,
                    model_name,
                    prompt_version,
                    event_type,
                    sentiment_label,
                    causality_score,
                    novelty_score,
                    summary_text,
                    created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                payload,
            )
            return len(payload)
        except Exception as exc:
            raise RepositoryError(f"failed to replace news_llm_enrichment: {exc}") from exc

    def replace_news_features_daily(self, rows: list[NewsFeatureDaily]) -> int:
        try:
            self.con.execute("DELETE FROM news_features_daily")
            if not rows:
                return 0

            payload = [
                (
                    row.instrument_id,
                    row.company_id,
                    row.symbol,
                    row.as_of_date,
                    row.article_count_1d,
                    row.unique_cluster_count_1d,
                    row.avg_link_confidence,
                    row.source_name,
                    row.created_at or datetime.utcnow(),
                )
                for row in rows
            ]
            self.con.executemany(
                """
                INSERT INTO news_features_daily (
                    instrument_id,
                    company_id,
                    symbol,
                    as_of_date,
                    article_count_1d,
                    unique_cluster_count_1d,
                    avg_link_confidence,
                    source_name,
                    created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                payload,
            )
            return len(payload)
        except Exception as exc:
            raise RepositoryError(f"failed to replace news_features_daily: {exc}") from exc
