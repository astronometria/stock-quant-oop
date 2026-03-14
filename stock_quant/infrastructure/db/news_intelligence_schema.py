from __future__ import annotations

from stock_quant.infrastructure.db.unit_of_work import DuckDbUnitOfWork


class NewsIntelligenceSchemaManager:
    def __init__(self, uow: DuckDbUnitOfWork) -> None:
        self.uow = uow

    @property
    def con(self):
        if self.uow.connection is None:
            raise RuntimeError("active DB connection is required")
        return self.uow.connection

    def initialize(self, drop_existing: bool = False) -> None:
        if drop_existing:
            self._drop_tables()

        self._create_news_articles_normalized()
        self._create_news_symbol_links()
        self._create_news_clusters()
        self._create_news_event_windows()
        self._create_news_llm_enrichment()
        self._create_news_features_daily()

    def _drop_tables(self) -> None:
        for table_name in [
            "news_features_daily",
            "news_llm_enrichment",
            "news_event_windows",
            "news_clusters",
            "news_symbol_links",
            "news_articles_normalized",
        ]:
            self.con.execute(f"DROP TABLE IF EXISTS {table_name}")

    def _create_news_articles_normalized(self) -> None:
        self.con.execute(
            """
            CREATE TABLE IF NOT EXISTS news_articles_normalized (
                article_id VARCHAR,
                published_at TIMESTAMP,
                available_at TIMESTAMP,
                source_name VARCHAR,
                title VARCHAR,
                article_url VARCHAR,
                article_hash VARCHAR,
                cluster_id VARCHAR,
                created_at TIMESTAMP
            )
            """
        )

    def _create_news_symbol_links(self) -> None:
        self.con.execute(
            """
            CREATE TABLE IF NOT EXISTS news_symbol_links (
                article_id VARCHAR,
                instrument_id VARCHAR,
                company_id VARCHAR,
                symbol VARCHAR,
                link_confidence DOUBLE,
                linking_method VARCHAR,
                created_at TIMESTAMP
            )
            """
        )

    def _create_news_clusters(self) -> None:
        self.con.execute(
            """
            CREATE TABLE IF NOT EXISTS news_clusters (
                cluster_id VARCHAR,
                article_id VARCHAR,
                created_at TIMESTAMP
            )
            """
        )

    def _create_news_event_windows(self) -> None:
        self.con.execute(
            """
            CREATE TABLE IF NOT EXISTS news_event_windows (
                article_id VARCHAR,
                instrument_id VARCHAR,
                symbol VARCHAR,
                event_date DATE,
                window_start_date DATE,
                window_end_date DATE,
                created_at TIMESTAMP
            )
            """
        )

    def _create_news_llm_enrichment(self) -> None:
        self.con.execute(
            """
            CREATE TABLE IF NOT EXISTS news_llm_enrichment (
                article_id VARCHAR,
                model_name VARCHAR,
                prompt_version VARCHAR,
                event_type VARCHAR,
                sentiment_label VARCHAR,
                causality_score DOUBLE,
                novelty_score DOUBLE,
                summary_text VARCHAR,
                created_at TIMESTAMP
            )
            """
        )

    def _create_news_features_daily(self) -> None:
        self.con.execute(
            """
            CREATE TABLE IF NOT EXISTS news_features_daily (
                instrument_id VARCHAR,
                company_id VARCHAR,
                symbol VARCHAR,
                as_of_date DATE,
                article_count_1d BIGINT,
                unique_cluster_count_1d BIGINT,
                avg_link_confidence DOUBLE,
                source_name VARCHAR,
                created_at TIMESTAMP
            )
            """
        )
