from __future__ import annotations

from stock_quant.app.dto.pipeline_result import PipelineResult
from stock_quant.infrastructure.db.unit_of_work import DuckDbUnitOfWork
from stock_quant.pipelines.base_pipeline import BasePipeline
from stock_quant.shared.exceptions import PipelineError


class BuildNewsIntelligencePipeline(BasePipeline):
    pipeline_name = "build_news_intelligence"

    def __init__(self, repository=None, uow: DuckDbUnitOfWork | None = None) -> None:
        if repository is not None:
            self.uow = repository.uow
        else:
            self.uow = uow

        if self.uow is None:
            raise ValueError("BuildNewsIntelligencePipeline requires repository or uow")

        self._metrics: dict[str, int] = {}
        self._rows_written = 0

    @property
    def con(self):
        if self.uow.connection is None:
            raise PipelineError("active DB connection is required")
        return self.uow.connection

    def extract(self):
        return None

    def transform(self, data):
        return None

    def validate(self, data) -> None:
        raw_articles = int(
            self.con.execute("SELECT COUNT(*) FROM news_articles_raw").fetchone()[0]
        )
        if raw_articles == 0:
            raise PipelineError("no news_articles_raw rows available")

    def load(self, data) -> None:
        con = self.con

        raw_articles = int(
            con.execute("SELECT COUNT(*) FROM news_articles_raw").fetchone()[0]
        )
        symbol_candidates = int(
            con.execute("SELECT COUNT(*) FROM news_symbol_candidates").fetchone()[0]
        )

        con.execute("DELETE FROM news_articles_normalized")
        con.execute("DELETE FROM news_symbol_links")
        con.execute("DELETE FROM news_clusters")
        con.execute("DELETE FROM news_event_windows")
        con.execute("DELETE FROM news_llm_enrichment")
        con.execute("DELETE FROM news_features_daily")

        con.execute("DROP TABLE IF EXISTS tmp_news_articles_normalized")
        con.execute(
            """
            CREATE TEMP TABLE tmp_news_articles_normalized AS
            SELECT
                'ARTICLE:' || SUBSTR(sha1(COALESCE(CAST(raw_id AS VARCHAR), '') || '|' || COALESCE(article_url, '') || '|' || COALESCE(title, '')), 1, 20) AS article_id,
                raw_id,
                published_at,
                published_at AS available_at,
                source_name,
                title,
                article_url,
                sha1(COALESCE(article_url, '') || '|' || COALESCE(title, '')) AS article_hash,
                'CLUSTER:' || SUBSTR(sha1(COALESCE(article_url, '') || '|' || COALESCE(title, '')), 1, 16) AS cluster_id
            FROM news_articles_raw
            """
        )

        con.execute(
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
            SELECT
                article_id,
                published_at,
                available_at,
                source_name,
                title,
                article_url,
                article_hash,
                cluster_id,
                CURRENT_TIMESTAMP AS created_at
            FROM tmp_news_articles_normalized
            """
        )

        normalized_articles = int(
            con.execute("SELECT COUNT(*) FROM news_articles_normalized").fetchone()[0]
        )

        con.execute(
            """
            INSERT INTO news_clusters (
                cluster_id,
                article_id,
                created_at
            )
            SELECT
                cluster_id,
                article_id,
                CURRENT_TIMESTAMP AS created_at
            FROM tmp_news_articles_normalized
            """
        )

        con.execute("DROP TABLE IF EXISTS tmp_news_symbol_links")
        con.execute(
            """
            CREATE TEMP TABLE tmp_news_symbol_links AS
            SELECT
                a.article_id,
                im.instrument_id,
                im.company_id,
                im.symbol,
                1.0 AS link_confidence,
                'symbol_candidate' AS linking_method
            FROM news_symbol_candidates c
            INNER JOIN tmp_news_articles_normalized a
              ON c.raw_id = a.raw_id
            INNER JOIN instrument_master im
              ON UPPER(TRIM(c.symbol)) = UPPER(TRIM(im.symbol))
            """
        )

        con.execute(
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
            SELECT
                article_id,
                instrument_id,
                company_id,
                symbol,
                link_confidence,
                linking_method,
                CURRENT_TIMESTAMP AS created_at
            FROM tmp_news_symbol_links
            """
        )

        news_symbol_links = int(
            con.execute("SELECT COUNT(*) FROM news_symbol_links").fetchone()[0]
        )

        con.execute(
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
            SELECT
                l.article_id,
                l.instrument_id,
                l.symbol,
                CAST(a.published_at AS DATE) AS event_date,
                CAST(a.published_at AS DATE) AS window_start_date,
                CAST(a.published_at AS DATE) + 1 AS window_end_date,
                CURRENT_TIMESTAMP AS created_at
            FROM tmp_news_symbol_links l
            INNER JOIN tmp_news_articles_normalized a
              ON l.article_id = a.article_id
            """
        )

        news_event_windows = int(
            con.execute("SELECT COUNT(*) FROM news_event_windows").fetchone()[0]
        )

        con.execute(
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
            SELECT
                article_id,
                'placeholder' AS model_name,
                'v1' AS prompt_version,
                'unknown' AS event_type,
                'neutral' AS sentiment_label,
                0.0 AS causality_score,
                0.0 AS novelty_score,
                title AS summary_text,
                CURRENT_TIMESTAMP AS created_at
            FROM tmp_news_articles_normalized
            """
        )

        news_llm_enrichment_rows = int(
            con.execute("SELECT COUNT(*) FROM news_llm_enrichment").fetchone()[0]
        )

        con.execute(
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
            SELECT
                l.instrument_id,
                l.company_id,
                l.symbol,
                CAST(a.published_at AS DATE) AS as_of_date,
                COUNT(*) AS article_count_1d,
                COUNT(DISTINCT a.cluster_id) AS unique_cluster_count_1d,
                AVG(l.link_confidence) AS avg_link_confidence,
                'news' AS source_name,
                CURRENT_TIMESTAMP AS created_at
            FROM tmp_news_symbol_links l
            INNER JOIN tmp_news_articles_normalized a
              ON l.article_id = a.article_id
            GROUP BY
                l.instrument_id,
                l.company_id,
                l.symbol,
                CAST(a.published_at AS DATE)
            """
        )

        news_feature_rows = int(
            con.execute("SELECT COUNT(*) FROM news_features_daily").fetchone()[0]
        )

        self._rows_written = (
            normalized_articles
            + news_symbol_links
            + news_event_windows
            + news_llm_enrichment_rows
            + news_feature_rows
        )

        self._metrics = {
            "raw_articles": raw_articles,
            "symbol_candidates": symbol_candidates,
            "normalized_articles": normalized_articles,
            "news_symbol_links": news_symbol_links,
            "news_event_windows": news_event_windows,
            "news_llm_enrichment_rows": news_llm_enrichment_rows,
            "news_feature_rows": news_feature_rows,
            "written_articles": normalized_articles,
            "written_links": news_symbol_links,
            "written_event_windows": news_event_windows,
            "written_llm": news_llm_enrichment_rows,
            "written_features": news_feature_rows,
        }

    def finalize(self, result: PipelineResult) -> PipelineResult:
        result.rows_read = int(self._metrics.get("raw_articles", 0))
        result.rows_written = self._rows_written
        result.metrics.update(self._metrics)
        return result
