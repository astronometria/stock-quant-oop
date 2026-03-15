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

        self._rows_read = 0
        self._rows_written = 0
        self._metrics: dict[str, int] = {}

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
        raw_count = int(
            self.con.execute("SELECT COUNT(*) FROM news_articles_raw").fetchone()[0]
        )
        if raw_count == 0:
            raise PipelineError("no rows available in news_articles_raw")

    def load(self, data) -> None:
        con = self.con

        for table_name in [
            "news_features_daily",
            "news_llm_enrichment",
            "news_event_windows",
            "news_clusters",
            "news_symbol_links",
            "news_articles_normalized",
        ]:
            con.execute(f"DELETE FROM {table_name}")

        for temp_name in [
            "tmp_news_raw",
            "tmp_news_candidate_links",
            "tmp_news_effective",
            "tmp_price_calendar",
            "tmp_news_effective_windows",
        ]:
            con.execute(f"DROP TABLE IF EXISTS {temp_name}")

        con.execute(
            """
            CREATE TEMP TABLE tmp_news_raw AS
            SELECT
                r.raw_id,
                r.published_at,
                r.published_at AS available_at,
                r.source_name,
                r.title,
                r.article_url,
                sha1(
                    concat_ws(
                        '||',
                        coalesce(CAST(r.raw_id AS VARCHAR), ''),
                        coalesce(CAST(r.published_at AS VARCHAR), ''),
                        coalesce(r.source_name, ''),
                        coalesce(r.title, ''),
                        coalesce(r.article_url, '')
                    )
                ) AS article_hash,
                'ARTICLE:' || substr(
                    sha1(
                        concat_ws(
                            '||',
                            coalesce(CAST(r.raw_id AS VARCHAR), ''),
                            coalesce(CAST(r.published_at AS VARCHAR), ''),
                            coalesce(r.source_name, ''),
                            coalesce(r.title, ''),
                            coalesce(r.article_url, '')
                        )
                    ),
                    1,
                    20
                ) AS article_id,
                'CLUSTER:' || substr(
                    sha1(
                        concat_ws(
                            '||',
                            coalesce(r.source_name, ''),
                            coalesce(r.title, ''),
                            coalesce(r.article_url, '')
                        )
                    ),
                    1,
                    16
                ) AS cluster_id
            FROM news_articles_raw r
            """
        )

        con.execute(
            """
            CREATE TEMP TABLE tmp_news_candidate_links AS
            SELECT
                nr.raw_id,
                nr.article_id,
                nr.cluster_id,
                nr.published_at,
                nr.available_at,
                nr.source_name,
                nr.title,
                nr.article_url,
                nr.article_hash,
                im.instrument_id,
                im.company_id,
                UPPER(TRIM(c.symbol)) AS symbol,
                COALESCE(c.match_score, 1.0) AS link_confidence,
                COALESCE(c.match_type, 'symbol_candidate') AS linking_method
            FROM tmp_news_raw nr
            JOIN news_symbol_candidates c
              ON nr.raw_id = c.raw_id
            JOIN instrument_master im
              ON UPPER(TRIM(im.symbol)) = UPPER(TRIM(c.symbol))
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
                CURRENT_TIMESTAMP
            FROM tmp_news_raw
            """
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
                CURRENT_TIMESTAMP
            FROM tmp_news_raw
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
                CURRENT_TIMESTAMP
            FROM tmp_news_candidate_links
            """
        )

        con.execute(
            """
            CREATE TEMP TABLE tmp_price_calendar AS
            SELECT DISTINCT
                instrument_id,
                bar_date
            FROM price_bars_adjusted
            """
        )

        con.execute(
            """
            CREATE TEMP TABLE tmp_news_effective AS
            SELECT
                l.article_id,
                l.cluster_id,
                l.instrument_id,
                l.company_id,
                l.symbol,
                l.link_confidence,
                l.linking_method,
                l.published_at,
                l.available_at,
                CASE
                    WHEN CAST(l.available_at AS TIME) < TIME '09:30:00' THEN (
                        SELECT MIN(pc.bar_date)
                        FROM tmp_price_calendar pc
                        WHERE pc.instrument_id = l.instrument_id
                          AND pc.bar_date >= CAST(l.available_at AS DATE)
                    )
                    ELSE (
                        SELECT MIN(pc.bar_date)
                        FROM tmp_price_calendar pc
                        WHERE pc.instrument_id = l.instrument_id
                          AND pc.bar_date > CAST(l.available_at AS DATE)
                    )
                END AS effective_as_of_date
            FROM tmp_news_candidate_links l
            """
        )

        con.execute(
            """
            CREATE TEMP TABLE tmp_news_effective_windows AS
            SELECT
                e.article_id,
                e.instrument_id,
                e.symbol,
                CAST(e.published_at AS DATE) AS event_date,
                e.effective_as_of_date AS window_start_date,
                (
                    SELECT MIN(pc.bar_date)
                    FROM tmp_price_calendar pc
                    WHERE pc.instrument_id = e.instrument_id
                      AND pc.bar_date > e.effective_as_of_date
                ) AS window_end_date
            FROM tmp_news_effective e
            WHERE e.effective_as_of_date IS NOT NULL
            """
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
                article_id,
                instrument_id,
                symbol,
                event_date,
                window_start_date,
                window_end_date,
                CURRENT_TIMESTAMP
            FROM tmp_news_effective_windows
            """
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
                CURRENT_TIMESTAMP
            FROM tmp_news_raw
            """
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
                e.instrument_id,
                e.company_id,
                e.symbol,
                e.effective_as_of_date AS as_of_date,
                COUNT(*) AS article_count_1d,
                COUNT(DISTINCT e.cluster_id) AS unique_cluster_count_1d,
                AVG(e.link_confidence) AS avg_link_confidence,
                'news' AS source_name,
                CURRENT_TIMESTAMP AS created_at
            FROM tmp_news_effective e
            WHERE e.effective_as_of_date IS NOT NULL
            GROUP BY
                e.instrument_id,
                e.company_id,
                e.symbol,
                e.effective_as_of_date
            """
        )

        self._rows_read = int(
            con.execute("SELECT COUNT(*) FROM news_articles_raw").fetchone()[0]
        )

        normalized_articles = int(
            con.execute("SELECT COUNT(*) FROM news_articles_normalized").fetchone()[0]
        )
        news_symbol_links = int(
            con.execute("SELECT COUNT(*) FROM news_symbol_links").fetchone()[0]
        )
        news_event_windows = int(
            con.execute("SELECT COUNT(*) FROM news_event_windows").fetchone()[0]
        )
        news_llm_enrichment_rows = int(
            con.execute("SELECT COUNT(*) FROM news_llm_enrichment").fetchone()[0]
        )
        news_feature_rows = int(
            con.execute("SELECT COUNT(*) FROM news_features_daily").fetchone()[0]
        )
        symbol_candidates = int(
            con.execute("SELECT COUNT(*) FROM news_symbol_candidates").fetchone()[0]
        )

        self._rows_written = (
            normalized_articles
            + news_symbol_links
            + news_event_windows
            + news_llm_enrichment_rows
            + news_feature_rows
        )

        self._metrics = {
            "raw_articles": self._rows_read,
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
        result.rows_read = self._rows_read
        result.rows_written = self._rows_written
        result.metrics.update(self._metrics)
        return result
