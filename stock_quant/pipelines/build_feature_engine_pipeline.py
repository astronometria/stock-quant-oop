from __future__ import annotations

from stock_quant.app.dto.pipeline_result import PipelineResult
from stock_quant.infrastructure.db.unit_of_work import DuckDbUnitOfWork
from stock_quant.pipelines.base_pipeline import BasePipeline
from stock_quant.shared.exceptions import PipelineError


class BuildFeatureEnginePipeline(BasePipeline):
    pipeline_name = "build_feature_engine"

    def __init__(self, repository=None, uow: DuckDbUnitOfWork | None = None) -> None:
        if repository is not None:
            self.uow = repository.uow
        else:
            self.uow = uow

        if self.uow is None:
            raise ValueError("BuildFeatureEnginePipeline requires repository or uow")

        self._input_price_rows = 0
        self._technical_rows = 0
        self._research_rows = 0
        self._fundamental_pit_matches = 0
        self._short_interest_pit_matches = 0
        self._short_volume_day_matches = 0
        self._news_day_matches = 0
        self._research_universe_rows = 0

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
        adjusted_count = int(
            self.con.execute("SELECT COUNT(*) FROM price_bars_adjusted").fetchone()[0]
        )
        if adjusted_count == 0:
            raise PipelineError("no rows available in price_bars_adjusted")

        included_universe_count = int(
            self.con.execute(
                """
                SELECT COUNT(*)
                FROM research_universe
                WHERE include_in_research_universe = TRUE
                """
            ).fetchone()[0]
        )
        if included_universe_count == 0:
            raise PipelineError("no included rows available in research_universe")

    def _rebuild_research_features_schema_if_needed(self) -> None:
        info = self.con.execute("PRAGMA table_info('research_features_daily')").fetchall()
        cols = [row[1] for row in info]
        if "short_interest_pct_volume" not in cols:
            return

        self.con.execute("DROP TABLE IF EXISTS research_features_daily__new")
        self.con.execute(
            """
            CREATE TABLE research_features_daily__new (
                instrument_id VARCHAR,
                company_id VARCHAR,
                symbol VARCHAR,
                as_of_date DATE,
                close_to_sma_20 DOUBLE,
                rsi_14 DOUBLE,
                revenue DOUBLE,
                net_income DOUBLE,
                net_margin DOUBLE,
                debt_to_equity DOUBLE,
                return_on_assets DOUBLE,
                short_interest DOUBLE,
                days_to_cover DOUBLE,
                short_volume_ratio DOUBLE,
                short_interest_change_pct DOUBLE,
                short_squeeze_score DOUBLE,
                short_pressure_zscore DOUBLE,
                days_to_cover_zscore DOUBLE,
                article_count_1d BIGINT,
                unique_cluster_count_1d BIGINT,
                avg_link_confidence DOUBLE,
                source_name VARCHAR,
                created_at TIMESTAMP
            )
            """
        )
        self.con.execute("DROP TABLE research_features_daily")
        self.con.execute("ALTER TABLE research_features_daily__new RENAME TO research_features_daily")

    def load(self, data) -> None:
        con = self.con
        self._rebuild_research_features_schema_if_needed()

        existing_columns = {
            row[1]
            for row in con.execute("PRAGMA table_info('research_features_daily')").fetchall()
        }
        for column_name, sql in [
            ("short_interest_change_pct", "ALTER TABLE research_features_daily ADD COLUMN short_interest_change_pct DOUBLE"),
            ("short_squeeze_score", "ALTER TABLE research_features_daily ADD COLUMN short_squeeze_score DOUBLE"),
            ("short_pressure_zscore", "ALTER TABLE research_features_daily ADD COLUMN short_pressure_zscore DOUBLE"),
            ("days_to_cover_zscore", "ALTER TABLE research_features_daily ADD COLUMN days_to_cover_zscore DOUBLE"),
        ]:
            if column_name not in existing_columns:
                con.execute(sql)

        con.execute("DELETE FROM technical_features_daily")
        con.execute("DELETE FROM research_features_daily")

        for table_name in [
            "tmp_research_universe_symbols",
            "tmp_price_base",
            "tmp_price_calendar",
            "tmp_technical_features",
            "tmp_fundamental_snapshot_meta",
            "tmp_fundamental_feature_source",
            "tmp_fundamental_feature_effective",
        ]:
            con.execute(f"DROP TABLE IF EXISTS {table_name}")

        con.execute(
            """
            CREATE TEMP TABLE tmp_research_universe_symbols AS
            SELECT DISTINCT symbol
            FROM research_universe
            WHERE include_in_research_universe = TRUE
            """
        )

        self._research_universe_rows = int(
            con.execute("SELECT COUNT(*) FROM tmp_research_universe_symbols").fetchone()[0]
        )

        con.execute(
            """
            CREATE TEMP TABLE tmp_price_base AS
            SELECT
                p.instrument_id,
                im.company_id,
                p.symbol,
                p.bar_date AS as_of_date,
                CAST(p.adj_close AS DOUBLE) AS adj_close
            FROM price_bars_adjusted p
            INNER JOIN tmp_research_universe_symbols ru
                ON UPPER(TRIM(p.symbol)) = UPPER(TRIM(ru.symbol))
            LEFT JOIN instrument_master im
                ON p.instrument_id = im.instrument_id
            ORDER BY p.instrument_id, p.bar_date
            """
        )

        self._input_price_rows = int(
            con.execute("SELECT COUNT(*) FROM tmp_price_base").fetchone()[0]
        )

        con.execute(
            """
            CREATE TEMP TABLE tmp_technical_features AS
            WITH base AS (
                SELECT
                    instrument_id,
                    company_id,
                    symbol,
                    as_of_date,
                    adj_close,
                    AVG(adj_close) OVER (
                        PARTITION BY instrument_id
                        ORDER BY as_of_date
                        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                    ) AS sma_20,
                    adj_close - LAG(adj_close) OVER (
                        PARTITION BY instrument_id
                        ORDER BY as_of_date
                    ) AS delta
                FROM tmp_price_base
            ),
            gains_losses AS (
                SELECT
                    instrument_id,
                    company_id,
                    symbol,
                    as_of_date,
                    adj_close,
                    sma_20,
                    CASE WHEN delta > 0 THEN delta ELSE 0 END AS gain,
                    CASE WHEN delta < 0 THEN -delta ELSE 0 END AS loss
                FROM base
            ),
            rsi_roll AS (
                SELECT
                    instrument_id,
                    company_id,
                    symbol,
                    as_of_date,
                    adj_close,
                    sma_20,
                    AVG(gain) OVER (
                        PARTITION BY instrument_id
                        ORDER BY as_of_date
                        ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
                    ) AS avg_gain_14,
                    AVG(loss) OVER (
                        PARTITION BY instrument_id
                        ORDER BY as_of_date
                        ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
                    ) AS avg_loss_14
                FROM gains_losses
            )
            SELECT
                instrument_id,
                company_id,
                symbol,
                as_of_date,
                CASE
                    WHEN sma_20 IS NULL OR sma_20 = 0 THEN NULL
                    ELSE (adj_close / sma_20) - 1
                END AS close_to_sma_20,
                CASE
                    WHEN avg_loss_14 IS NULL OR avg_gain_14 IS NULL THEN NULL
                    WHEN avg_loss_14 = 0 AND avg_gain_14 = 0 THEN 50.0
                    WHEN avg_loss_14 = 0 THEN 100.0
                    ELSE 100 - (100 / (1 + (avg_gain_14 / avg_loss_14)))
                END AS rsi_14
            FROM rsi_roll
            """
        )

        con.execute(
            """
            INSERT INTO technical_features_daily (
                instrument_id,
                company_id,
                symbol,
                as_of_date,
                close_to_sma_20,
                rsi_14,
                source_name,
                created_at
            )
            SELECT
                instrument_id,
                company_id,
                symbol,
                as_of_date,
                close_to_sma_20,
                rsi_14,
                'prices',
                CURRENT_TIMESTAMP
            FROM tmp_technical_features
            """
        )

        self._technical_rows = int(
            con.execute("SELECT COUNT(*) FROM technical_features_daily").fetchone()[0]
        )

        con.execute(
            """
            CREATE TEMP TABLE tmp_fundamental_snapshot_meta AS
            SELECT company_id, period_type, period_end_date AS as_of_date, available_at
            FROM fundamental_snapshot_quarterly
            UNION ALL
            SELECT company_id, period_type, period_end_date AS as_of_date, available_at
            FROM fundamental_snapshot_annual
            UNION ALL
            SELECT company_id, period_type, period_end_date AS as_of_date, available_at
            FROM fundamental_ttm
            """
        )

        con.execute(
            """
            CREATE TEMP TABLE tmp_fundamental_feature_source AS
            SELECT
                ff.company_id,
                ff.as_of_date AS period_end_date,
                m.available_at,
                ff.revenue,
                ff.net_income,
                ff.net_margin,
                ff.debt_to_equity,
                ff.return_on_assets
            FROM fundamental_features_daily ff
            LEFT JOIN tmp_fundamental_snapshot_meta m
                ON ff.company_id = m.company_id
               AND ff.as_of_date = m.as_of_date
            """
        )

        con.execute(
            """
            CREATE TEMP TABLE tmp_fundamental_feature_effective AS
            SELECT
                company_id,
                period_end_date,
                COALESCE(available_at, period_end_date) AS available_at,
                COALESCE(available_at, period_end_date) AS effective_as_of_date,
                revenue,
                net_income,
                net_margin,
                debt_to_equity,
                return_on_assets
            FROM tmp_fundamental_feature_source
            """
        )

        con.execute(
            """
            INSERT INTO research_features_daily (
                instrument_id,
                company_id,
                symbol,
                as_of_date,
                close_to_sma_20,
                rsi_14,
                revenue,
                net_income,
                net_margin,
                debt_to_equity,
                return_on_assets,
                short_interest,
                days_to_cover,
                short_volume_ratio,
                short_interest_change_pct,
                short_squeeze_score,
                short_pressure_zscore,
                days_to_cover_zscore,
                article_count_1d,
                unique_cluster_count_1d,
                avg_link_confidence,
                source_name,
                created_at
            )
            SELECT
                t.instrument_id,
                t.company_id,
                t.symbol,
                t.as_of_date,
                t.close_to_sma_20,
                t.rsi_14,
                f.revenue,
                f.net_income,
                f.net_margin,
                f.debt_to_equity,
                f.return_on_assets,
                s.short_interest,
                s.days_to_cover,
                s.short_volume_ratio,
                s.short_interest_change_pct,
                s.short_squeeze_score,
                s.short_pressure_zscore,
                s.days_to_cover_zscore,
                n.article_count_1d,
                n.unique_cluster_count_1d,
                n.avg_link_confidence,
                'research',
                CURRENT_TIMESTAMP
            FROM technical_features_daily t
            LEFT JOIN LATERAL (
                SELECT
                    fe.revenue,
                    fe.net_income,
                    fe.net_margin,
                    fe.debt_to_equity,
                    fe.return_on_assets
                FROM tmp_fundamental_feature_effective fe
                WHERE fe.company_id = t.company_id
                  AND fe.effective_as_of_date IS NOT NULL
                  AND fe.effective_as_of_date <= t.as_of_date
                ORDER BY
                    fe.effective_as_of_date DESC,
                    fe.available_at DESC,
                    fe.period_end_date DESC
                LIMIT 1
            ) f ON TRUE
            LEFT JOIN short_features_daily s
                ON t.instrument_id = s.instrument_id
               AND t.as_of_date = s.as_of_date
            LEFT JOIN news_features_daily n
                ON t.instrument_id = n.instrument_id
               AND t.as_of_date = n.as_of_date
            """
        )

        self._research_rows = int(
            con.execute("SELECT COUNT(*) FROM research_features_daily").fetchone()[0]
        )

        self._fundamental_pit_matches = int(
            con.execute(
                """
                SELECT COUNT(*)
                FROM research_features_daily
                WHERE revenue IS NOT NULL
                   OR net_income IS NOT NULL
                   OR net_margin IS NOT NULL
                   OR debt_to_equity IS NOT NULL
                   OR return_on_assets IS NOT NULL
                """
            ).fetchone()[0]
        )

        self._short_interest_pit_matches = int(
            con.execute(
                """
                SELECT COUNT(*)
                FROM research_features_daily
                WHERE short_interest IS NOT NULL
                """
            ).fetchone()[0]
        )

        self._short_volume_day_matches = int(
            con.execute(
                """
                SELECT COUNT(*)
                FROM research_features_daily
                WHERE short_volume_ratio IS NOT NULL
                """
            ).fetchone()[0]
        )

        self._news_day_matches = int(
            con.execute(
                """
                SELECT COUNT(*)
                FROM research_features_daily
                WHERE article_count_1d IS NOT NULL
                """
            ).fetchone()[0]
        )

    def finalize(self, result: PipelineResult) -> PipelineResult:
        result.rows_read = self._input_price_rows
        result.rows_written = self._technical_rows + self._research_rows
        result.metrics["research_universe_rows"] = self._research_universe_rows
        result.metrics["input_price_rows"] = self._input_price_rows
        result.metrics["technical_feature_rows"] = self._technical_rows
        result.metrics["research_feature_rows"] = self._research_rows
        result.metrics["fundamental_pit_matches"] = self._fundamental_pit_matches
        result.metrics["short_interest_pit_matches"] = self._short_interest_pit_matches
        result.metrics["short_volume_day_matches"] = self._short_volume_day_matches
        result.metrics["news_day_matches"] = self._news_day_matches
        result.metrics["written_technical"] = self._technical_rows
        result.metrics["written_research"] = self._research_rows
        return result
