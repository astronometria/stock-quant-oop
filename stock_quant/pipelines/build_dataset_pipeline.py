from __future__ import annotations

from datetime import datetime
import json

from stock_quant.app.dto.pipeline_result import PipelineResult
from stock_quant.app.services.dataset_version_service import DatasetVersionService
from stock_quant.infrastructure.db.unit_of_work import DuckDbUnitOfWork
# REMOVED: BasePipeline supprimé pendant la consolidation brutale
from stock_quant.shared.exceptions import PipelineError


class BuildDatasetBuilderPipeline(BasePipeline):
    pipeline_name = "build_dataset_builder"

    def __init__(
        self,
        repository=None,
        uow: DuckDbUnitOfWork | None = None,
        dataset_name: str = "research_dataset_v1",
        dataset_version: str = "v1",
    ) -> None:
        if repository is not None:
            self.uow = repository.uow
            self.repository = repository
        else:
            self.uow = uow
            self.repository = None

        if self.uow is None:
            raise ValueError("BuildDatasetBuilderPipeline requires repository or uow")

        self.dataset_name = dataset_name
        self.dataset_version = dataset_version
        self.version_service = DatasetVersionService()

        self._rows_read = 0
        self._dataset_rows = 0
        self._version_rows = 0
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

    def _table_columns(self, table_name: str) -> set[str]:
        rows = self.con.execute(f"PRAGMA table_info('{table_name}')").fetchall()
        return {str(row[1]).strip() for row in rows}

    def _has_table(self, table_name: str) -> bool:
        row = self.con.execute(
            """
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_name = ?
            """,
            [table_name],
        ).fetchone()
        return bool(row and row[0])

    def _history_universe_name_expr(self) -> str:
        return """
            CASE
                WHEN LOWER(TRIM(universe_name)) IN ('research', 'research_universe')
                    THEN 1
                ELSE 0
            END = 1
        """

    def _history_membership_active_expr(self) -> str:
        columns = self._table_columns("universe_membership_history")

        if "membership_status" in columns:
            return """
                UPPER(TRIM(COALESCE(membership_status, ''))) IN ('ACTIVE', 'INCLUDED')
            """
        if "is_active" in columns:
            return "COALESCE(is_active, FALSE) = TRUE"
        if "include_in_universe" in columns:
            return "COALESCE(include_in_universe, FALSE) = TRUE"

        raise PipelineError(
            "universe_membership_history does not expose membership_status/is_active/include_in_universe"
        )

    def _ensure_dataset_schema(self) -> None:
        info = self.con.execute("PRAGMA table_info('research_dataset_daily')").fetchall()
        cols = [row[1] for row in info]

        if "short_interest_pct_volume" in cols:
            self.con.execute("DROP TABLE IF EXISTS research_dataset_daily__new")
            self.con.execute(
                """
                CREATE TABLE research_dataset_daily__new (
                    dataset_name VARCHAR,
                    dataset_version VARCHAR,
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
                    article_count_1d BIGINT,
                    unique_cluster_count_1d BIGINT,
                    avg_link_confidence DOUBLE,
                    fwd_return_1d DOUBLE,
                    fwd_return_5d DOUBLE,
                    fwd_return_20d DOUBLE,
                    direction_1d INTEGER,
                    direction_5d INTEGER,
                    direction_20d INTEGER,
                    realized_vol_20d DOUBLE,
                    created_at TIMESTAMP,
                    short_squeeze_score DOUBLE,
                    short_pressure_zscore DOUBLE,
                    days_to_cover_zscore DOUBLE,
                    short_interest_change_pct DOUBLE
                )
                """
            )
            self.con.execute("DROP TABLE research_dataset_daily")
            self.con.execute("ALTER TABLE research_dataset_daily__new RENAME TO research_dataset_daily")

        existing_columns = {
            row[1]
            for row in self.con.execute("PRAGMA table_info('research_dataset_daily')").fetchall()
        }
        for column_name, sql in [
            (
                "short_interest_change_pct",
                "ALTER TABLE research_dataset_daily ADD COLUMN short_interest_change_pct DOUBLE",
            ),
            (
                "short_squeeze_score",
                "ALTER TABLE research_dataset_daily ADD COLUMN short_squeeze_score DOUBLE",
            ),
            (
                "short_pressure_zscore",
                "ALTER TABLE research_dataset_daily ADD COLUMN short_pressure_zscore DOUBLE",
            ),
            (
                "days_to_cover_zscore",
                "ALTER TABLE research_dataset_daily ADD COLUMN days_to_cover_zscore DOUBLE",
            ),
        ]:
            if column_name not in existing_columns:
                self.con.execute(sql)

    def validate(self, data) -> None:
        if not self._has_table("universe_membership_history"):
            raise PipelineError("universe_membership_history table is required for PIT dataset build")

        active_expr = self._history_membership_active_expr()
        universe_expr = self._history_universe_name_expr()

        included_history_count = int(
            self.con.execute(
                f"""
                SELECT COUNT(*)
                FROM universe_membership_history
                WHERE {universe_expr}
                  AND {active_expr}
                """
            ).fetchone()[0]
        )
        if included_history_count == 0:
            raise PipelineError(
                "no active rows available in universe_membership_history for research universe"
            )

        self._rows_read = int(
            self.con.execute(
                f"""
                SELECT COUNT(*)
                FROM research_features_daily f
                INNER JOIN universe_membership_history u
                    ON u.instrument_id = f.instrument_id
                   AND {universe_expr.replace("universe_name", "u.universe_name")}
                   AND {active_expr.replace("membership_status", "u.membership_status")
                                   .replace("is_active", "u.is_active")
                                   .replace("include_in_universe", "u.include_in_universe")}
                   AND u.effective_from <= f.as_of_date
                   AND COALESCE(u.effective_to, DATE '9999-12-31') > f.as_of_date
                """
            ).fetchone()[0]
        )
        if self._rows_read == 0:
            raise PipelineError(
                "no PIT-filtered rows available in research_features_daily from universe_membership_history"
            )

    def load(self, data) -> None:
        con = self.con
        self._ensure_dataset_schema()

        active_expr = self._history_membership_active_expr()
        universe_expr = self._history_universe_name_expr()

        active_expr_u = (
            active_expr.replace("membership_status", "u.membership_status")
            .replace("is_active", "u.is_active")
            .replace("include_in_universe", "u.include_in_universe")
        )
        universe_expr_u = universe_expr.replace("universe_name", "u.universe_name")

        con.execute(
            """
            DELETE FROM research_dataset_daily
            WHERE dataset_name = ? AND dataset_version = ?
            """,
            [self.dataset_name, self.dataset_version],
        )

        con.execute(
            """
            DELETE FROM dataset_versions
            WHERE dataset_name = ? AND dataset_version = ?
            """,
            [self.dataset_name, self.dataset_version],
        )

        con.execute("DROP TABLE IF EXISTS tmp_research_universe_membership")
        con.execute("DROP TABLE IF EXISTS tmp_dataset_joined")

        con.execute(
            f"""
            CREATE TEMP TABLE tmp_research_universe_membership AS
            SELECT
                instrument_id,
                company_id,
                symbol,
                effective_from,
                COALESCE(effective_to, DATE '9999-12-31') AS effective_to
            FROM universe_membership_history
            WHERE {universe_expr}
              AND {active_expr}
            """
        )

        self._research_universe_rows = int(
            con.execute("SELECT COUNT(*) FROM tmp_research_universe_membership").fetchone()[0]
        )

        con.execute(
            f"""
            CREATE TEMP TABLE tmp_dataset_joined AS
            SELECT
                ? AS dataset_name,
                ? AS dataset_version,
                f.instrument_id,
                f.company_id,
                f.symbol,
                f.as_of_date,
                f.close_to_sma_20,
                f.rsi_14,
                f.revenue,
                f.net_income,
                f.net_margin,
                f.debt_to_equity,
                f.return_on_assets,
                f.short_interest,
                f.days_to_cover,
                f.short_volume_ratio,
                f.article_count_1d,
                f.unique_cluster_count_1d,
                f.avg_link_confidence,
                r.fwd_return_1d,
                r.fwd_return_5d,
                r.fwd_return_20d,
                r.direction_1d,
                r.direction_5d,
                r.direction_20d,
                v.realized_vol_20d,
                CURRENT_TIMESTAMP AS created_at,
                f.short_squeeze_score,
                f.short_pressure_zscore,
                f.days_to_cover_zscore,
                f.short_interest_change_pct
            FROM research_features_daily f
            INNER JOIN universe_membership_history u
                ON u.instrument_id = f.instrument_id
               AND {universe_expr_u}
               AND {active_expr_u}
               AND u.effective_from <= f.as_of_date
               AND COALESCE(u.effective_to, DATE '9999-12-31') > f.as_of_date
            LEFT JOIN return_labels_daily r
                ON f.instrument_id = r.instrument_id
               AND f.as_of_date = r.as_of_date
            LEFT JOIN volatility_labels_daily v
                ON f.instrument_id = v.instrument_id
               AND f.as_of_date = v.as_of_date
            """,
            [self.dataset_name, self.dataset_version],
        )

        con.execute(
            """
            INSERT INTO research_dataset_daily (
                dataset_name,
                dataset_version,
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
                article_count_1d,
                unique_cluster_count_1d,
                avg_link_confidence,
                fwd_return_1d,
                fwd_return_5d,
                fwd_return_20d,
                direction_1d,
                direction_5d,
                direction_20d,
                realized_vol_20d,
                created_at,
                short_squeeze_score,
                short_pressure_zscore,
                days_to_cover_zscore,
                short_interest_change_pct
            )
            SELECT
                dataset_name,
                dataset_version,
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
                article_count_1d,
                unique_cluster_count_1d,
                avg_link_confidence,
                fwd_return_1d,
                fwd_return_5d,
                fwd_return_20d,
                direction_1d,
                direction_5d,
                direction_20d,
                realized_vol_20d,
                created_at,
                short_squeeze_score,
                short_pressure_zscore,
                days_to_cover_zscore,
                short_interest_change_pct
            FROM tmp_dataset_joined
            """
        )

        self._dataset_rows = int(
            con.execute(
                """
                SELECT COUNT(*)
                FROM research_dataset_daily
                WHERE dataset_name = ? AND dataset_version = ?
                """,
                [self.dataset_name, self.dataset_version],
            ).fetchone()[0]
        )

        max_as_of = con.execute(
            """
            SELECT MAX(as_of_date)
            FROM research_dataset_daily
            WHERE dataset_name = ? AND dataset_version = ?
            """,
            [self.dataset_name, self.dataset_version],
        ).fetchone()[0]

        if max_as_of is None:
            raise PipelineError("dataset build produced zero rows")

        config_payload = json.dumps(
            {
                "filter_source": "universe_membership_history",
                "survivor_bias_aware": True,
                "dataset_pipeline": self.pipeline_name,
                "dataset_name": self.dataset_name,
                "dataset_version": self.dataset_version,
            },
            sort_keys=True,
        )

        version_row = self.version_service.build_dataset_version(
            dataset_name=self.dataset_name,
            dataset_version=self.dataset_version,
            universe_name="research_universe",
            as_of_date=max_as_of,
            row_count=self._dataset_rows,
            feature_run_id=None,
            label_run_id=None,
            config_json=config_payload,
            created_at=datetime.utcnow(),
        )

        con.execute(
            """
            INSERT INTO dataset_versions (
                dataset_name,
                dataset_version,
                universe_name,
                as_of_date,
                feature_run_id,
                label_run_id,
                row_count,
                config_json,
                created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                version_row.dataset_name,
                version_row.dataset_version,
                version_row.universe_name,
                version_row.as_of_date,
                version_row.feature_run_id,
                version_row.label_run_id,
                version_row.row_count,
                version_row.config_json,
                version_row.created_at,
            ),
        )

        self._version_rows = int(
            con.execute(
                """
                SELECT COUNT(*)
                FROM dataset_versions
                WHERE dataset_name = ? AND dataset_version = ?
                """,
                [self.dataset_name, self.dataset_version],
            ).fetchone()[0]
        )

    def finalize(self, result: PipelineResult) -> PipelineResult:
        result.rows_read = self._rows_read
        result.rows_written = self._dataset_rows
        result.metrics["dataset_rows"] = self._dataset_rows
        result.metrics["dataset_version_rows"] = self._version_rows
        result.metrics["research_universe_rows"] = self._research_universe_rows
        result.metrics["pit_filter_source"] = "universe_membership_history"
        result.metrics["survivor_bias_aware"] = 1
        return result
