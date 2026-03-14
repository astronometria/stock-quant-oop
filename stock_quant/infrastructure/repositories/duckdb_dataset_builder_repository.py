from __future__ import annotations

from datetime import datetime
from typing import Any

from stock_quant.domain.entities.research_dataset import ResearchDatasetDaily
from stock_quant.infrastructure.db.unit_of_work import DuckDbUnitOfWork
from stock_quant.research.datasets.dataset_version import DatasetVersion
from stock_quant.shared.exceptions import RepositoryError


class DuckDbDatasetBuilderRepository:
    def __init__(self, uow: DuckDbUnitOfWork) -> None:
        self.uow = uow

    @property
    def con(self):
        if self.uow.connection is None:
            raise RepositoryError("active DB connection is required")
        return self.uow.connection

    def load_research_features_rows(self) -> list[dict[str, Any]]:
        try:
            rows = self.con.execute(
                """
                SELECT
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
                    avg_link_confidence
                FROM research_features_daily
                ORDER BY symbol, as_of_date
                """
            ).fetchall()

            return [
                {
                    "instrument_id": row[0],
                    "company_id": row[1],
                    "symbol": row[2],
                    "as_of_date": row[3],
                    "close_to_sma_20": row[4],
                    "rsi_14": row[5],
                    "revenue": row[6],
                    "net_income": row[7],
                    "net_margin": row[8],
                    "debt_to_equity": row[9],
                    "return_on_assets": row[10],
                    "short_interest": row[11],
                    "days_to_cover": row[12],
                    "short_volume_ratio": row[13],
                    "article_count_1d": row[14],
                    "unique_cluster_count_1d": row[15],
                    "avg_link_confidence": row[16],
                }
                for row in rows
            ]
        except Exception as exc:
            raise RepositoryError(f"failed to load research_features_daily rows: {exc}") from exc

    def load_return_labels_rows(self) -> list[dict[str, Any]]:
        try:
            rows = self.con.execute(
                """
                SELECT
                    instrument_id,
                    as_of_date,
                    fwd_return_1d,
                    fwd_return_5d,
                    fwd_return_20d,
                    direction_1d,
                    direction_5d,
                    direction_20d
                FROM return_labels_daily
                ORDER BY symbol, as_of_date
                """
            ).fetchall()

            return [
                {
                    "instrument_id": row[0],
                    "as_of_date": row[1],
                    "fwd_return_1d": row[2],
                    "fwd_return_5d": row[3],
                    "fwd_return_20d": row[4],
                    "direction_1d": row[5],
                    "direction_5d": row[6],
                    "direction_20d": row[7],
                }
                for row in rows
            ]
        except Exception as exc:
            raise RepositoryError(f"failed to load return_labels_daily rows: {exc}") from exc

    def load_volatility_labels_rows(self) -> list[dict[str, Any]]:
        try:
            rows = self.con.execute(
                """
                SELECT
                    instrument_id,
                    as_of_date,
                    realized_vol_20d
                FROM volatility_labels_daily
                ORDER BY symbol, as_of_date
                """
            ).fetchall()

            return [
                {
                    "instrument_id": row[0],
                    "as_of_date": row[1],
                    "realized_vol_20d": row[2],
                }
                for row in rows
            ]
        except Exception as exc:
            raise RepositoryError(f"failed to load volatility_labels_daily rows: {exc}") from exc

    def replace_research_dataset_daily(self, rows: list[ResearchDatasetDaily]) -> int:
        try:
            self.con.execute("DELETE FROM research_dataset_daily")
            if not rows:
                return 0

            payload = [
                (
                    row.dataset_name,
                    row.dataset_version,
                    row.instrument_id,
                    row.company_id,
                    row.symbol,
                    row.as_of_date,
                    row.close_to_sma_20,
                    row.rsi_14,
                    row.revenue,
                    row.net_income,
                    row.net_margin,
                    row.debt_to_equity,
                    row.return_on_assets,
                    row.short_interest,
                    row.days_to_cover,
                    row.short_volume_ratio,
                    row.article_count_1d,
                    row.unique_cluster_count_1d,
                    row.avg_link_confidence,
                    row.fwd_return_1d,
                    row.fwd_return_5d,
                    row.fwd_return_20d,
                    row.direction_1d,
                    row.direction_5d,
                    row.direction_20d,
                    row.realized_vol_20d,
                    row.created_at or datetime.utcnow(),
                )
                for row in rows
            ]
            self.con.executemany(
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
                    created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                payload,
            )
            return len(payload)
        except Exception as exc:
            raise RepositoryError(f"failed to replace research_dataset_daily: {exc}") from exc

    def insert_dataset_version(self, row: DatasetVersion) -> int:
        try:
            payload = (
                row.dataset_name,
                row.dataset_version,
                row.universe_name,
                row.as_of_date,
                row.feature_run_id,
                row.label_run_id,
                row.row_count,
                row.config_json,
                row.created_at or datetime.utcnow(),
            )
            self.con.execute(
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
                payload,
            )
            return 1
        except Exception as exc:
            raise RepositoryError(f"failed to insert dataset_versions row: {exc}") from exc
