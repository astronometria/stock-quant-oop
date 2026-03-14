from __future__ import annotations

from datetime import datetime
from typing import Any

from stock_quant.domain.entities.research_feature import ResearchFeatureDaily, TechnicalFeatureDaily
from stock_quant.infrastructure.db.unit_of_work import DuckDbUnitOfWork
from stock_quant.shared.exceptions import RepositoryError


class DuckDbFeatureEngineRepository:
    def __init__(self, uow: DuckDbUnitOfWork) -> None:
        self.uow = uow

    @property
    def con(self):
        if self.uow.connection is None:
            raise RepositoryError("active DB connection is required")
        return self.uow.connection

    def load_price_bars_adjusted_rows(self) -> list[dict[str, Any]]:
        try:
            rows = self.con.execute(
                """
                SELECT instrument_id, symbol, bar_date, adj_close
                FROM price_bars_adjusted
                ORDER BY instrument_id, bar_date
                """
            ).fetchall()
            return [
                {
                    "instrument_id": row[0],
                    "symbol": row[1],
                    "bar_date": row[2],
                    "adj_close": row[3],
                }
                for row in rows
            ]
        except Exception as exc:
            raise RepositoryError(f"failed to load price_bars_adjusted rows: {exc}") from exc

    def load_fundamental_features_rows(self) -> list[dict[str, Any]]:
        try:
            rows = self.con.execute(
                """
                SELECT
                    company_id,
                    as_of_date,
                    revenue,
                    net_income,
                    net_margin,
                    debt_to_equity,
                    return_on_assets
                FROM fundamental_features_daily
                ORDER BY company_id, as_of_date
                """
            ).fetchall()
            return [
                {
                    "company_id": row[0],
                    "as_of_date": row[1],
                    "revenue": row[2],
                    "net_income": row[3],
                    "net_margin": row[4],
                    "debt_to_equity": row[5],
                    "return_on_assets": row[6],
                }
                for row in rows
            ]
        except Exception as exc:
            raise RepositoryError(f"failed to load fundamental_features_daily rows: {exc}") from exc

    def load_short_features_rows(self) -> list[dict[str, Any]]:
        try:
            rows = self.con.execute(
                """
                SELECT
                    instrument_id,
                    company_id,
                    symbol,
                    as_of_date,
                    short_interest,
                    days_to_cover,
                    short_volume_ratio
                FROM short_features_daily
                ORDER BY instrument_id, as_of_date
                """
            ).fetchall()
            return [
                {
                    "instrument_id": row[0],
                    "company_id": row[1],
                    "symbol": row[2],
                    "as_of_date": row[3],
                    "short_interest": row[4],
                    "days_to_cover": row[5],
                    "short_volume_ratio": row[6],
                }
                for row in rows
            ]
        except Exception as exc:
            raise RepositoryError(f"failed to load short_features_daily rows: {exc}") from exc

    def load_news_features_rows(self) -> list[dict[str, Any]]:
        try:
            rows = self.con.execute(
                """
                SELECT
                    instrument_id,
                    company_id,
                    symbol,
                    as_of_date,
                    article_count_1d,
                    unique_cluster_count_1d,
                    avg_link_confidence
                FROM news_features_daily
                ORDER BY instrument_id, as_of_date
                """
            ).fetchall()
            return [
                {
                    "instrument_id": row[0],
                    "company_id": row[1],
                    "symbol": row[2],
                    "as_of_date": row[3],
                    "article_count_1d": row[4],
                    "unique_cluster_count_1d": row[5],
                    "avg_link_confidence": row[6],
                }
                for row in rows
            ]
        except Exception as exc:
            raise RepositoryError(f"failed to load news_features_daily rows: {exc}") from exc

    def load_instrument_company_map(self) -> dict[str, str | None]:
        try:
            rows = self.con.execute(
                """
                SELECT instrument_id, company_id
                FROM instrument_master
                ORDER BY instrument_id
                """
            ).fetchall()
            return {row[0]: row[1] for row in rows}
        except Exception as exc:
            raise RepositoryError(f"failed to load instrument company map: {exc}") from exc

    def replace_technical_features_daily(self, rows: list[TechnicalFeatureDaily], instrument_company_map: dict[str, str | None]) -> int:
        try:
            self.con.execute("DELETE FROM technical_features_daily")
            if not rows:
                return 0

            payload = [
                (
                    row.instrument_id,
                    instrument_company_map.get(row.instrument_id),
                    row.symbol,
                    row.as_of_date,
                    row.close_to_sma_20,
                    row.rsi_14,
                    row.source_name,
                    row.created_at or datetime.utcnow(),
                )
                for row in rows
            ]
            self.con.executemany(
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
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                payload,
            )
            return len(payload)
        except Exception as exc:
            raise RepositoryError(f"failed to replace technical_features_daily: {exc}") from exc

    def load_technical_features_rows(self) -> list[dict[str, Any]]:
        try:
            rows = self.con.execute(
                """
                SELECT
                    instrument_id,
                    company_id,
                    symbol,
                    as_of_date,
                    close_to_sma_20,
                    rsi_14
                FROM technical_features_daily
                ORDER BY instrument_id, as_of_date
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
                }
                for row in rows
            ]
        except Exception as exc:
            raise RepositoryError(f"failed to load technical_features_daily rows: {exc}") from exc

    def replace_research_features_daily(self, rows: list[ResearchFeatureDaily]) -> int:
        try:
            self.con.execute("DELETE FROM research_features_daily")
            if not rows:
                return 0

            payload = [
                (
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
                    row.source_name,
                    row.created_at or datetime.utcnow(),
                )
                for row in rows
            ]
            self.con.executemany(
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
                    article_count_1d,
                    unique_cluster_count_1d,
                    avg_link_confidence,
                    source_name,
                    created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                payload,
            )
            return len(payload)
        except Exception as exc:
            raise RepositoryError(f"failed to replace research_features_daily: {exc}") from exc
