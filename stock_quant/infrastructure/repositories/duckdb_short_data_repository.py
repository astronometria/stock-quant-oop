from __future__ import annotations

from datetime import datetime
from typing import Any

from stock_quant.domain.entities.short_data import (
    DailyShortVolumeSnapshot,
    ShortFeatureDaily,
    ShortInterestSnapshot,
)
from stock_quant.infrastructure.db.unit_of_work import DuckDbUnitOfWork
from stock_quant.shared.exceptions import RepositoryError


class DuckDbShortDataRepository:
    """
    Incremental canonical short-data repository.

    Design goals:
    - no global DELETE on canonical history tables
    - point-in-time fields preserved when available
    - compatibility with evolving schema during migration
    - symbol/instrument keyed upserts instead of destructive rebuilds
    """

    def __init__(self, uow: DuckDbUnitOfWork) -> None:
        self.uow = uow

    @property
    def con(self):
        if self.uow.connection is None:
            raise RepositoryError("active DB connection is required")
        return self.uow.connection

    # ------------------------------------------------------------------
    # Reads
    # ------------------------------------------------------------------

    def load_symbol_map(self) -> dict[str, dict[str, str | None]]:
        try:
            rows = self.con.execute(
                """
                SELECT symbol, instrument_id, company_id
                FROM instrument_master
                WHERE symbol IS NOT NULL
                  AND TRIM(symbol) <> ''
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

    def load_finra_short_interest_raw_rows(self) -> list[dict[str, Any]]:
        try:
            info = self.con.execute("PRAGMA table_info('finra_short_interest_source_raw')").fetchall()
            column_names = {str(row[1]).strip() for row in info}

            settlement_expr = "settlement_date"
            if "settlement_date" not in column_names and "source_date" in column_names:
                settlement_expr = "source_date"

            days_to_cover_expr = "days_to_cover"
            if "days_to_cover" not in column_names:
                if "short_interest" in column_names and "avg_daily_volume" in column_names:
                    days_to_cover_expr = """
                        CASE
                            WHEN avg_daily_volume IS NOT NULL
                             AND avg_daily_volume <> 0
                             AND short_interest IS NOT NULL
                            THEN short_interest / avg_daily_volume
                            ELSE NULL
                        END
                    """
                else:
                    days_to_cover_expr = "NULL"

            publication_date_expr = "NULL"
            if "publication_date" in column_names:
                publication_date_expr = "publication_date"
            elif "source_date" in column_names:
                publication_date_expr = "source_date"

            available_at_expr = "NULL"
            if "available_at" in column_names:
                available_at_expr = "available_at"
            elif "accepted_at" in column_names:
                available_at_expr = "accepted_at"

            rows = self.con.execute(
                f"""
                SELECT
                    symbol,
                    {settlement_expr} AS settlement_date,
                    {publication_date_expr} AS publication_date,
                    {available_at_expr} AS available_at,
                    short_interest,
                    previous_short_interest,
                    avg_daily_volume,
                    {days_to_cover_expr} AS days_to_cover,
                    source_file
                FROM finra_short_interest_source_raw
                ORDER BY symbol, settlement_date
                """
            ).fetchall()

            return [
                {
                    "symbol": row[0],
                    "settlement_date": row[1],
                    "publication_date": row[2],
                    "available_at": row[3],
                    "short_interest": row[4],
                    "previous_short_interest": row[5],
                    "avg_daily_volume": row[6],
                    "days_to_cover": row[7],
                    "source_file": row[8],
                }
                for row in rows
            ]
        except Exception as exc:
            raise RepositoryError(f"failed to load finra_short_interest_source_raw: {exc}") from exc

    def load_finra_daily_short_volume_raw_rows(self) -> list[dict[str, Any]]:
        try:
            info = self.con.execute("PRAGMA table_info('finra_daily_short_volume_source_raw')").fetchall()
            column_names = {str(row[1]).strip() for row in info}

            publication_date_expr = "NULL"
            if "publication_date" in column_names:
                publication_date_expr = "publication_date"

            available_at_expr = "NULL"
            if "available_at" in column_names:
                available_at_expr = "available_at"

            short_exempt_volume_expr = "NULL"
            if "short_exempt_volume" in column_names:
                short_exempt_volume_expr = "short_exempt_volume"

            rows = self.con.execute(
                f"""
                SELECT
                    symbol,
                    trade_date,
                    {publication_date_expr} AS publication_date,
                    {available_at_expr} AS available_at,
                    short_volume,
                    {short_exempt_volume_expr} AS short_exempt_volume,
                    total_volume,
                    source_name
                FROM finra_daily_short_volume_source_raw
                ORDER BY symbol, trade_date
                """
            ).fetchall()

            return [
                {
                    "symbol": row[0],
                    "trade_date": row[1],
                    "publication_date": row[2],
                    "available_at": row[3],
                    "short_volume": row[4],
                    "short_exempt_volume": row[5],
                    "total_volume": row[6],
                    "source_name": row[7],
                }
                for row in rows
            ]
        except Exception as exc:
            raise RepositoryError(f"failed to load finra_daily_short_volume_source_raw: {exc}") from exc

    # ------------------------------------------------------------------
    # Raw source table
    # ------------------------------------------------------------------

    def replace_daily_short_volume_source_raw(self, rows: list[dict[str, Any]]) -> int:
        return self.upsert_daily_short_volume_source_raw(rows)

    def upsert_daily_short_volume_source_raw(self, rows: list[dict[str, Any]]) -> int:
        try:
            if not rows:
                return 0

            payload = [
                (
                    self._norm_symbol(row.get("symbol")),
                    row.get("trade_date"),
                    row.get("short_volume"),
                    row.get("short_exempt_volume"),
                    row.get("total_volume"),
                    row.get("market_code"),
                    row.get("source_file"),
                    row.get("source_name", "finra"),
                    row.get("publication_date"),
                    row.get("available_at"),
                    row.get("ingested_at", datetime.utcnow()),
                )
                for row in rows
                if self._norm_symbol(row.get("symbol")) and row.get("trade_date") is not None
            ]
            if not payload:
                return 0

            target_columns = self._table_columns("finra_daily_short_volume_source_raw")
            stage_table = "tmp_finra_daily_short_volume_source_raw_stage"

            self.con.execute(f"DROP TABLE IF EXISTS {stage_table}")
            self.con.execute(
                f"""
                CREATE TEMP TABLE {stage_table} (
                    symbol VARCHAR,
                    trade_date DATE,
                    short_volume DOUBLE,
                    short_exempt_volume DOUBLE,
                    total_volume DOUBLE,
                    market_code VARCHAR,
                    source_file VARCHAR,
                    source_name VARCHAR,
                    publication_date DATE,
                    available_at TIMESTAMP,
                    ingested_at TIMESTAMP
                )
                """
            )

            try:
                self.con.executemany(
                    f"""
                    INSERT INTO {stage_table} (
                        symbol,
                        trade_date,
                        short_volume,
                        short_exempt_volume,
                        total_volume,
                        market_code,
                        source_file,
                        source_name,
                        publication_date,
                        available_at,
                        ingested_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    payload,
                )

                delete_predicate = """
                    WHERE UPPER(TRIM(target.symbol)) = UPPER(TRIM(stage.symbol))
                      AND target.trade_date = stage.trade_date
                """
                if "source_name" in target_columns:
                    delete_predicate += " AND COALESCE(target.source_name, '') = COALESCE(stage.source_name, '')"

                self.con.execute(
                    f"""
                    DELETE FROM finra_daily_short_volume_source_raw AS target
                    USING {stage_table} AS stage
                    {delete_predicate}
                    """
                )

                insert_cols = [name for name in [
                    "symbol",
                    "trade_date",
                    "short_volume",
                    "short_exempt_volume",
                    "total_volume",
                    "market_code",
                    "source_file",
                    "source_name",
                    "publication_date",
                    "available_at",
                    "ingested_at",
                ] if name in target_columns]

                self.con.execute(
                    f"""
                    INSERT INTO finra_daily_short_volume_source_raw ({", ".join(insert_cols)})
                    SELECT {", ".join(insert_cols)}
                    FROM (
                        SELECT
                            *,
                            ROW_NUMBER() OVER (
                                PARTITION BY symbol, trade_date, COALESCE(source_name, '')
                                ORDER BY available_at DESC NULLS LAST,
                                         ingested_at DESC NULLS LAST
                            ) AS rn
                        FROM {stage_table}
                    ) x
                    WHERE rn = 1
                    """
                )
            finally:
                self.con.execute(f"DROP TABLE IF EXISTS {stage_table}")

            return len(payload)
        except Exception as exc:
            raise RepositoryError(f"failed to upsert finra_daily_short_volume_source_raw: {exc}") from exc

    # ------------------------------------------------------------------
    # Canonical history tables
    # ------------------------------------------------------------------

    def replace_short_interest_history(self, rows: list[ShortInterestSnapshot]) -> int:
        return self.upsert_short_interest_history(rows)

    def upsert_short_interest_history(self, rows: list[ShortInterestSnapshot]) -> int:
        try:
            if not rows:
                return 0

            payload = [
                (
                    self._nullable_str(getattr(row, "instrument_id", None)),
                    self._nullable_str(getattr(row, "company_id", None)),
                    self._norm_symbol(getattr(row, "symbol", None)),
                    getattr(row, "settlement_date", None),
                    getattr(row, "publication_date", None),
                    getattr(row, "available_at", None),
                    getattr(row, "short_interest", None),
                    getattr(row, "previous_short_interest", None),
                    getattr(row, "avg_daily_volume", None),
                    getattr(row, "days_to_cover", None),
                    self._nullable_str(getattr(row, "source_name", None)) or "finra",
                    getattr(row, "created_at", None) or datetime.utcnow(),
                )
                for row in rows
                if self._norm_symbol(getattr(row, "symbol", None))
                and getattr(row, "settlement_date", None) is not None
            ]
            if not payload:
                return 0

            target_columns = self._table_columns("short_interest_history")
            stage_table = "tmp_short_interest_history_stage"

            self.con.execute(f"DROP TABLE IF EXISTS {stage_table}")
            self.con.execute(
                f"""
                CREATE TEMP TABLE {stage_table} (
                    instrument_id VARCHAR,
                    company_id VARCHAR,
                    symbol VARCHAR,
                    settlement_date DATE,
                    publication_date DATE,
                    available_at TIMESTAMP,
                    short_interest DOUBLE,
                    previous_short_interest DOUBLE,
                    avg_daily_volume DOUBLE,
                    days_to_cover DOUBLE,
                    source_name VARCHAR,
                    created_at TIMESTAMP
                )
                """
            )

            try:
                self.con.executemany(
                    f"""
                    INSERT INTO {stage_table} (
                        instrument_id,
                        company_id,
                        symbol,
                        settlement_date,
                        publication_date,
                        available_at,
                        short_interest,
                        previous_short_interest,
                        avg_daily_volume,
                        days_to_cover,
                        source_name,
                        created_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    payload,
                )

                delete_predicate = """
                    WHERE target.settlement_date = stage.settlement_date
                      AND (
                            (
                                target.instrument_id IS NOT NULL
                                AND stage.instrument_id IS NOT NULL
                                AND target.instrument_id = stage.instrument_id
                            )
                            OR (
                                (target.instrument_id IS NULL OR stage.instrument_id IS NULL)
                                AND UPPER(TRIM(target.symbol)) = UPPER(TRIM(stage.symbol))
                            )
                      )
                """
                if "source_name" in target_columns:
                    delete_predicate += " AND COALESCE(target.source_name, '') = COALESCE(stage.source_name, '')"

                self.con.execute(
                    f"""
                    DELETE FROM short_interest_history AS target
                    USING {stage_table} AS stage
                    {delete_predicate}
                    """
                )

                insert_cols = [name for name in [
                    "instrument_id",
                    "company_id",
                    "symbol",
                    "settlement_date",
                    "publication_date",
                    "available_at",
                    "short_interest",
                    "previous_short_interest",
                    "avg_daily_volume",
                    "days_to_cover",
                    "source_name",
                    "created_at",
                ] if name in target_columns]

                self.con.execute(
                    f"""
                    INSERT INTO short_interest_history ({", ".join(insert_cols)})
                    SELECT {", ".join(insert_cols)}
                    FROM (
                        SELECT
                            *,
                            ROW_NUMBER() OVER (
                                PARTITION BY
                                    COALESCE(instrument_id, symbol),
                                    settlement_date,
                                    COALESCE(source_name, '')
                                ORDER BY available_at DESC NULLS LAST,
                                         created_at DESC NULLS LAST
                            ) AS rn
                        FROM {stage_table}
                    ) x
                    WHERE rn = 1
                    """
                )
            finally:
                self.con.execute(f"DROP TABLE IF EXISTS {stage_table}")

            return len(payload)
        except Exception as exc:
            raise RepositoryError(f"failed to upsert short_interest_history: {exc}") from exc

    def replace_daily_short_volume_history(self, rows: list[DailyShortVolumeSnapshot]) -> int:
        return self.upsert_daily_short_volume_history(rows)

    def upsert_daily_short_volume_history(self, rows: list[DailyShortVolumeSnapshot]) -> int:
        try:
            if not rows:
                return 0

            payload = [
                (
                    self._nullable_str(getattr(row, "instrument_id", None)),
                    self._nullable_str(getattr(row, "company_id", None)),
                    self._norm_symbol(getattr(row, "symbol", None)),
                    getattr(row, "trade_date", None),
                    getattr(row, "publication_date", None),
                    getattr(row, "available_at", None),
                    getattr(row, "short_volume", None),
                    getattr(row, "short_exempt_volume", None),
                    getattr(row, "total_volume", None),
                    getattr(row, "short_volume_ratio", None),
                    self._nullable_str(getattr(row, "source_name", None)) or "finra",
                    getattr(row, "created_at", None) or datetime.utcnow(),
                )
                for row in rows
                if self._norm_symbol(getattr(row, "symbol", None))
                and getattr(row, "trade_date", None) is not None
            ]
            if not payload:
                return 0

            target_columns = self._table_columns("daily_short_volume_history")
            stage_table = "tmp_daily_short_volume_history_stage"

            self.con.execute(f"DROP TABLE IF EXISTS {stage_table}")
            self.con.execute(
                f"""
                CREATE TEMP TABLE {stage_table} (
                    instrument_id VARCHAR,
                    company_id VARCHAR,
                    symbol VARCHAR,
                    trade_date DATE,
                    publication_date DATE,
                    available_at TIMESTAMP,
                    short_volume DOUBLE,
                    short_exempt_volume DOUBLE,
                    total_volume DOUBLE,
                    short_volume_ratio DOUBLE,
                    source_name VARCHAR,
                    created_at TIMESTAMP
                )
                """
            )

            try:
                self.con.executemany(
                    f"""
                    INSERT INTO {stage_table} (
                        instrument_id,
                        company_id,
                        symbol,
                        trade_date,
                        publication_date,
                        available_at,
                        short_volume,
                        short_exempt_volume,
                        total_volume,
                        short_volume_ratio,
                        source_name,
                        created_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    payload,
                )

                delete_predicate = """
                    WHERE target.trade_date = stage.trade_date
                      AND (
                            (
                                target.instrument_id IS NOT NULL
                                AND stage.instrument_id IS NOT NULL
                                AND target.instrument_id = stage.instrument_id
                            )
                            OR (
                                (target.instrument_id IS NULL OR stage.instrument_id IS NULL)
                                AND UPPER(TRIM(target.symbol)) = UPPER(TRIM(stage.symbol))
                            )
                      )
                """
                if "source_name" in target_columns:
                    delete_predicate += " AND COALESCE(target.source_name, '') = COALESCE(stage.source_name, '')"

                self.con.execute(
                    f"""
                    DELETE FROM daily_short_volume_history AS target
                    USING {stage_table} AS stage
                    {delete_predicate}
                    """
                )

                insert_cols = [name for name in [
                    "instrument_id",
                    "company_id",
                    "symbol",
                    "trade_date",
                    "publication_date",
                    "available_at",
                    "short_volume",
                    "short_exempt_volume",
                    "total_volume",
                    "short_volume_ratio",
                    "source_name",
                    "created_at",
                ] if name in target_columns]

                self.con.execute(
                    f"""
                    INSERT INTO daily_short_volume_history ({", ".join(insert_cols)})
                    SELECT {", ".join(insert_cols)}
                    FROM (
                        SELECT
                            *,
                            ROW_NUMBER() OVER (
                                PARTITION BY
                                    COALESCE(instrument_id, symbol),
                                    trade_date,
                                    COALESCE(source_name, '')
                                ORDER BY available_at DESC NULLS LAST,
                                         created_at DESC NULLS LAST
                            ) AS rn
                        FROM {stage_table}
                    ) x
                    WHERE rn = 1
                    """
                )
            finally:
                self.con.execute(f"DROP TABLE IF EXISTS {stage_table}")

            return len(payload)
        except Exception as exc:
            raise RepositoryError(f"failed to upsert daily_short_volume_history: {exc}") from exc

    def replace_short_features_daily(self, rows: list[ShortFeatureDaily]) -> int:
        return self.upsert_short_features_daily(rows)

    def upsert_short_features_daily(self, rows: list[ShortFeatureDaily]) -> int:
        try:
            if not rows:
                return 0

            payload = [
                (
                    self._nullable_str(getattr(row, "instrument_id", None)),
                    self._nullable_str(getattr(row, "company_id", None)),
                    self._norm_symbol(getattr(row, "symbol", None)),
                    getattr(row, "as_of_date", None),
                    getattr(row, "short_interest", None),
                    getattr(row, "avg_daily_volume", None),
                    getattr(row, "days_to_cover", None),
                    getattr(row, "short_volume", None),
                    getattr(row, "short_exempt_volume", None),
                    getattr(row, "total_volume", None),
                    getattr(row, "short_volume_ratio", None),
                    getattr(row, "short_interest_change", None),
                    getattr(row, "short_interest_change_pct", None),
                    getattr(row, "short_squeeze_score", None),
                    getattr(row, "short_pressure_zscore", None),
                    getattr(row, "days_to_cover_zscore", None),
                    getattr(row, "max_source_available_at", None),
                    self._nullable_str(getattr(row, "source_name", None)) or "finra",
                    getattr(row, "created_at", None) or datetime.utcnow(),
                )
                for row in rows
                if self._norm_symbol(getattr(row, "symbol", None))
                and getattr(row, "as_of_date", None) is not None
            ]
            if not payload:
                return 0

            target_columns = self._table_columns("short_features_daily")
            stage_table = "tmp_short_features_daily_stage"

            self.con.execute(f"DROP TABLE IF EXISTS {stage_table}")
            self.con.execute(
                f"""
                CREATE TEMP TABLE {stage_table} (
                    instrument_id VARCHAR,
                    company_id VARCHAR,
                    symbol VARCHAR,
                    as_of_date DATE,
                    short_interest DOUBLE,
                    avg_daily_volume DOUBLE,
                    days_to_cover DOUBLE,
                    short_volume DOUBLE,
                    short_exempt_volume DOUBLE,
                    total_volume DOUBLE,
                    short_volume_ratio DOUBLE,
                    short_interest_change DOUBLE,
                    short_interest_change_pct DOUBLE,
                    short_squeeze_score DOUBLE,
                    short_pressure_zscore DOUBLE,
                    days_to_cover_zscore DOUBLE,
                    max_source_available_at TIMESTAMP,
                    source_name VARCHAR,
                    created_at TIMESTAMP
                )
                """
            )

            try:
                self.con.executemany(
                    f"""
                    INSERT INTO {stage_table} (
                        instrument_id,
                        company_id,
                        symbol,
                        as_of_date,
                        short_interest,
                        avg_daily_volume,
                        days_to_cover,
                        short_volume,
                        short_exempt_volume,
                        total_volume,
                        short_volume_ratio,
                        short_interest_change,
                        short_interest_change_pct,
                        short_squeeze_score,
                        short_pressure_zscore,
                        days_to_cover_zscore,
                        max_source_available_at,
                        source_name,
                        created_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    payload,
                )

                delete_predicate = """
                    WHERE target.as_of_date = stage.as_of_date
                      AND (
                            (
                                target.instrument_id IS NOT NULL
                                AND stage.instrument_id IS NOT NULL
                                AND target.instrument_id = stage.instrument_id
                            )
                            OR (
                                (target.instrument_id IS NULL OR stage.instrument_id IS NULL)
                                AND UPPER(TRIM(target.symbol)) = UPPER(TRIM(stage.symbol))
                            )
                      )
                """

                self.con.execute(
                    f"""
                    DELETE FROM short_features_daily AS target
                    USING {stage_table} AS stage
                    {delete_predicate}
                    """
                )

                insert_cols = [name for name in [
                    "instrument_id",
                    "company_id",
                    "symbol",
                    "as_of_date",
                    "short_interest",
                    "avg_daily_volume",
                    "days_to_cover",
                    "short_volume",
                    "short_exempt_volume",
                    "total_volume",
                    "short_volume_ratio",
                    "short_interest_change",
                    "short_interest_change_pct",
                    "short_squeeze_score",
                    "short_pressure_zscore",
                    "days_to_cover_zscore",
                    "max_source_available_at",
                    "source_name",
                    "created_at",
                ] if name in target_columns]

                self.con.execute(
                    f"""
                    INSERT INTO short_features_daily ({", ".join(insert_cols)})
                    SELECT {", ".join(insert_cols)}
                    FROM (
                        SELECT
                            *,
                            ROW_NUMBER() OVER (
                                PARTITION BY COALESCE(instrument_id, symbol), as_of_date
                                ORDER BY max_source_available_at DESC NULLS LAST,
                                         created_at DESC NULLS LAST
                            ) AS rn
                        FROM {stage_table}
                    ) x
                    WHERE rn = 1
                    """
                )
            finally:
                self.con.execute(f"DROP TABLE IF EXISTS {stage_table}")

            return len(payload)
        except Exception as exc:
            raise RepositoryError(f"failed to upsert short_features_daily: {exc}") from exc

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _table_columns(self, table_name: str) -> set[str]:
        try:
            rows = self.con.execute(f"PRAGMA table_info('{table_name}')").fetchall()
            return {str(row[1]).strip() for row in rows}
        except Exception as exc:
            raise RepositoryError(f"failed to inspect table columns for {table_name}: {exc}") from exc

    def _norm_symbol(self, value: Any) -> str:
        if value is None:
            return ""
        return str(value).strip().upper()

    def _nullable_str(self, value: Any) -> str | None:
        if value is None:
            return None
        text = str(value).strip()
        return text or None
