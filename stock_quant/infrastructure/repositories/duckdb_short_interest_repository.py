from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from stock_quant.domain.entities.short_interest import (
    RawShortInterestRecord,
    ShortInterestRecord,
    ShortInterestSourceFile,
)
from stock_quant.domain.ports.repositories import ShortInterestRepositoryPort
from stock_quant.infrastructure.db.table_names import (
    FINRA_SHORT_INTEREST_HISTORY,
    FINRA_SHORT_INTEREST_LATEST,
    FINRA_SHORT_INTEREST_SOURCE_RAW,
    FINRA_SHORT_INTEREST_SOURCES,
)
from stock_quant.shared.exceptions import RepositoryError


# -----------------------------------------------------------------------------
# SQL-first repository for FINRA short interest.
#
# Important design choice:
# - raw table stays raw/staging-oriented
# - history/latest are canonical SQL-derived layers
# - no Python row-by-row transforms in the canonical path
# -----------------------------------------------------------------------------

@dataclass(frozen=True)
class ShortInterestBuildState:
    raw_row_count: int
    source_row_count: int
    history_row_count: int
    latest_row_count: int
    max_raw_source_date: str | None
    max_history_settlement_date: str | None
    max_latest_settlement_date: str | None


class DuckDbShortInterestRepository(ShortInterestRepositoryPort):
    """
    Canonical SQL-first repository.

    This class intentionally keeps a few legacy abstract-method names implemented
    so the existing repository port remains satisfiable, while the main pipeline
    now uses SQL-first methods instead of object-by-object transforms.
    """

    def __init__(self, con: Any) -> None:
        self.con = con

    # -------------------------------------------------------------------------
    # Internal helpers
    # -------------------------------------------------------------------------

    def _require_connection(self):
        if self.con is None:
            raise RepositoryError("active DuckDB connection is required")
        return self.con

    @staticmethod
    def _scalar_int(value: Any) -> int:
        if value is None:
            return 0
        return int(value)

    @staticmethod
    def _scalar_str_or_none(value: Any) -> str | None:
        if value is None:
            return None
        text = str(value).strip()
        return text if text else None

    # -------------------------------------------------------------------------
    # Legacy / abstract-method compatibility
    # -------------------------------------------------------------------------

    def load_included_symbols(self) -> set[str]:
        """
        Historical-safe symbol probe.

        This method is preserved mainly for compatibility with the abstract port.
        The SQL-first canonical pipeline does not depend on current-universe
        filtering for short-interest history construction.
        """
        try:
            con = self._require_connection()
            tables = {
                str(row[0]).strip().lower()
                for row in con.execute("SHOW TABLES").fetchall()
            }

            union_parts: list[str] = []

            if "ticker_history" in tables:
                union_parts.append("SELECT symbol FROM ticker_history")

            if "instrument_master" in tables:
                union_parts.append("SELECT symbol FROM instrument_master")

            if union_parts:
                rows = con.execute(
                    f"""
                    SELECT DISTINCT UPPER(TRIM(symbol)) AS symbol
                    FROM (
                        {' UNION '.join(union_parts)}
                    ) t
                    WHERE symbol IS NOT NULL
                      AND TRIM(symbol) <> ''
                    ORDER BY symbol
                    """
                ).fetchall()
                result = {str(row[0]).strip().upper() for row in rows if row[0]}
                if result:
                    return result

            if "market_universe" in tables:
                rows = con.execute(
                    """
                    SELECT DISTINCT UPPER(TRIM(symbol)) AS symbol
                    FROM market_universe
                    WHERE include_in_universe = TRUE
                      AND symbol IS NOT NULL
                      AND TRIM(symbol) <> ''
                    ORDER BY symbol
                    """
                ).fetchall()
                return {str(row[0]).strip().upper() for row in rows if row[0]}

            return set()
        except Exception as exc:
            raise RepositoryError(f"failed to load included symbols: {exc}") from exc

    def load_raw_short_interest_records(self) -> list[RawShortInterestRecord]:
        """
        Compatibility method for older tests / call sites.

        Note:
        - raw table does NOT reliably store derived columns like days_to_cover
        - those belong to canonical history/latest and are computed in SQL later
        """
        try:
            con = self._require_connection()
            rows = con.execute(
                f"""
                SELECT
                    symbol,
                    settlement_date,
                    short_interest,
                    previous_short_interest,
                    avg_daily_volume,
                    shares_float,
                    revision_flag,
                    source_market,
                    source_file,
                    source_date
                FROM {FINRA_SHORT_INTEREST_SOURCE_RAW}
                ORDER BY settlement_date, symbol, source_file
                """
            ).fetchall()

            return [
                RawShortInterestRecord(
                    symbol=row[0],
                    settlement_date=row[1],
                    short_interest=row[2],
                    previous_short_interest=row[3],
                    avg_daily_volume=row[4],
                    shares_float=row[5],
                    revision_flag=row[6],
                    source_market=row[7] or "unknown",
                    source_file=row[8] or "unknown",
                    source_date=row[9],
                )
                for row in rows
            ]
        except Exception as exc:
            raise RepositoryError(f"failed to load raw short-interest records: {exc}") from exc

    def replace_short_interest_history(self, entries: list[ShortInterestRecord]) -> int:
        """
        Legacy compatibility path.

        The canonical pipeline should not use this anymore. We keep it working
        for compatibility, but it is intentionally not the primary code path.
        """
        return self.upsert_short_interest_history(entries)

    def replace_short_interest_sources(self, entries: list[ShortInterestSourceFile]) -> int:
        """
        Legacy compatibility path.
        """
        return self.upsert_short_interest_sources(entries)

    # -------------------------------------------------------------------------
    # State probes used by SQL-first pipeline
    # -------------------------------------------------------------------------

    def get_raw_row_count(self) -> int:
        try:
            con = self._require_connection()
            return self._scalar_int(
                con.execute(f"SELECT COUNT(*) FROM {FINRA_SHORT_INTEREST_SOURCE_RAW}").fetchone()[0]
            )
        except Exception as exc:
            raise RepositoryError(f"failed to get raw row count: {exc}") from exc

    def get_source_row_count(self) -> int:
        try:
            con = self._require_connection()
            return self._scalar_int(
                con.execute(f"SELECT COUNT(*) FROM {FINRA_SHORT_INTEREST_SOURCES}").fetchone()[0]
            )
        except Exception as exc:
            raise RepositoryError(f"failed to get source row count: {exc}") from exc

    def get_history_row_count(self) -> int:
        try:
            con = self._require_connection()
            return self._scalar_int(
                con.execute(f"SELECT COUNT(*) FROM {FINRA_SHORT_INTEREST_HISTORY}").fetchone()[0]
            )
        except Exception as exc:
            raise RepositoryError(f"failed to get history row count: {exc}") from exc

    def get_latest_row_count(self) -> int:
        try:
            con = self._require_connection()
            return self._scalar_int(
                con.execute(f"SELECT COUNT(*) FROM {FINRA_SHORT_INTEREST_LATEST}").fetchone()[0]
            )
        except Exception as exc:
            raise RepositoryError(f"failed to get latest row count: {exc}") from exc

    def get_max_raw_source_date(self) -> str | None:
        try:
            con = self._require_connection()
            value = con.execute(
                f"SELECT CAST(MAX(source_date) AS VARCHAR) FROM {FINRA_SHORT_INTEREST_SOURCE_RAW}"
            ).fetchone()[0]
            return self._scalar_str_or_none(value)
        except Exception as exc:
            raise RepositoryError(f"failed to get max raw source date: {exc}") from exc

    def get_max_history_settlement_date(self) -> str | None:
        try:
            con = self._require_connection()
            value = con.execute(
                f"SELECT CAST(MAX(settlement_date) AS VARCHAR) FROM {FINRA_SHORT_INTEREST_HISTORY}"
            ).fetchone()[0]
            return self._scalar_str_or_none(value)
        except Exception as exc:
            raise RepositoryError(f"failed to get max history settlement date: {exc}") from exc

    def get_max_latest_settlement_date(self) -> str | None:
        try:
            con = self._require_connection()
            value = con.execute(
                f"SELECT CAST(MAX(settlement_date) AS VARCHAR) FROM {FINRA_SHORT_INTEREST_LATEST}"
            ).fetchone()[0]
            return self._scalar_str_or_none(value)
        except Exception as exc:
            raise RepositoryError(f"failed to get max latest settlement date: {exc}") from exc

    def get_build_state(self) -> ShortInterestBuildState:
        """
        One compact state object for the pipeline decision layer.
        """
        return ShortInterestBuildState(
            raw_row_count=self.get_raw_row_count(),
            source_row_count=self.get_source_row_count(),
            history_row_count=self.get_history_row_count(),
            latest_row_count=self.get_latest_row_count(),
            max_raw_source_date=self.get_max_raw_source_date(),
            max_history_settlement_date=self.get_max_history_settlement_date(),
            max_latest_settlement_date=self.get_max_latest_settlement_date(),
        )

    # -------------------------------------------------------------------------
    # SQL-first canonical write path
    # -------------------------------------------------------------------------

    def insert_history_from_raw_sql_first(self) -> dict[str, int]:
        """
        Canonical SQL-first build from raw -> history.

        Key properties:
        - vectorized DuckDB SQL only
        - dedupe inside raw staging by (symbol, settlement_date, source_file)
        - avoid duplicate reinserts into history
        - compute derived metrics here, not from raw
        """
        try:
            con = self._require_connection()

            before_count = self.get_history_row_count()

            con.execute("DROP TABLE IF EXISTS tmp_finra_short_interest_history_stage")
            con.execute(
                """
                CREATE TEMP TABLE tmp_finra_short_interest_history_stage AS
                SELECT
                    UPPER(TRIM(symbol)) AS symbol,
                    settlement_date,
                    CAST(short_interest AS BIGINT) AS short_interest,
                    CAST(previous_short_interest AS BIGINT) AS previous_short_interest,
                    CAST(avg_daily_volume AS DOUBLE) AS avg_daily_volume,
                    CASE
                        WHEN avg_daily_volume IS NOT NULL AND avg_daily_volume > 0
                            THEN CAST(short_interest AS DOUBLE) / CAST(avg_daily_volume AS DOUBLE)
                        ELSE NULL
                    END AS days_to_cover,
                    CAST(shares_float AS BIGINT) AS shares_float,
                    CASE
                        WHEN shares_float IS NOT NULL AND shares_float > 0
                            THEN CAST(short_interest AS DOUBLE) / CAST(shares_float AS DOUBLE)
                        ELSE NULL
                    END AS short_interest_pct_float,
                    revision_flag,
                    source_market,
                    source_file,
                    COALESCE(ingested_at, CURRENT_TIMESTAMP) AS ingested_at
                FROM (
                    SELECT
                        *,
                        ROW_NUMBER() OVER (
                            PARTITION BY UPPER(TRIM(symbol)), settlement_date, source_file
                            ORDER BY ingested_at DESC NULLS LAST
                        ) AS rn
                    FROM finra_short_interest_source_raw
                    WHERE symbol IS NOT NULL
                      AND TRIM(symbol) <> ''
                      AND settlement_date IS NOT NULL
                      AND source_file IS NOT NULL
                      AND TRIM(source_file) <> ''
                ) s
                WHERE rn = 1
                """
            )

            staged_count = self._scalar_int(
                con.execute("SELECT COUNT(*) FROM tmp_finra_short_interest_history_stage").fetchone()[0]
            )

            # Insert only rows not already present in canonical history.
            con.execute(
                f"""
                INSERT INTO {FINRA_SHORT_INTEREST_HISTORY} (
                    symbol,
                    settlement_date,
                    short_interest,
                    previous_short_interest,
                    avg_daily_volume,
                    days_to_cover,
                    shares_float,
                    short_interest_pct_float,
                    revision_flag,
                    source_market,
                    source_file,
                    ingested_at
                )
                SELECT
                    s.symbol,
                    s.settlement_date,
                    s.short_interest,
                    s.previous_short_interest,
                    s.avg_daily_volume,
                    s.days_to_cover,
                    s.shares_float,
                    s.short_interest_pct_float,
                    s.revision_flag,
                    s.source_market,
                    s.source_file,
                    s.ingested_at
                FROM tmp_finra_short_interest_history_stage s
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM {FINRA_SHORT_INTEREST_HISTORY} h
                    WHERE UPPER(TRIM(h.symbol)) = s.symbol
                      AND h.settlement_date = s.settlement_date
                      AND COALESCE(h.source_file, '') = COALESCE(s.source_file, '')
                )
                """
            )

            after_count = self.get_history_row_count()
            inserted_count = after_count - before_count

            con.execute("DROP TABLE IF EXISTS tmp_finra_short_interest_history_stage")

            return {
                "history_rows_before": before_count,
                "history_stage_rows": staged_count,
                "history_rows_inserted": inserted_count,
                "history_rows_after": after_count,
            }
        except Exception as exc:
            raise RepositoryError(f"failed to build history from raw in SQL-first mode: {exc}") from exc

    def rebuild_latest_from_history_sql_first(self) -> dict[str, int]:
        """
        Canonical SQL-first rebuild of latest snapshot from history.
        """
        try:
            con = self._require_connection()

            before_count = self.get_latest_row_count()

            con.execute(f"DELETE FROM {FINRA_SHORT_INTEREST_LATEST}")

            con.execute(
                f"""
                INSERT INTO {FINRA_SHORT_INTEREST_LATEST} (
                    symbol,
                    settlement_date,
                    short_interest,
                    previous_short_interest,
                    avg_daily_volume,
                    days_to_cover,
                    shares_float,
                    short_interest_pct_float,
                    revision_flag,
                    source_market,
                    source_file,
                    updated_at
                )
                SELECT
                    symbol,
                    settlement_date,
                    short_interest,
                    previous_short_interest,
                    avg_daily_volume,
                    days_to_cover,
                    shares_float,
                    short_interest_pct_float,
                    revision_flag,
                    source_market,
                    source_file,
                    CURRENT_TIMESTAMP
                FROM (
                    SELECT
                        h.*,
                        ROW_NUMBER() OVER (
                            PARTITION BY UPPER(TRIM(h.symbol))
                            ORDER BY h.settlement_date DESC, h.ingested_at DESC NULLS LAST, h.source_file DESC
                        ) AS rn
                    FROM {FINRA_SHORT_INTEREST_HISTORY} h
                    WHERE h.symbol IS NOT NULL
                      AND TRIM(h.symbol) <> ''
                      AND h.settlement_date IS NOT NULL
                ) x
                WHERE rn = 1
                """
            )

            after_count = self.get_latest_row_count()

            return {
                "latest_rows_before": before_count,
                "latest_rows_inserted": after_count,
                "latest_rows_after": after_count,
            }
        except Exception as exc:
            raise RepositoryError(f"failed to rebuild latest from history in SQL-first mode: {exc}") from exc

    # -------------------------------------------------------------------------
    # Service compatibility helpers
    # -------------------------------------------------------------------------

    def load_raw(self) -> list[dict[str, Any]]:
        """
        Lightweight compatibility helper.

        The SQL-first pipeline no longer needs the full Python payload, but some
        service code still probes this method. We keep it functional and schema-safe.
        """
        try:
            con = self._require_connection()
            rows = con.execute(
                f"""
                SELECT
                    UPPER(TRIM(symbol)) AS symbol,
                    settlement_date,
                    short_interest,
                    previous_short_interest,
                    avg_daily_volume,
                    shares_float,
                    revision_flag,
                    source_market,
                    source_file,
                    source_date,
                    ingested_at
                FROM {FINRA_SHORT_INTEREST_SOURCE_RAW}
                WHERE symbol IS NOT NULL
                  AND TRIM(symbol) <> ''
                  AND settlement_date IS NOT NULL
                ORDER BY settlement_date, symbol, source_file
                """
            ).fetchall()

            return [
                {
                    "symbol": row[0],
                    "settlement_date": row[1],
                    "short_interest": row[2],
                    "previous_short_interest": row[3],
                    "avg_daily_volume": row[4],
                    "shares_float": row[5],
                    "revision_flag": row[6],
                    "source_market": row[7],
                    "source_file": row[8],
                    "source_date": row[9],
                    "ingested_at": row[10],
                }
                for row in rows
            ]
        except Exception as exc:
            raise RepositoryError(f"failed to load raw short-interest rows: {exc}") from exc

    def insert_history(self, normalized_rows: list[dict[str, Any]]) -> int:
        """
        Compatibility shim.

        Canonical path should use insert_history_from_raw_sql_first().
        """
        result = self.insert_history_from_raw_sql_first()
        return int(result["history_rows_inserted"])

    def rebuild_latest(self) -> int:
        """
        Compatibility shim.
        """
        result = self.rebuild_latest_from_history_sql_first()
        return int(result["latest_rows_after"])

    # -------------------------------------------------------------------------
    # Legacy upsert paths retained for compatibility
    # -------------------------------------------------------------------------

    def upsert_short_interest_history(self, entries: list[ShortInterestRecord]) -> int:
        try:
            if not entries:
                return 0

            con = self._require_connection()

            payload = [
                (
                    str(e.symbol).strip().upper(),
                    e.settlement_date,
                    e.short_interest,
                    e.previous_short_interest,
                    e.avg_daily_volume,
                    e.days_to_cover,
                    e.shares_float,
                    e.short_interest_pct_float,
                    e.revision_flag,
                    e.source_market,
                    e.source_file,
                    e.ingested_at,
                )
                for e in entries
                if e.symbol
                and str(e.symbol).strip()
                and e.settlement_date is not None
                and e.source_file
                and str(e.source_file).strip()
            ]

            if not payload:
                return 0

            con.execute("DROP TABLE IF EXISTS tmp_finra_short_interest_history_manual_stage")
            con.execute(
                """
                CREATE TEMP TABLE tmp_finra_short_interest_history_manual_stage (
                    symbol VARCHAR,
                    settlement_date DATE,
                    short_interest BIGINT,
                    previous_short_interest BIGINT,
                    avg_daily_volume DOUBLE,
                    days_to_cover DOUBLE,
                    shares_float BIGINT,
                    short_interest_pct_float DOUBLE,
                    revision_flag VARCHAR,
                    source_market VARCHAR,
                    source_file VARCHAR,
                    ingested_at TIMESTAMP
                )
                """
            )
            con.executemany(
                """
                INSERT INTO tmp_finra_short_interest_history_manual_stage (
                    symbol,
                    settlement_date,
                    short_interest,
                    previous_short_interest,
                    avg_daily_volume,
                    days_to_cover,
                    shares_float,
                    short_interest_pct_float,
                    revision_flag,
                    source_market,
                    source_file,
                    ingested_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                payload,
            )

            con.execute(
                f"""
                DELETE FROM {FINRA_SHORT_INTEREST_HISTORY} h
                USING tmp_finra_short_interest_history_manual_stage s
                WHERE UPPER(TRIM(h.symbol)) = UPPER(TRIM(s.symbol))
                  AND h.settlement_date = s.settlement_date
                  AND COALESCE(h.source_file, '') = COALESCE(s.source_file, '')
                """
            )

            con.execute(
                f"""
                INSERT INTO {FINRA_SHORT_INTEREST_HISTORY} (
                    symbol,
                    settlement_date,
                    short_interest,
                    previous_short_interest,
                    avg_daily_volume,
                    days_to_cover,
                    shares_float,
                    short_interest_pct_float,
                    revision_flag,
                    source_market,
                    source_file,
                    ingested_at
                )
                SELECT
                    symbol,
                    settlement_date,
                    short_interest,
                    previous_short_interest,
                    avg_daily_volume,
                    days_to_cover,
                    shares_float,
                    short_interest_pct_float,
                    revision_flag,
                    source_market,
                    source_file,
                    ingested_at
                FROM tmp_finra_short_interest_history_manual_stage
                """
            )

            con.execute("DROP TABLE IF EXISTS tmp_finra_short_interest_history_manual_stage")
            return len(payload)
        except Exception as exc:
            raise RepositoryError(f"failed to upsert short-interest history: {exc}") from exc

    def upsert_short_interest_sources(self, entries: list[ShortInterestSourceFile]) -> int:
        try:
            if not entries:
                return 0

            con = self._require_connection()

            payload = [
                (
                    e.source_file,
                    e.source_market,
                    e.source_date,
                    e.row_count,
                    e.loaded_at,
                )
                for e in entries
                if e.source_file and str(e.source_file).strip()
            ]

            if not payload:
                return 0

            con.execute("DROP TABLE IF EXISTS tmp_finra_short_interest_sources_manual_stage")
            con.execute(
                """
                CREATE TEMP TABLE tmp_finra_short_interest_sources_manual_stage (
                    source_file VARCHAR,
                    source_market VARCHAR,
                    source_date DATE,
                    row_count BIGINT,
                    loaded_at TIMESTAMP
                )
                """
            )
            con.executemany(
                """
                INSERT INTO tmp_finra_short_interest_sources_manual_stage (
                    source_file,
                    source_market,
                    source_date,
                    row_count,
                    loaded_at
                )
                VALUES (?, ?, ?, ?, ?)
                """,
                payload,
            )

            con.execute(
                f"""
                DELETE FROM {FINRA_SHORT_INTEREST_SOURCES} t
                USING tmp_finra_short_interest_sources_manual_stage s
                WHERE COALESCE(t.source_file, '') = COALESCE(s.source_file, '')
                  AND COALESCE(t.source_market, '') = COALESCE(s.source_market, '')
                  AND t.source_date = s.source_date
                """
            )

            con.execute(
                f"""
                INSERT INTO {FINRA_SHORT_INTEREST_SOURCES} (
                    source_file,
                    source_market,
                    source_date,
                    row_count,
                    loaded_at
                )
                SELECT
                    source_file,
                    source_market,
                    source_date,
                    row_count,
                    loaded_at
                FROM tmp_finra_short_interest_sources_manual_stage
                """
            )

            con.execute("DROP TABLE IF EXISTS tmp_finra_short_interest_sources_manual_stage")
            return len(payload)
        except Exception as exc:
            raise RepositoryError(f"failed to upsert short-interest sources: {exc}") from exc
