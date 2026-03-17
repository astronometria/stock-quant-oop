from __future__ import annotations

"""
DuckDB repository for FINRA short-interest data.

Design goals
------------
- keep the repository compatible with the abstract port expected by the app layer
- stay SQL-first
- avoid destructive full-refresh of history during normal incremental runs
- keep PIT-safe raw/source/history/latest layers separated
- expose a small set of helper methods for the canonical pipeline

Important note
--------------
This repository intentionally does NOT filter by the *current* market universe when
reading historical raw short-interest data. Filtering history by today's universe would
create survivor bias in quant research.
"""

from dataclasses import asdict
from datetime import datetime
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


class DuckDbShortInterestRepository(ShortInterestRepositoryPort):
    """
    Incremental FINRA short-interest repository.

    This class is the canonical repository implementation used by:
    - raw short-interest source loading
    - canonical short-interest history build
    - latest snapshot rebuild
    """

    def __init__(self, con: Any) -> None:
        self.con = con

    # ------------------------------------------------------------------
    # Connection / helpers
    # ------------------------------------------------------------------
    def _require_connection(self) -> Any:
        if self.con is None:
            raise RepositoryError("active DB connection is required")
        return self.con

    def _norm_symbol(self, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = str(value).strip().upper()
        return normalized or None

    def _list_tables(self) -> set[str]:
        con = self._require_connection()
        rows = con.execute("SHOW TABLES").fetchall()
        return {str(row[0]).strip().lower() for row in rows}

    # ------------------------------------------------------------------
    # Required abstract-port methods
    # ------------------------------------------------------------------
    def load_raw_short_interest_records(self) -> list[RawShortInterestRecord]:
        """
        Load normalized raw short-interest rows from the staging table.

        This method is required by the abstract repository port.
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
            raise RepositoryError(
                f"failed to load raw short interest records: {exc}"
            ) from exc

    def load_included_symbols(self) -> set[str]:
        """
        Historical-safe symbol probe.

        Preference order:
        1. ticker_history / instrument_master if available
        2. market_universe included symbols as fallback for small test DBs

        We intentionally do not require current-universe membership for history
        that is already on disk. This avoids survivor bias.
        """
        try:
            con = self._require_connection()
            tables = self._list_tables()
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
                result = {row[0] for row in rows}
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
                return {row[0] for row in rows}

            return set()
        except Exception as exc:
            raise RepositoryError(f"failed to load known symbols: {exc}") from exc

    def replace_short_interest_history(self, entries: list[ShortInterestRecord]) -> int:
        """
        Backward-compatible alias required by the abstract port.

        The implementation is incremental upsert, not destructive replace.
        """
        return self.upsert_short_interest_history(entries)

    def replace_short_interest_sources(self, entries: list[ShortInterestSourceFile]) -> int:
        """
        Backward-compatible alias required by the abstract port.

        The implementation is incremental upsert, not destructive replace.
        """
        return self.upsert_short_interest_sources(entries)

    # ------------------------------------------------------------------
    # Raw-source helpers used by canonical build
    # ------------------------------------------------------------------
    def list_pending_source_files(self) -> list[ShortInterestSourceFile]:
        """
        Detect source files staged in raw but not yet represented in the
        canonical source tracking table.

        This is the key anti-noop fix:
        if raw rows exist and source metadata is missing from
        `finra_short_interest_sources`, the builder must still see work to do.
        """
        try:
            con = self._require_connection()
            rows = con.execute(
                f"""
                WITH raw_sources AS (
                    SELECT
                        source_file,
                        source_market,
                        source_date,
                        COUNT(*) AS row_count
                    FROM {FINRA_SHORT_INTEREST_SOURCE_RAW}
                    WHERE source_file IS NOT NULL
                      AND TRIM(source_file) <> ''
                      AND source_date IS NOT NULL
                    GROUP BY 1, 2, 3
                ),
                tracked_sources AS (
                    SELECT
                        source_file,
                        source_market,
                        source_date
                    FROM {FINRA_SHORT_INTEREST_SOURCES}
                )
                SELECT
                    r.source_file,
                    r.source_market,
                    r.source_date,
                    r.row_count
                FROM raw_sources r
                LEFT JOIN tracked_sources t
                  ON r.source_file = t.source_file
                 AND COALESCE(r.source_market, '') = COALESCE(t.source_market, '')
                 AND r.source_date = t.source_date
                WHERE t.source_file IS NULL
                ORDER BY r.source_date, r.source_market, r.source_file
                """
            ).fetchall()

            now = datetime.utcnow()
            return [
                ShortInterestSourceFile(
                    source_file=row[0],
                    source_market=row[1],
                    source_date=row[2],
                    row_count=int(row[3]),
                    loaded_at=now,
                )
                for row in rows
            ]
        except Exception as exc:
            raise RepositoryError(f"failed to list pending source files: {exc}") from exc

    def load_raw_short_interest_records_for_source_files(
        self,
        source_files: list[str],
    ) -> list[RawShortInterestRecord]:
        """
        Load only the raw rows associated with a selected set of staged source files.
        """
        if not source_files:
            return []

        try:
            con = self._require_connection()
            normalized_files = [str(value).strip() for value in source_files if str(value).strip()]
            if not normalized_files:
                return []

            placeholders = ", ".join(["?"] * len(normalized_files))
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
                WHERE source_file IN ({placeholders})
                ORDER BY settlement_date, symbol, source_file
                """,
                normalized_files,
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
            raise RepositoryError(
                f"failed to load raw short interest records for selected source files: {exc}"
            ) from exc

    # ------------------------------------------------------------------
    # Incremental upserts
    # ------------------------------------------------------------------
    def upsert_short_interest_history(self, entries: list[ShortInterestRecord]) -> int:
        """
        Incrementally upsert canonical history.

        Deduplication key:
        (symbol, settlement_date, source_file)

        We do not delete the whole history table. That would be both expensive and
        dangerous for reproducibility.
        """
        try:
            con = self._require_connection()

            if not entries:
                return 0

            payload = [
                (
                    self._norm_symbol(entry.symbol),
                    entry.settlement_date,
                    entry.short_interest,
                    entry.previous_short_interest,
                    entry.avg_daily_volume,
                    entry.days_to_cover,
                    entry.shares_float,
                    entry.short_interest_pct_float,
                    entry.revision_flag,
                    entry.source_market,
                    entry.source_file,
                    entry.ingested_at,
                )
                for entry in entries
                if self._norm_symbol(entry.symbol)
                and entry.settlement_date is not None
                and entry.source_file is not None
                and str(entry.source_file).strip() != ""
            ]

            if not payload:
                return 0

            stage_table = "tmp_finra_short_interest_history_stage"

            con.execute(f"DROP TABLE IF EXISTS {stage_table}")
            con.execute(
                f"""
                CREATE TEMP TABLE {stage_table} (
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

            try:
                con.executemany(
                    f"""
                    INSERT INTO {stage_table} (
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
                    DELETE FROM {FINRA_SHORT_INTEREST_HISTORY} AS target
                    USING {stage_table} AS stage
                    WHERE UPPER(TRIM(target.symbol)) = UPPER(TRIM(stage.symbol))
                      AND target.settlement_date = stage.settlement_date
                      AND COALESCE(target.source_file, '') = COALESCE(stage.source_file, '')
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
                    FROM (
                        SELECT
                            *,
                            ROW_NUMBER() OVER (
                                PARTITION BY symbol, settlement_date, source_file
                                ORDER BY ingested_at DESC NULLS LAST
                            ) AS rn
                        FROM {stage_table}
                    ) x
                    WHERE rn = 1
                    """
                )
            finally:
                con.execute(f"DROP TABLE IF EXISTS {stage_table}")

            return len(payload)
        except Exception as exc:
            raise RepositoryError(
                f"failed to upsert finra_short_interest_history: {exc}"
            ) from exc

    def upsert_short_interest_sources(self, entries: list[ShortInterestSourceFile]) -> int:
        """
        Incrementally upsert source-file tracking metadata.

        Deduplication key:
        (source_file, source_market, source_date)
        """
        try:
            con = self._require_connection()

            if not entries:
                return 0

            payload = [
                (
                    str(entry.source_file).strip(),
                    str(entry.source_market).strip().lower() if entry.source_market is not None else None,
                    entry.source_date,
                    int(entry.row_count),
                    entry.loaded_at,
                )
                for entry in entries
                if entry.source_file is not None
                and str(entry.source_file).strip() != ""
                and entry.source_date is not None
            ]

            if not payload:
                return 0

            stage_table = "tmp_finra_short_interest_sources_stage"

            con.execute(f"DROP TABLE IF EXISTS {stage_table}")
            con.execute(
                f"""
                CREATE TEMP TABLE {stage_table} (
                    source_file VARCHAR,
                    source_market VARCHAR,
                    source_date DATE,
                    row_count BIGINT,
                    loaded_at TIMESTAMP
                )
                """
            )

            try:
                con.executemany(
                    f"""
                    INSERT INTO {stage_table} (
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
                    DELETE FROM {FINRA_SHORT_INTEREST_SOURCES} AS target
                    USING {stage_table} AS stage
                    WHERE target.source_file = stage.source_file
                      AND COALESCE(target.source_market, '') = COALESCE(stage.source_market, '')
                      AND target.source_date = stage.source_date
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
                    FROM (
                        SELECT
                            *,
                            ROW_NUMBER() OVER (
                                PARTITION BY source_file, source_market, source_date
                                ORDER BY loaded_at DESC NULLS LAST
                            ) AS rn
                        FROM {stage_table}
                    ) x
                    WHERE rn = 1
                    """
                )
            finally:
                con.execute(f"DROP TABLE IF EXISTS {stage_table}")

            return len(payload)
        except Exception as exc:
            raise RepositoryError(
                f"failed to upsert finra_short_interest_sources: {exc}"
            ) from exc

    def rebuild_short_interest_latest(self) -> int:
        """
        Recompute latest snapshot from canonical history.

        This is intentionally derived from normalized history, never from raw.
        """
        try:
            con = self._require_connection()

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
                    h.symbol,
                    h.settlement_date,
                    h.short_interest,
                    h.previous_short_interest,
                    h.avg_daily_volume,
                    h.days_to_cover,
                    h.shares_float,
                    h.short_interest_pct_float,
                    h.revision_flag,
                    h.source_market,
                    h.source_file,
                    CURRENT_TIMESTAMP
                FROM {FINRA_SHORT_INTEREST_HISTORY} h
                INNER JOIN (
                    SELECT
                        symbol,
                        MAX(settlement_date) AS max_settlement_date
                    FROM {FINRA_SHORT_INTEREST_HISTORY}
                    GROUP BY symbol
                ) latest
                  ON h.symbol = latest.symbol
                 AND h.settlement_date = latest.max_settlement_date
                """
            )

            row = con.execute(
                f"SELECT COUNT(*) FROM {FINRA_SHORT_INTEREST_LATEST}"
            ).fetchone()
            return int(row[0])
        except Exception as exc:
            raise RepositoryError(
                f"failed to rebuild finra_short_interest_latest: {exc}"
            ) from exc

    # ------------------------------------------------------------------
    # Backward-compatible aliases used by older service code
    # ------------------------------------------------------------------
    def load_raw(self) -> list[dict[str, Any]]:
        """
        Legacy helper retained for compatibility with older service code.
        """
        records = self.load_raw_short_interest_records()
        result: list[dict[str, Any]] = []
        for record in records:
            result.append(
                {
                    "symbol": self._norm_symbol(record.symbol),
                    "as_of_date": record.settlement_date,
                    "settlement_date": record.settlement_date,
                    "short_interest": record.short_interest,
                    "previous_short_interest": record.previous_short_interest,
                    "avg_daily_volume": record.avg_daily_volume,
                    "shares_float": record.shares_float,
                    "source_market": record.source_market,
                    "source_file": record.source_file,
                    # Conservative PIT placeholder:
                    # for now visibility is aligned to settlement date because the
                    # current short-interest foundation has not yet introduced a
                    # separate publication calendar table.
                    "available_at": record.settlement_date,
                }
            )
        return result

    def insert_history(self, rows: list[dict[str, Any]]) -> int:
        """
        Legacy helper retained for compatibility with older service code.
        """
        entries: list[ShortInterestRecord] = []
        for row in rows:
            entries.append(
                ShortInterestRecord(
                    symbol=self._norm_symbol(row.get("symbol")) or "",
                    settlement_date=row.get("settlement_date") or row.get("as_of_date"),
                    short_interest=row.get("short_interest"),
                    previous_short_interest=row.get("previous_short_interest"),
                    avg_daily_volume=row.get("avg_daily_volume"),
                    days_to_cover=row.get("days_to_cover"),
                    shares_float=row.get("shares_float"),
                    short_interest_pct_float=row.get("short_interest_pct_float"),
                    revision_flag=row.get("revision_flag"),
                    source_market=row.get("source_market"),
                    source_file=row.get("source_file"),
                    ingested_at=row.get("ingested_at") or datetime.utcnow(),
                )
            )
        return self.upsert_short_interest_history(entries)

    def rebuild_latest(self) -> int:
        """
        Legacy alias retained for compatibility with older service code.
        """
        return self.rebuild_short_interest_latest()
