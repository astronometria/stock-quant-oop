from __future__ import annotations

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
from stock_quant.infrastructure.db.unit_of_work import DuckDbUnitOfWork
from stock_quant.shared.exceptions import RepositoryError


class DuckDbShortInterestRepository(ShortInterestRepositoryPort):
    """
    Incremental FINRA short-interest repository.

    Design goals:
    - never full-delete history/source tables during regular refresh
    - keep legacy tables compatible while improving PIT safety
    - avoid current-universe filtering to reduce survivor bias
    - preserve existing method names for pipeline compatibility
    """

    def __init__(self, uow: DuckDbUnitOfWork) -> None:
        self.uow = uow

    @property
    def con(self):
        if self.uow.connection is None:
            raise RepositoryError("active DB connection is required")
        return self.uow.connection

    def load_raw_short_interest_records(self) -> list[RawShortInterestRecord]:
        try:
            rows = self.con.execute(
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
            raise RepositoryError(f"failed to load raw short interest records: {exc}") from exc

    def load_included_symbols(self) -> set[str]:
        """
        Historical-safe symbol probe.

        Preference order:
        1. ticker_history / instrument_master if available
        2. market_universe included symbols as fallback for minimal test DBs
        """
        try:
            tables = self._list_tables()
            union_parts: list[str] = []

            if "ticker_history" in tables:
                union_parts.append("SELECT symbol FROM ticker_history")
            if "instrument_master" in tables:
                union_parts.append("SELECT symbol FROM instrument_master")

            if union_parts:
                rows = self.con.execute(
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
                rows = self.con.execute(
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

    # -------------------------------------------------------------------------
    # Backward-compatible public API
    # -------------------------------------------------------------------------

    def replace_short_interest_history(self, entries: list[ShortInterestRecord]) -> int:
        return self.upsert_short_interest_history(entries)

    def replace_short_interest_sources(self, entries: list[ShortInterestSourceFile]) -> int:
        return self.upsert_short_interest_sources(entries)

    # -------------------------------------------------------------------------
    # Incremental upserts
    # -------------------------------------------------------------------------

    def upsert_short_interest_history(self, entries: list[ShortInterestRecord]) -> int:
        try:
            if not entries:
                return 0

            payload = [
                (
                    self._norm_symbol(e.symbol),
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
                if self._norm_symbol(e.symbol)
                and e.settlement_date is not None
                and e.source_file is not None
                and str(e.source_file).strip() != ""
            ]
            if not payload:
                return 0

            stage_table = "tmp_finra_short_interest_history_stage"
            self.con.execute(f"DROP TABLE IF EXISTS {stage_table}")
            self.con.execute(
                f"""
                CREATE TEMP TABLE {stage_table} (
                    symbol VARCHAR,
                    settlement_date DATE,
                    short_interest DOUBLE,
                    previous_short_interest DOUBLE,
                    avg_daily_volume DOUBLE,
                    days_to_cover DOUBLE,
                    shares_float DOUBLE,
                    short_interest_pct_float DOUBLE,
                    revision_flag VARCHAR,
                    source_market VARCHAR,
                    source_file VARCHAR,
                    ingested_at TIMESTAMP
                )
                """
            )
            try:
                self.con.executemany(
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

                self.con.execute(
                    f"""
                    DELETE FROM {FINRA_SHORT_INTEREST_HISTORY} AS target
                    USING {stage_table} AS stage
                    WHERE UPPER(TRIM(target.symbol)) = UPPER(TRIM(stage.symbol))
                      AND target.settlement_date = stage.settlement_date
                      AND COALESCE(target.source_file, '') = COALESCE(stage.source_file, '')
                    """
                )

                self.con.execute(
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
                self.con.execute(f"DROP TABLE IF EXISTS {stage_table}")

            return len(payload)
        except Exception as exc:
            raise RepositoryError(f"failed to upsert finra_short_interest_history: {exc}") from exc

    def rebuild_short_interest_latest(self) -> int:
        """
        Kept for backward compatibility.
        Recomputes latest snapshot table from history without touching history.
        """
        try:
            self.con.execute(f"DELETE FROM {FINRA_SHORT_INTEREST_LATEST}")
            self.con.execute(
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
            count = self.con.execute(
                f"SELECT COUNT(*) FROM {FINRA_SHORT_INTEREST_LATEST}"
            ).fetchone()[0]
            return int(count)
        except Exception as exc:
            raise RepositoryError(f"failed to rebuild finra_short_interest_latest: {exc}") from exc

    def upsert_short_interest_sources(self, entries: list[ShortInterestSourceFile]) -> int:
        try:
            if not entries:
                return 0

            rows = [
                (
                    str(e.source_file).strip(),
                    e.source_market,
                    e.source_date,
                    e.row_count,
                    e.loaded_at,
                )
                for e in entries
                if e.source_file is not None and str(e.source_file).strip() != ""
            ]
            if not rows:
                return 0

            stage_table = "tmp_finra_short_interest_sources_stage"
            self.con.execute(f"DROP TABLE IF EXISTS {stage_table}")
            self.con.execute(
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
                self.con.executemany(
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
                    rows,
                )

                self.con.execute(
                    f"""
                    DELETE FROM {FINRA_SHORT_INTEREST_SOURCES} AS target
                    USING {stage_table} AS stage
                    WHERE COALESCE(target.source_file, '') = COALESCE(stage.source_file, '')
                    """
                )

                self.con.execute(
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
                                PARTITION BY source_file
                                ORDER BY loaded_at DESC NULLS LAST, source_date DESC NULLS LAST
                            ) AS rn
                        FROM {stage_table}
                    ) x
                    WHERE rn = 1
                    """
                )
            finally:
                self.con.execute(f"DROP TABLE IF EXISTS {stage_table}")

            return len(rows)
        except Exception as exc:
            raise RepositoryError(f"failed to upsert finra_short_interest_sources: {exc}") from exc

    # -------------------------------------------------------------------------
    # Helpers
    # -------------------------------------------------------------------------

    def _norm_symbol(self, value: Any) -> str:
        if value is None:
            return ""
        return str(value).strip().upper()

    def _list_tables(self) -> set[str]:
        rows = self.con.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'main'
            """
        ).fetchall()
        return {row[0] for row in rows}
