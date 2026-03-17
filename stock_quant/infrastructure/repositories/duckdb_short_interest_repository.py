from __future__ import annotations

"""
Incremental DuckDB repository for FINRA short interest.

Design goals:
- no destructive full refresh of history/source_raw during normal runs
- process only unloaded source_file values
- keep latest derived from history
- preserve compatibility with the existing normalized FINRA schema
"""

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
    def __init__(self, con: Any) -> None:
        self._con = con

    def _require_connection(self):
        if self._con is None:
            raise RepositoryError("active DB connection is required")
        return self._con

    def _norm_symbol(self, value: str | None) -> str:
        return str(value or "").strip().upper()

    def _list_tables(self) -> set[str]:
        con = self._require_connection()
        rows = con.execute("SHOW TABLES").fetchall()
        return {str(row[0]).strip().lower() for row in rows}

    def list_loaded_source_files(self) -> set[str]:
        """
        Source files déjà normalisés dans finra_short_interest_sources.
        """
        con = self._require_connection()
        try:
            rows = con.execute(
                f"""
                SELECT DISTINCT TRIM(source_file) AS source_file
                FROM {FINRA_SHORT_INTEREST_SOURCES}
                WHERE source_file IS NOT NULL
                  AND TRIM(source_file) <> ''
                """
            ).fetchall()
            return {str(row[0]).strip() for row in rows if row[0]}
        except Exception as exc:
            raise RepositoryError(f"failed to list loaded source files: {exc}") from exc

    def list_available_raw_source_files(self) -> set[str]:
        """
        Source files présents dans la raw table.
        """
        con = self._require_connection()
        try:
            rows = con.execute(
                f"""
                SELECT DISTINCT TRIM(source_file) AS source_file
                FROM {FINRA_SHORT_INTEREST_SOURCE_RAW}
                WHERE source_file IS NOT NULL
                  AND TRIM(source_file) <> ''
                """
            ).fetchall()
            return {str(row[0]).strip() for row in rows if row[0]}
        except Exception as exc:
            raise RepositoryError(f"failed to list available raw source files: {exc}") from exc

    def list_pending_source_files(self) -> list[str]:
        """
        Détecte les raw source files non encore chargés dans la couche normalisée.
        """
        available = self.list_available_raw_source_files()
        loaded = self.list_loaded_source_files()
        pending = sorted(available - loaded)
        return pending

    def load_raw_short_interest_records_for_source_files(
        self,
        source_files: list[str],
    ) -> list[RawShortInterestRecord]:
        con = self._require_connection()

        if not source_files:
            return []

        try:
            placeholders = ", ".join(["?"] * len(source_files))
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
                source_files,
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
                f"failed to load raw short interest records for source files: {exc}"
            ) from exc

    def upsert_short_interest_history(self, entries: list[ShortInterestRecord]) -> int:
        con = self._require_connection()

        try:
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
                and str(entry.source_file or "").strip() != ""
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
                    ) stage_dedup
                    WHERE rn = 1
                    """
                )
            finally:
                con.execute(f"DROP TABLE IF EXISTS {stage_table}")

            return len(payload)

        except Exception as exc:
            raise RepositoryError(f"failed to upsert short interest history: {exc}") from exc

    def upsert_short_interest_sources(self, entries: list[ShortInterestSourceFile]) -> int:
        con = self._require_connection()

        try:
            if not entries:
                return 0

            payload = [
                (
                    str(entry.source_file or "").strip(),
                    str(entry.source_market or "unknown").strip().lower(),
                    entry.source_date,
                    int(entry.row_count or 0),
                    entry.loaded_at,
                )
                for entry in entries
                if str(entry.source_file or "").strip() != ""
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
                    WHERE COALESCE(target.source_file, '') = COALESCE(stage.source_file, '')
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
                                PARTITION BY source_file
                                ORDER BY loaded_at DESC NULLS LAST
                            ) AS rn
                        FROM {stage_table}
                    ) stage_dedup
                    WHERE rn = 1
                    """
                )
            finally:
                con.execute(f"DROP TABLE IF EXISTS {stage_table}")

            return len(payload)

        except Exception as exc:
            raise RepositoryError(f"failed to upsert short interest sources: {exc}") from exc

    def rebuild_short_interest_latest(self) -> int:
        con = self._require_connection()

        try:
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

            row_count = con.execute(
                f"SELECT COUNT(*) FROM {FINRA_SHORT_INTEREST_LATEST}"
            ).fetchone()[0]

            return int(row_count)

        except Exception as exc:
            raise RepositoryError(f"failed to rebuild short interest latest: {exc}") from exc
