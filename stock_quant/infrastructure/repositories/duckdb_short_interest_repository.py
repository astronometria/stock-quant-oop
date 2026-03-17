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
from stock_quant.shared.exceptions import RepositoryError


class DuckDbShortInterestRepository(ShortInterestRepositoryPort):
    """
    Incremental FINRA short-interest repository.

    Notes importantes :
    - cette classe reste compatible avec l'ancien contrat abstrait
    - on ajoute les helpers de comptage/date nécessaires au nouveau pipeline
    - on garde une logique SQL-first simple et explicite
    """

    def __init__(self, con: Any) -> None:
        self.con = con

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _require_connection(self):
        if self.con is None:
            raise RepositoryError("active DB connection is required")
        return self.con

    def _scalar(self, sql: str, params: list[Any] | None = None, default: Any = 0) -> Any:
        """
        Petit helper centralisé pour limiter le bruit répétitif et garder
        les accès SQL lisibles.
        """
        con = self._require_connection()
        row = con.execute(sql, params or []).fetchone()
        if row is None:
            return default
        value = row[0]
        return default if value is None else value

    def _norm_symbol(self, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = str(value).strip().upper()
        return normalized or None

    def _list_tables(self) -> set[str]:
        con = self._require_connection()
        return {
            str(row[0]).strip().lower()
            for row in con.execute("SHOW TABLES").fetchall()
        }

    # ------------------------------------------------------------------
    # Pipeline state helpers required by BuildShortInterestPipeline
    # ------------------------------------------------------------------
    def get_raw_row_count(self) -> int:
        return int(self._scalar(f"SELECT COUNT(*) FROM {FINRA_SHORT_INTEREST_SOURCE_RAW}", default=0))

    def get_source_row_count(self) -> int:
        return int(self._scalar(f"SELECT COUNT(*) FROM {FINRA_SHORT_INTEREST_SOURCES}", default=0))

    def get_history_row_count(self) -> int:
        return int(self._scalar(f"SELECT COUNT(*) FROM {FINRA_SHORT_INTEREST_HISTORY}", default=0))

    def get_latest_row_count(self) -> int:
        return int(self._scalar(f"SELECT COUNT(*) FROM {FINRA_SHORT_INTEREST_LATEST}", default=0))

    def get_max_raw_source_date(self):
        return self._scalar(
            f"SELECT MAX(source_date) FROM {FINRA_SHORT_INTEREST_SOURCE_RAW}",
            default=None,
        )

    def get_max_history_settlement_date(self):
        return self._scalar(
            f"SELECT MAX(settlement_date) FROM {FINRA_SHORT_INTEREST_HISTORY}",
            default=None,
        )

    def get_max_latest_settlement_date(self):
        return self._scalar(
            f"SELECT MAX(settlement_date) FROM {FINRA_SHORT_INTEREST_LATEST}",
            default=None,
        )

    # ------------------------------------------------------------------
    # Abstract contract compatibility
    # ------------------------------------------------------------------
    def load_raw_short_interest_records(self) -> list[RawShortInterestRecord]:
        """
        Méthode exigée par le port abstrait.
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
            raise RepositoryError(f"failed to load raw short interest records: {exc}") from exc

    def load_included_symbols(self) -> set[str]:
        """
        Historical-safe symbol probe.

        Preference order:
        1. ticker_history / instrument_master
        2. market_universe comme fallback
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
        Alias backward-compatible exigé par le port abstrait.
        """
        return self.upsert_short_interest_history(entries)

    def replace_short_interest_sources(self, entries: list[ShortInterestSourceFile]) -> int:
        """
        Alias backward-compatible exigé par le port abstrait.
        """
        return self.upsert_short_interest_sources(entries)

    # ------------------------------------------------------------------
    # Raw/staging helpers used by service/pipeline
    # ------------------------------------------------------------------
    def load_raw(self) -> list[dict[str, Any]]:
        """
        Retourne un format dict simple pour le service actuel.
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
                    days_to_cover,
                    shares_float,
                    revision_flag,
                    source_market,
                    source_file,
                    source_date,
                    ingested_at
                FROM {FINRA_SHORT_INTEREST_SOURCE_RAW}
                ORDER BY settlement_date, symbol, source_file
                """
            ).fetchall()

            result: list[dict[str, Any]] = []
            for row in rows:
                result.append(
                    {
                        "symbol": row[0],
                        "as_of_date": row[1],
                        "settlement_date": row[1],
                        "short_interest": row[2],
                        "previous_short_interest": row[3],
                        "avg_daily_volume": row[4],
                        "days_to_cover": row[5],
                        "shares_float": row[6],
                        "revision_flag": row[7],
                        "source_market": row[8],
                        "source_file": row[9],
                        "source_date": row[10],
                        "available_at": row[11],
                    }
                )
            return result
        except Exception as exc:
            raise RepositoryError(f"failed to load raw short-interest rows: {exc}") from exc

    def insert_history(self, normalized_rows: list[dict[str, Any]]) -> int:
        """
        Insert/upsert format simplifié utilisé par le service actuel.
        """
        entries: list[ShortInterestRecord] = []

        for row in normalized_rows:
            symbol = self._norm_symbol(row.get("symbol"))
            if not symbol:
                continue

            settlement_date = row.get("settlement_date") or row.get("as_of_date")
            if settlement_date is None:
                continue

            short_interest = row.get("short_interest")
            if short_interest is None:
                continue

            previous_short_interest = row.get("previous_short_interest")
            avg_daily_volume = row.get("avg_daily_volume") or 0.0
            days_to_cover = row.get("days_to_cover")
            shares_float = row.get("shares_float")
            revision_flag = row.get("revision_flag")
            source_market = row.get("source_market") or "unknown"
            source_file = row.get("source_file") or "unknown"
            ingested_at = row.get("available_at")

            short_interest_pct_float = None
            if shares_float not in (None, 0):
                try:
                    short_interest_pct_float = float(short_interest) / float(shares_float)
                except Exception:
                    short_interest_pct_float = None

            entries.append(
                ShortInterestRecord(
                    symbol=symbol,
                    settlement_date=settlement_date,
                    short_interest=int(short_interest),
                    previous_short_interest=int(previous_short_interest or 0),
                    avg_daily_volume=float(avg_daily_volume or 0.0),
                    days_to_cover=days_to_cover,
                    shares_float=shares_float,
                    short_interest_pct_float=short_interest_pct_float,
                    revision_flag=revision_flag,
                    source_market=source_market,
                    source_file=source_file,
                    ingested_at=ingested_at,
                )
            )

        return self.upsert_short_interest_history(entries)

    def rebuild_latest(self) -> int:
        return self.rebuild_short_interest_latest()

    # ------------------------------------------------------------------
    # Incremental upserts
    # ------------------------------------------------------------------
    def upsert_short_interest_history(self, entries: list[ShortInterestRecord]) -> int:
        try:
            con = self._require_connection()
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
            con.execute(f"DROP TABLE IF EXISTS {stage_table}")
            con.execute(
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
            raise RepositoryError(f"failed to upsert finra_short_interest_history: {exc}") from exc

    def upsert_short_interest_sources(self, entries: list[ShortInterestSourceFile]) -> int:
        try:
            con = self._require_connection()
            if not entries:
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
                payload = [
                    (
                        entry.source_file,
                        entry.source_market,
                        entry.source_date,
                        entry.row_count,
                        entry.loaded_at,
                    )
                    for entry in entries
                    if entry.source_file is not None and str(entry.source_file).strip() != ""
                ]
                if not payload:
                    return 0

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
                return len(payload)
            finally:
                con.execute(f"DROP TABLE IF EXISTS {stage_table}")
        except Exception as exc:
            raise RepositoryError(f"failed to upsert finra_short_interest_sources: {exc}") from exc

    def rebuild_short_interest_latest(self) -> int:
        """
        Recompute latest snapshot from history without touching history.
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
            return int(self._scalar(f"SELECT COUNT(*) FROM {FINRA_SHORT_INTEREST_LATEST}", default=0))
        except Exception as exc:
            raise RepositoryError(f"failed to rebuild finra_short_interest_latest: {exc}") from exc
