from __future__ import annotations

from typing import Any

from stock_quant.domain.entities.prices import PriceBar, RawPriceBar
from stock_quant.domain.ports.repositories import PriceRepositoryPort
from stock_quant.infrastructure.db.unit_of_work import DuckDbUnitOfWork
from stock_quant.shared.exceptions import RepositoryError


class DuckDbPriceRepository(PriceRepositoryPort):
    """
    Incremental DuckDB repository for canonical price storage.

    Design goals:
    - no full-table delete on price_history during incremental updates
    - preserve legacy public API used by pipelines/tests
    - remain compatible with current symbol-based schema
    - support future instrument/company identifiers when present
    """

    def __init__(self, uow: DuckDbUnitOfWork) -> None:
        self.uow = uow

    @property
    def con(self):
        if self.uow.connection is None:
            raise RepositoryError("active DB connection is required")
        return self.uow.connection

    # ------------------------------------------------------------------
    # Incremental API
    # ------------------------------------------------------------------

    def list_allowed_symbols(self) -> list[str]:
        try:
            tables = self._list_tables()

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
                return [row[0] for row in rows]

            if "universe_membership_history" in tables:
                rows = self.con.execute(
                    """
                    SELECT DISTINCT UPPER(TRIM(symbol)) AS symbol
                    FROM universe_membership_history
                    WHERE membership_status IN ('included', 'active', 'eligible')
                      AND (effective_to IS NULL OR effective_to >= CURRENT_DATE)
                      AND symbol IS NOT NULL
                      AND TRIM(symbol) <> ''
                    ORDER BY symbol
                    """
                ).fetchall()
                return [row[0] for row in rows]

            return sorted(self.load_included_symbols())
        except Exception as exc:
            raise RepositoryError(f"failed to list allowed symbols: {exc}") from exc

    def upsert_price_history(self, frame) -> int:
        try:
            if frame is None or frame.empty:
                return 0

            self.con.register("tmp_price_history_input_frame", frame)
            try:
                self._create_stage_table()
                self._delete_replaced_price_history_rows()
                written = self._insert_price_history_from_stage()
            finally:
                self._safe_unregister("tmp_price_history_input_frame")
                self._drop_temp_table("tmp_price_history_stage")

            return int(written)
        except Exception as exc:
            raise RepositoryError(f"failed to upsert price_history: {exc}") from exc

    def refresh_price_latest(self, symbols: list[str] | None = None) -> int:
        try:
            latest_columns = self._table_columns("price_latest")
            history_columns = self._table_columns("price_history")
            if not latest_columns:
                raise RepositoryError("price_latest table is missing")
            if not history_columns:
                raise RepositoryError("price_history table is missing")

            if symbols:
                normalized_symbols = sorted(
                    {self._norm_symbol(symbol) for symbol in symbols if self._norm_symbol(symbol)}
                )
                if not normalized_symbols:
                    return self._count_rows("price_latest")

                self.con.execute("CREATE TEMP TABLE tmp_price_latest_symbols(symbol VARCHAR)")
                try:
                    values_sql = ", ".join(["(?)"] * len(normalized_symbols))
                    self.con.execute(
                        f"INSERT INTO tmp_price_latest_symbols VALUES {values_sql}",
                        normalized_symbols,
                    )

                    self.con.execute(
                        """
                        DELETE FROM price_latest
                        WHERE UPPER(TRIM(symbol)) IN (
                            SELECT UPPER(TRIM(symbol))
                            FROM tmp_price_latest_symbols
                        )
                        """
                    )

                    insert_sql = self._build_price_latest_insert_sql(
                        latest_columns=latest_columns,
                        history_columns=history_columns,
                        symbol_filter_sql="""
                            WHERE UPPER(TRIM(h.symbol)) IN (
                                SELECT UPPER(TRIM(symbol))
                                FROM tmp_price_latest_symbols
                            )
                        """,
                    )
                    self.con.execute(insert_sql)
                finally:
                    self._drop_temp_table("tmp_price_latest_symbols")
            else:
                self.con.execute("DELETE FROM price_latest")
                insert_sql = self._build_price_latest_insert_sql(
                    latest_columns=latest_columns,
                    history_columns=history_columns,
                    symbol_filter_sql="",
                )
                self.con.execute(insert_sql)

            return self._count_rows("price_latest")
        except Exception as exc:
            raise RepositoryError(f"failed to refresh price_latest: {exc}") from exc

    # ------------------------------------------------------------------
    # Backward-compatible public API
    # ------------------------------------------------------------------

    def load_raw_price_bars(self) -> list[RawPriceBar]:
        try:
            rows = self.con.execute(
                """
                SELECT
                    symbol,
                    price_date,
                    open,
                    high,
                    low,
                    close,
                    volume,
                    source_name
                FROM price_source_daily_raw
                ORDER BY symbol, price_date
                """
            ).fetchall()
            return [
                RawPriceBar(
                    symbol=row[0],
                    price_date=row[1],
                    open=row[2],
                    high=row[3],
                    low=row[4],
                    close=row[5],
                    volume=row[6],
                    source_name=row[7] or "unknown",
                )
                for row in rows
            ]
        except Exception as exc:
            raise RepositoryError(f"failed to load raw price bars: {exc}") from exc

    def load_included_symbols(self) -> set[str]:
        """
        Historical-safe symbol probe.

        Preference order:
        1. ticker_history / instrument_master when present
        2. market_universe included symbols as fallback for minimal test DBs
        """
        try:
            rows: list[tuple[str]] = []
            tables = self._list_tables()

            if "ticker_history" in tables or "instrument_master" in tables:
                union_parts: list[str] = []
                if "ticker_history" in tables:
                    union_parts.append("SELECT symbol FROM ticker_history")
                if "instrument_master" in tables:
                    union_parts.append("SELECT symbol FROM instrument_master")

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

            if not rows and "market_universe" in tables:
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
        except Exception as exc:
            raise RepositoryError(f"failed to load included symbols: {exc}") from exc

    def replace_price_history(self, entries: list[PriceBar]) -> int:
        try:
            if not entries:
                return 0

            import pandas as pd

            frame = pd.DataFrame(
                [
                    {
                        "symbol": self._norm_symbol(e.symbol),
                        "price_date": e.price_date,
                        "open": e.open,
                        "high": e.high,
                        "low": e.low,
                        "close": e.close,
                        "volume": e.volume,
                        "source_name": e.source_name,
                        "ingested_at": e.ingested_at,
                    }
                    for e in entries
                    if self._norm_symbol(e.symbol) and e.price_date is not None
                ]
            )
            return self.upsert_price_history(frame)
        except Exception as exc:
            raise RepositoryError(f"failed to replace price_history: {exc}") from exc

    def rebuild_price_latest(self) -> int:
        return self.refresh_price_latest(symbols=None)

    def load_price_source_daily_raw_rows(self) -> list[dict[str, Any]]:
        try:
            rows = self.con.execute(
                """
                SELECT
                    symbol,
                    price_date,
                    open,
                    high,
                    low,
                    close,
                    volume,
                    source_name,
                    ingested_at
                FROM price_source_daily_raw
                ORDER BY symbol, price_date
                """
            ).fetchall()

            return [
                {
                    "symbol": row[0],
                    "price_date": row[1],
                    "open": row[2],
                    "high": row[3],
                    "low": row[4],
                    "close": row[5],
                    "volume": row[6],
                    "source_name": row[7],
                    "ingested_at": row[8],
                }
                for row in rows
            ]
        except Exception as exc:
            raise RepositoryError(f"failed to load price_source_daily_raw rows: {exc}") from exc

    def load_price_source_daily_raw_all_rows(self) -> list[dict[str, Any]]:
        try:
            rows = self.con.execute(
                """
                SELECT
                    symbol,
                    price_date,
                    open,
                    high,
                    low,
                    close,
                    volume,
                    source_name,
                    source_path,
                    asset_class,
                    venue_group,
                    ingested_at
                FROM price_source_daily_raw_all
                ORDER BY symbol, price_date, source_name
                """
            ).fetchall()

            return [
                {
                    "symbol": row[0],
                    "price_date": row[1],
                    "open": row[2],
                    "high": row[3],
                    "low": row[4],
                    "close": row[5],
                    "volume": row[6],
                    "source_name": row[7],
                    "source_path": row[8],
                    "asset_class": row[9],
                    "venue_group": row[10],
                    "ingested_at": row[11],
                }
                for row in rows
            ]
        except Exception as exc:
            raise RepositoryError(f"failed to load price_source_daily_raw_all rows: {exc}") from exc

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _create_stage_table(self) -> None:
        price_history_columns = self._table_columns("price_history")
        if not price_history_columns:
            raise RepositoryError("price_history table is missing")

        instrument_master_columns = self._table_columns("instrument_master")
        has_instrument_master = bool(instrument_master_columns)

        join_sql = ""
        select_instrument_sql = "CAST(NULL AS VARCHAR) AS instrument_id,"
        select_company_sql = "CAST(NULL AS VARCHAR) AS company_id,"

        if has_instrument_master:
            select_instrument_sql = "im.instrument_id AS instrument_id,"
            if "company_id" in instrument_master_columns:
                select_company_sql = "im.company_id AS company_id,"
            else:
                select_company_sql = "CAST(NULL AS VARCHAR) AS company_id,"

            join_sql = """
                LEFT JOIN instrument_master im
                  ON UPPER(TRIM(d.symbol)) = UPPER(TRIM(im.symbol))
            """

        self.con.execute(
            f"""
            CREATE TEMP TABLE tmp_price_history_stage AS
            WITH src AS (
                SELECT
                    UPPER(TRIM(symbol)) AS symbol,
                    CAST(price_date AS DATE) AS price_date,
                    CAST(open AS DOUBLE) AS open,
                    CAST(high AS DOUBLE) AS high,
                    CAST(low AS DOUBLE) AS low,
                    CAST(close AS DOUBLE) AS close,
                    CAST(volume AS BIGINT) AS volume,
                    CAST(source_name AS VARCHAR) AS source_name,
                    CAST(ingested_at AS TIMESTAMP) AS ingested_at
                FROM tmp_price_history_input_frame
                WHERE symbol IS NOT NULL
                  AND TRIM(symbol) <> ''
                  AND price_date IS NOT NULL
            ),
            dedup AS (
                SELECT *
                FROM (
                    SELECT
                        src.*,
                        ROW_NUMBER() OVER (
                            PARTITION BY src.symbol, src.price_date
                            ORDER BY src.ingested_at DESC NULLS LAST, src.source_name DESC
                        ) AS rn
                    FROM src
                ) x
                WHERE rn = 1
            )
            SELECT
                {select_instrument_sql}
                {select_company_sql}
                d.symbol,
                d.price_date,
                d.open,
                d.high,
                d.low,
                d.close,
                d.volume,
                d.source_name,
                COALESCE(d.ingested_at, CURRENT_TIMESTAMP) AS ingested_at
            FROM dedup d
            {join_sql}
            """
        )

    def _delete_replaced_price_history_rows(self) -> None:
        columns = self._table_columns("price_history")

        if "instrument_id" in columns:
            self.con.execute(
                """
                DELETE FROM price_history AS target
                USING tmp_price_history_stage AS stage
                WHERE target.price_date = stage.price_date
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
            )
            return

        self.con.execute(
            """
            DELETE FROM price_history AS target
            USING tmp_price_history_stage AS stage
            WHERE target.price_date = stage.price_date
              AND UPPER(TRIM(target.symbol)) = UPPER(TRIM(stage.symbol))
            """
        )

    def _insert_price_history_from_stage(self) -> int:
        target_columns = self._table_columns("price_history")
        if not target_columns:
            raise RepositoryError("price_history table is missing")

        insert_columns: list[str] = []
        select_columns: list[str] = []

        for name in [
            "instrument_id",
            "company_id",
            "symbol",
            "price_date",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "source_name",
            "ingested_at",
        ]:
            if name in target_columns:
                insert_columns.append(name)
                select_columns.append(name)

        if not insert_columns:
            raise RepositoryError("price_history has no supported columns")

        self.con.execute(
            f"""
            INSERT INTO price_history ({", ".join(insert_columns)})
            SELECT {", ".join(select_columns)}
            FROM tmp_price_history_stage
            """
        )
        return self._count_rows("tmp_price_history_stage")

    def _build_price_latest_insert_sql(
        self,
        latest_columns: set[str],
        history_columns: set[str],
        symbol_filter_sql: str,
    ) -> str:
        insert_columns: list[str] = []
        select_columns: list[str] = []

        mapping = [
            ("instrument_id", "h.instrument_id", "instrument_id" in history_columns),
            ("company_id", "h.company_id", "company_id" in history_columns),
            ("symbol", "h.symbol", "symbol" in history_columns),
            ("latest_price_date", "h.price_date", "price_date" in history_columns),
            ("close", "h.close", "close" in history_columns),
            ("volume", "h.volume", "volume" in history_columns),
            ("source_name", "h.source_name", "source_name" in history_columns),
            ("updated_at", "CURRENT_TIMESTAMP", True),
        ]

        for target_name, expr, allowed in mapping:
            if target_name in latest_columns and allowed:
                insert_columns.append(target_name)
                select_columns.append(expr)

        if not insert_columns:
            raise RepositoryError("price_latest has no supported columns")

        partition_expr = "h.symbol"
        if "instrument_id" in history_columns:
            partition_expr = "COALESCE(h.instrument_id, h.symbol)"

        return f"""
            INSERT INTO price_latest ({", ".join(insert_columns)})
            WITH ranked AS (
                SELECT
                    h.*,
                    ROW_NUMBER() OVER (
                        PARTITION BY {partition_expr}
                        ORDER BY h.price_date DESC, h.ingested_at DESC NULLS LAST
                    ) AS rn
                FROM price_history h
                {symbol_filter_sql}
            )
            SELECT
                {", ".join(select_columns)}
            FROM ranked h
            WHERE h.rn = 1
        """

    def _norm_symbol(self, value: Any) -> str:
        if value is None:
            return ""
        return str(value).strip().upper()

    def _table_columns(self, table_name: str) -> set[str]:
        try:
            rows = self.con.execute(f"PRAGMA table_info('{table_name}')").fetchall()
            return {row[1] for row in rows}
        except Exception:
            return set()

    def _list_tables(self) -> set[str]:
        rows = self.con.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'main'
            """
        ).fetchall()
        return {row[0] for row in rows}

    def _count_rows(self, table_name: str) -> int:
        return int(self.con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0])

    def _drop_temp_table(self, table_name: str) -> None:
        try:
            self.con.execute(f"DROP TABLE IF EXISTS {table_name}")
        except Exception:
            pass

    def _safe_unregister(self, name: str) -> None:
        try:
            self.con.unregister(name)
        except Exception:
            pass
