from __future__ import annotations

from stock_quant.infrastructure.db.unit_of_work import DuckDbUnitOfWork


class MasterDataService:
    def __init__(self, uow: DuckDbUnitOfWork) -> None:
        self.uow = uow

    @property
    def con(self):
        if self.uow.connection is None:
            raise RuntimeError("active DB connection is required")
        return self.uow.connection

    def rebuild_master_from_price_history(self) -> dict[str, int]:
        con = self.con

        price_symbol_count = int(
            con.repository_execute("SELECT COUNT(DISTINCT symbol) FROM price_history").fetchone()[0]
        )

        con.repository_execute("DROP TABLE IF EXISTS tmp_old_instrument_master")
        con.repository_execute("DROP TABLE IF EXISTS tmp_old_ticker_history")

        con.repository_execute(
            """
            CREATE TEMP TABLE tmp_old_instrument_master AS
            SELECT *
            FROM instrument_master
            """
        )

        con.repository_execute(
            """
            CREATE TEMP TABLE tmp_old_ticker_history AS
            SELECT *
            FROM ticker_history
            """
        )

        con.repository_execute("DELETE FROM instrument_identifier_map")
        con.repository_execute("DELETE FROM ticker_history")
        con.repository_execute("DELETE FROM instrument_master")

        con.repository_execute(
            """
            INSERT INTO instrument_master (
                instrument_id,
                company_id,
                symbol,
                exchange,
                security_type,
                share_class,
                is_active,
                created_at
            )
            WITH price_symbols AS (
                SELECT
                    UPPER(TRIM(symbol)) AS symbol,
                    MIN(price_date) AS min_price_date,
                    MAX(price_date) AS max_price_date
                FROM price_history
                GROUP BY 1
            )
            SELECT
                COALESCE(
                    o.instrument_id,
                    'INST:' || SUBSTR(MD5(ps.symbol), 1, 16)
                ) AS instrument_id,
                COALESCE(
                    o.company_id,
                    'COMP:' || SUBSTR(MD5(ps.symbol), 1, 16)
                ) AS company_id,
                ps.symbol,
                COALESCE(o.exchange, 'UNKNOWN') AS exchange,
                COALESCE(o.security_type, 'COMMON_STOCK') AS security_type,
                COALESCE(o.share_class, 'ORDINARY') AS share_class,
                CASE
                    WHEN ps.max_price_date >= CURRENT_DATE - INTERVAL 30 DAY THEN TRUE
                    ELSE COALESCE(o.is_active, FALSE)
                END AS is_active,
                CURRENT_TIMESTAMP AS created_at
            FROM price_symbols ps
            LEFT JOIN tmp_old_instrument_master o
              ON UPPER(TRIM(ps.symbol)) = UPPER(TRIM(o.symbol))
            ORDER BY ps.symbol
            """
        )

        con.repository_execute(
            """
            INSERT INTO ticker_history (
                instrument_id,
                symbol,
                exchange,
                valid_from,
                valid_to,
                is_current,
                created_at
            )
            WITH price_symbols AS (
                SELECT
                    UPPER(TRIM(ph.symbol)) AS symbol,
                    MIN(ph.price_date) AS min_price_date,
                    MAX(ph.price_date) AS max_price_date
                FROM price_history ph
                GROUP BY 1
            )
            SELECT
                im.instrument_id,
                ps.symbol,
                im.exchange,
                ps.min_price_date AS valid_from,
                NULL AS valid_to,
                TRUE AS is_current,
                CURRENT_TIMESTAMP AS created_at
            FROM price_symbols ps
            INNER JOIN instrument_master im
              ON UPPER(TRIM(ps.symbol)) = UPPER(TRIM(im.symbol))
            ORDER BY ps.symbol
            """
        )

        con.repository_execute(
            """
            INSERT INTO instrument_identifier_map (
                instrument_id,
                identifier_type,
                identifier_value,
                is_primary,
                created_at
            )
            SELECT
                instrument_id,
                'ticker' AS identifier_type,
                symbol AS identifier_value,
                TRUE AS is_primary,
                CURRENT_TIMESTAMP AS created_at
            FROM instrument_master
            """
        )

        instrument_master_rows = int(
            con.repository_execute("SELECT COUNT(*) FROM instrument_master").fetchone()[0]
        )
        ticker_history_rows = int(
            con.repository_execute("SELECT COUNT(*) FROM ticker_history").fetchone()[0]
        )
        identifier_map_rows = int(
            con.repository_execute("SELECT COUNT(*) FROM instrument_identifier_map").fetchone()[0]
        )

        return {
            "price_symbol_count": price_symbol_count,
            "instrument_master_rows": instrument_master_rows,
            "ticker_history_rows": ticker_history_rows,
            "identifier_map_rows": identifier_map_rows,
        }
