from __future__ import annotations


class MasterDataService:
    def __init__(self, uow) -> None:
        self.uow = uow

    @property
    def con(self):
        if self.uow.connection is None:
            raise RuntimeError("active DB connection is required")
        return self.uow.connection

    def rebuild_master_from_price_history(self) -> dict[str, int]:
        con = self.con

        price_symbol_count = int(
            con.execute(
                "SELECT COUNT(DISTINCT symbol) FROM price_bars_adjusted"
            ).fetchone()[0]
        )

        con.execute("DROP TABLE IF EXISTS tmp_old_instrument_master")
        con.execute("DROP TABLE IF EXISTS tmp_old_ticker_history")
        con.execute("DROP TABLE IF EXISTS tmp_price_symbol_bounds")

        con.execute(
            """
            CREATE TEMP TABLE tmp_old_instrument_master AS
            SELECT *
            FROM instrument_master
            """
        )
        con.execute(
            """
            CREATE TEMP TABLE tmp_old_ticker_history AS
            SELECT *
            FROM ticker_history
            """
        )
        con.execute(
            """
            CREATE TEMP TABLE tmp_price_symbol_bounds AS
            SELECT
                p.symbol,
                p.instrument_id,
                MIN(p.bar_date) AS min_bar_date,
                MAX(p.bar_date) AS max_bar_date
            FROM price_bars_adjusted p
            GROUP BY p.symbol, p.instrument_id
            """
        )

        con.execute("DELETE FROM instrument_identifier_map")
        con.execute("DELETE FROM ticker_history")
        con.execute("DELETE FROM instrument_master")

        # 1) Instruments pilotés par les ids historiques déjà présents dans les prix
        con.execute(
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
            SELECT
                b.instrument_id,
                o.company_id,
                b.symbol,
                COALESCE(o.exchange, t.exchange),
                COALESCE(o.security_type, 'COMMON_STOCK'),
                o.share_class,
                TRUE AS is_active,
                CURRENT_TIMESTAMP AS created_at
            FROM tmp_price_symbol_bounds b
            LEFT JOIN tmp_old_instrument_master o
                ON o.symbol = b.symbol
            LEFT JOIN tmp_old_ticker_history t
                ON t.symbol = b.symbol
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY b.symbol, b.instrument_id
                ORDER BY
                    CASE WHEN o.instrument_id IS NOT NULL THEN 0 ELSE 1 END,
                    CASE WHEN t.instrument_id IS NOT NULL THEN 0 ELSE 1 END
            ) = 1
            """
        )

        # 2) Conserver les anciens symboles non présents dans les prix
        con.execute(
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
            SELECT
                o.instrument_id,
                o.company_id,
                o.symbol,
                o.exchange,
                o.security_type,
                o.share_class,
                o.is_active,
                CURRENT_TIMESTAMP AS created_at
            FROM tmp_old_instrument_master o
            WHERE NOT EXISTS (
                SELECT 1
                FROM tmp_price_symbol_bounds b
                WHERE b.symbol = o.symbol
            )
            """
        )

        # 3) Ticker history PIT à partir de l'historique prix
        con.execute(
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
            SELECT
                b.instrument_id,
                b.symbol,
                COALESCE(
                    im.exchange,
                    o.exchange,
                    t.exchange
                ) AS exchange,
                b.min_bar_date AS valid_from,
                NULL AS valid_to,
                TRUE AS is_current,
                CURRENT_TIMESTAMP AS created_at
            FROM tmp_price_symbol_bounds b
            LEFT JOIN instrument_master im
                ON im.instrument_id = b.instrument_id
            LEFT JOIN tmp_old_instrument_master o
                ON o.symbol = b.symbol
            LEFT JOIN tmp_old_ticker_history t
                ON t.symbol = b.symbol
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY b.symbol, b.instrument_id
                ORDER BY
                    CASE WHEN im.instrument_id IS NOT NULL THEN 0 ELSE 1 END,
                    CASE WHEN o.instrument_id IS NOT NULL THEN 0 ELSE 1 END,
                    CASE WHEN t.instrument_id IS NOT NULL THEN 0 ELSE 1 END
            ) = 1
            """
        )

        # 4) Conserver les anciens ticker_history pour les symboles hors historique prix
        con.execute(
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
            SELECT
                t.instrument_id,
                t.symbol,
                t.exchange,
                t.valid_from,
                t.valid_to,
                t.is_current,
                CURRENT_TIMESTAMP AS created_at
            FROM tmp_old_ticker_history t
            WHERE NOT EXISTS (
                SELECT 1
                FROM tmp_price_symbol_bounds b
                WHERE b.symbol = t.symbol
            )
            """
        )

        # 5) Recréer une map ticker minimale cohérente avec les ids reconstruits
        con.execute(
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
            con.execute("SELECT COUNT(*) FROM instrument_master").fetchone()[0]
        )
        ticker_history_rows = int(
            con.execute("SELECT COUNT(*) FROM ticker_history").fetchone()[0]
        )
        identifier_map_rows = int(
            con.execute("SELECT COUNT(*) FROM instrument_identifier_map").fetchone()[0]
        )

        return {
            "price_symbol_count": price_symbol_count,
            "instrument_master_rows": instrument_master_rows,
            "ticker_history_rows": ticker_history_rows,
            "identifier_map_rows": identifier_map_rows,
        }
