from __future__ import annotations

from stock_quant.infrastructure.db.unit_of_work import DuckDbUnitOfWork


class ResearchUniverseService:
    def __init__(self, uow: DuckDbUnitOfWork) -> None:
        self.uow = uow

    @property
    def con(self):
        if self.uow.connection is None:
            raise RuntimeError("active DB connection is required")
        return self.uow.connection

    def rebuild_conservative_research_universe(self) -> dict[str, int]:
        con = self.con

        con.execute("DELETE FROM research_universe")

        con.execute("DROP TABLE IF EXISTS tmp_stock_base")
        con.execute("DROP TABLE IF EXISTS tmp_adr_symbols")
        con.execute("DROP TABLE IF EXISTS tmp_suffix_candidates")
        con.execute("DROP TABLE IF EXISTS tmp_suffix_exclusions")
        con.execute("DROP TABLE IF EXISTS tmp_manual_overrides")
        con.execute("DROP TABLE IF EXISTS tmp_research_universe_base")

        con.execute(
            """
            CREATE TEMP TABLE tmp_stock_base AS
            SELECT
                UPPER(TRIM(symbol)) AS symbol,
                venue_group,
                asset_class
            FROM (
                SELECT
                    UPPER(TRIM(symbol)) AS symbol,
                    venue_group,
                    asset_class,
                    ROW_NUMBER() OVER (
                        PARTITION BY UPPER(TRIM(symbol))
                        ORDER BY
                            CASE venue_group
                                WHEN 'NASDAQ' THEN 1
                                WHEN 'NYSE' THEN 2
                                WHEN 'NYSEMKT' THEN 3
                                ELSE 99
                            END,
                            asset_class
                    ) AS rn
                FROM price_source_daily_raw_all
                WHERE asset_class = 'STOCK'
                  AND venue_group IN ('NASDAQ', 'NYSE', 'NYSEMKT')
                  AND symbol IS NOT NULL
                  AND TRIM(symbol) <> ''
            ) q
            WHERE rn = 1
            """
        )

        con.execute(
            """
            CREATE TEMP TABLE tmp_adr_symbols AS
            SELECT DISTINCT UPPER(TRIM(symbol)) AS symbol
            FROM instrument_master
            WHERE UPPER(TRIM(security_type)) = 'ADR'
              AND symbol IS NOT NULL
              AND TRIM(symbol) <> ''
            """
        )

        con.execute(
            """
            CREATE TEMP TABLE tmp_suffix_candidates AS
            SELECT
                s.symbol,
                LENGTH(s.symbol) AS symbol_len,
                SUBSTR(s.symbol, 1, LENGTH(s.symbol) - 1) AS base_symbol,
                RIGHT(s.symbol, 1) AS suffix_type
            FROM tmp_stock_base s
            WHERE LENGTH(s.symbol) >= 5
            """
        )

        con.execute(
            """
            CREATE TEMP TABLE tmp_suffix_exclusions AS
            SELECT
                c.symbol,
                c.base_symbol,
                c.suffix_type
            FROM tmp_suffix_candidates c
            INNER JOIN tmp_stock_base b
              ON c.base_symbol = b.symbol
            WHERE c.suffix_type IN ('R', 'U', 'W')
            """
        )

        con.execute(
            """
            CREATE TEMP TABLE tmp_manual_overrides AS
            SELECT
                UPPER(TRIM(symbol)) AS symbol,
                include_in_research_universe,
                override_reason
            FROM (
                SELECT
                    symbol,
                    include_in_research_universe,
                    override_reason,
                    ROW_NUMBER() OVER (
                        PARTITION BY UPPER(TRIM(symbol))
                        ORDER BY created_at DESC, override_reason
                    ) AS rn
                FROM research_universe_manual_overrides
                WHERE symbol IS NOT NULL
                  AND TRIM(symbol) <> ''
            ) q
            WHERE rn = 1
            """
        )

        con.execute(
            """
            CREATE TEMP TABLE tmp_research_universe_base AS
            SELECT
                s.symbol,
                s.venue_group,
                s.asset_class,
                CASE WHEN a.symbol IS NOT NULL THEN TRUE ELSE FALSE END AS is_adr,
                CASE WHEN x.symbol IS NOT NULL THEN TRUE ELSE FALSE END AS is_suffix_derived,
                x.suffix_type,
                x.base_symbol,
                FALSE AS is_preferred_candidate,
                CASE
                    WHEN a.symbol IS NOT NULL THEN FALSE
                    WHEN x.symbol IS NOT NULL THEN FALSE
                    ELSE TRUE
                END AS auto_include_in_research_universe,
                CASE
                    WHEN a.symbol IS NOT NULL THEN 'adr'
                    WHEN x.symbol IS NOT NULL THEN 'derived_suffix_' || x.suffix_type
                    ELSE NULL
                END AS auto_exclusion_reason
            FROM tmp_stock_base s
            LEFT JOIN tmp_adr_symbols a
              ON s.symbol = a.symbol
            LEFT JOIN tmp_suffix_exclusions x
              ON s.symbol = x.symbol
            """
        )

        con.execute(
            """
            INSERT INTO research_universe (
                symbol,
                venue_group,
                asset_class,
                is_adr,
                is_suffix_derived,
                suffix_type,
                base_symbol,
                is_preferred_candidate,
                include_in_research_universe,
                exclusion_reason,
                manual_override_applied,
                manual_override_include,
                manual_override_reason,
                created_at
            )
            SELECT
                b.symbol,
                b.venue_group,
                b.asset_class,
                b.is_adr,
                b.is_suffix_derived,
                b.suffix_type,
                b.base_symbol,
                b.is_preferred_candidate,
                CASE
                    WHEN m.symbol IS NOT NULL THEN m.include_in_research_universe
                    ELSE b.auto_include_in_research_universe
                END AS include_in_research_universe,
                CASE
                    WHEN m.symbol IS NOT NULL AND m.include_in_research_universe = FALSE THEN m.override_reason
                    WHEN m.symbol IS NOT NULL AND m.include_in_research_universe = TRUE THEN NULL
                    ELSE b.auto_exclusion_reason
                END AS exclusion_reason,
                CASE WHEN m.symbol IS NOT NULL THEN TRUE ELSE FALSE END AS manual_override_applied,
                m.include_in_research_universe AS manual_override_include,
                m.override_reason AS manual_override_reason,
                CURRENT_TIMESTAMP AS created_at
            FROM tmp_research_universe_base b
            LEFT JOIN tmp_manual_overrides m
              ON b.symbol = m.symbol
            ORDER BY b.symbol
            """
        )

        stock_base_count = int(
            con.execute("SELECT COUNT(*) FROM tmp_stock_base").fetchone()[0]
        )
        adr_count = int(
            con.execute("SELECT COUNT(*) FROM tmp_adr_symbols").fetchone()[0]
        )
        suffix_exclusion_count = int(
            con.execute("SELECT COUNT(*) FROM tmp_suffix_exclusions").fetchone()[0]
        )
        manual_override_count = int(
            con.execute("SELECT COUNT(*) FROM tmp_manual_overrides").fetchone()[0]
        )
        manual_exclusion_count = int(
            con.execute(
                """
                SELECT COUNT(*)
                FROM research_universe
                WHERE manual_override_applied = TRUE
                  AND include_in_research_universe = FALSE
                """
            ).fetchone()[0]
        )
        included_count = int(
            con.execute(
                """
                SELECT COUNT(*)
                FROM research_universe
                WHERE include_in_research_universe = TRUE
                """
            ).fetchone()[0]
        )
        total_rows = int(
            con.execute("SELECT COUNT(*) FROM research_universe").fetchone()[0]
        )

        return {
            "stock_base_count": stock_base_count,
            "adr_count": adr_count,
            "derived_suffix_exclusion_count": suffix_exclusion_count,
            "manual_override_count": manual_override_count,
            "manual_exclusion_count": manual_exclusion_count,
            "research_universe_rows": total_rows,
            "included_research_universe_rows": included_count,
            "preferred_auto_exclusions": 0,
        }
