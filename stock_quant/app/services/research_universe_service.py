from __future__ import annotations

from stock_quant.infrastructure.db.unit_of_work import DuckDbUnitOfWork


class ResearchUniverseService:
    RESEARCH_UNIVERSE_NAME = "research_universe"
    RESEARCH_UNIVERSE_SOURCE = "research_universe_service"

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

        for table_name in [
            "tmp_stock_base",
            "tmp_adr_symbols",
            "tmp_suffix_candidates",
            "tmp_suffix_exclusions",
            "tmp_manual_overrides",
            "tmp_research_universe_base",
            "tmp_research_universe_snapshot",
            "tmp_research_universe_snapshot_resolved",
            "tmp_current_research_membership_open",
            "tmp_current_research_membership_changed",
        ]:
            con.execute(f"DROP TABLE IF EXISTS {table_name}")

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
            WHERE UPPER(TRIM(COALESCE(security_type, ''))) = 'ADR'
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

        snapshot_price_date = con.execute(
            """
            SELECT MAX(price_date)
            FROM price_source_daily_raw_all
            WHERE asset_class = 'STOCK'
              AND venue_group IN ('NASDAQ', 'NYSE', 'NYSEMKT')
            """
        ).fetchone()[0]
        if snapshot_price_date is None:
            raise RuntimeError("unable to derive research universe snapshot date from price_source_daily_raw_all")

        con.execute(
            """
            CREATE TEMP TABLE tmp_research_universe_snapshot AS
            SELECT
                ru.symbol,
                ru.include_in_research_universe,
                ru.exclusion_reason,
                ru.manual_override_applied,
                ru.manual_override_include,
                ru.manual_override_reason,
                ?::DATE AS snapshot_date,
                CASE
                    WHEN ru.include_in_research_universe THEN 'ACTIVE'
                    ELSE 'EXCLUDED'
                END AS membership_status,
                CASE
                    WHEN ru.include_in_research_universe THEN COALESCE(ru.manual_override_reason, 'included')
                    ELSE COALESCE(ru.manual_override_reason, ru.exclusion_reason, 'excluded')
                END AS membership_reason
            FROM research_universe ru
            WHERE ru.symbol IS NOT NULL
              AND TRIM(ru.symbol) <> ''
            """,
            [snapshot_price_date],
        )

        con.execute(
            """
            CREATE TEMP TABLE tmp_research_universe_snapshot_resolved AS
            WITH th_ranked AS (
                SELECT
                    s.symbol,
                    s.snapshot_date,
                    th.instrument_id,
                    COALESCE(th.company_id, im.company_id) AS company_id,
                    ROW_NUMBER() OVER (
                        PARTITION BY s.symbol, s.snapshot_date
                        ORDER BY
                            COALESCE(th.is_primary, FALSE) DESC,
                            th.valid_from DESC NULLS LAST,
                            th.created_at DESC NULLS LAST
                    ) AS rn
                FROM tmp_research_universe_snapshot s
                LEFT JOIN ticker_history th
                    ON UPPER(TRIM(s.symbol)) = UPPER(TRIM(th.symbol))
                   AND s.snapshot_date BETWEEN th.valid_from AND COALESCE(th.valid_to, DATE '9999-12-31')
                LEFT JOIN instrument_master im
                    ON th.instrument_id = im.instrument_id
            ),
            im_fallback AS (
                SELECT
                    UPPER(TRIM(symbol)) AS symbol_key,
                    instrument_id,
                    company_id,
                    ROW_NUMBER() OVER (
                        PARTITION BY UPPER(TRIM(symbol))
                        ORDER BY
                            COALESCE(is_active, FALSE) DESC,
                            COALESCE(listing_date, DATE '1900-01-01') DESC,
                            created_at DESC NULLS LAST
                    ) AS rn
                FROM instrument_master
                WHERE symbol IS NOT NULL
                  AND TRIM(symbol) <> ''
            )
            SELECT
                COALESCE(t.instrument_id, f.instrument_id) AS instrument_id,
                COALESCE(t.company_id, f.company_id) AS company_id,
                s.symbol,
                ?::VARCHAR AS universe_name,
                s.snapshot_date AS effective_from,
                CAST(NULL AS DATE) AS effective_to,
                s.membership_status,
                s.membership_reason AS reason,
                ?::VARCHAR AS source_name
            FROM tmp_research_universe_snapshot s
            LEFT JOIN th_ranked t
                ON s.symbol = t.symbol
               AND s.snapshot_date = t.snapshot_date
               AND t.rn = 1
            LEFT JOIN im_fallback f
                ON UPPER(TRIM(s.symbol)) = f.symbol_key
               AND f.rn = 1
            """,
            [self.RESEARCH_UNIVERSE_NAME, self.RESEARCH_UNIVERSE_SOURCE],
        )

        unresolved_membership_rows = int(
            con.execute(
                """
                SELECT COUNT(*)
                FROM tmp_research_universe_snapshot_resolved
                WHERE instrument_id IS NULL
                """
            ).fetchone()[0]
        )

        con.execute(
            """
            DELETE FROM universe_membership_history
            WHERE LOWER(TRIM(universe_name)) = LOWER(TRIM(?))
              AND effective_from = ?
            """,
            [self.RESEARCH_UNIVERSE_NAME, snapshot_price_date],
        )

        con.execute(
            """
            CREATE TEMP TABLE tmp_current_research_membership_open AS
            SELECT
                instrument_id,
                company_id,
                symbol,
                universe_name,
                effective_from,
                effective_to,
                membership_status,
                reason,
                source_name
            FROM universe_membership_history
            WHERE LOWER(TRIM(universe_name)) = LOWER(TRIM(?))
              AND effective_to IS NULL
            """,
            [self.RESEARCH_UNIVERSE_NAME],
        )

        con.execute(
            """
            CREATE TEMP TABLE tmp_current_research_membership_changed AS
            SELECT
                cur.instrument_id,
                cur.company_id,
                cur.symbol,
                cur.universe_name,
                cur.effective_from,
                cur.effective_to,
                cur.membership_status,
                cur.reason,
                cur.source_name
            FROM tmp_current_research_membership_open cur
            INNER JOIN tmp_research_universe_snapshot_resolved snap
                ON UPPER(TRIM(cur.symbol)) = UPPER(TRIM(snap.symbol))
            WHERE (
                    COALESCE(cur.instrument_id, '') <> COALESCE(snap.instrument_id, '')
                 OR COALESCE(cur.company_id, '') <> COALESCE(snap.company_id, '')
                 OR COALESCE(cur.membership_status, '') <> COALESCE(snap.membership_status, '')
                 OR COALESCE(cur.reason, '') <> COALESCE(snap.reason, '')
            )
              AND cur.effective_from < snap.effective_from
            """
        )

        changed_membership_rows = int(
            con.execute(
                "SELECT COUNT(*) FROM tmp_current_research_membership_changed"
            ).fetchone()[0]
        )

        con.execute(
            """
            UPDATE universe_membership_history AS h
            SET effective_to = c.effective_from - INTERVAL 1 DAY
            FROM tmp_current_research_membership_changed c
            WHERE LOWER(TRIM(h.universe_name)) = LOWER(TRIM(c.universe_name))
              AND UPPER(TRIM(h.symbol)) = UPPER(TRIM(c.symbol))
              AND h.effective_to IS NULL
              AND h.effective_from = c.effective_from
            """
        )

        con.execute(
            """
            INSERT INTO universe_membership_history (
                instrument_id,
                company_id,
                symbol,
                universe_name,
                effective_from,
                effective_to,
                membership_status,
                reason,
                source_name,
                created_at
            )
            SELECT
                snap.instrument_id,
                snap.company_id,
                snap.symbol,
                snap.universe_name,
                snap.effective_from,
                snap.effective_to,
                snap.membership_status,
                snap.reason,
                snap.source_name,
                CURRENT_TIMESTAMP
            FROM tmp_research_universe_snapshot_resolved snap
            LEFT JOIN universe_membership_history h
                ON LOWER(TRIM(h.universe_name)) = LOWER(TRIM(snap.universe_name))
               AND UPPER(TRIM(h.symbol)) = UPPER(TRIM(snap.symbol))
               AND h.effective_to IS NULL
               AND COALESCE(h.instrument_id, '') = COALESCE(snap.instrument_id, '')
               AND COALESCE(h.company_id, '') = COALESCE(snap.company_id, '')
               AND COALESCE(h.membership_status, '') = COALESCE(snap.membership_status, '')
               AND COALESCE(h.reason, '') = COALESCE(snap.reason, '')
            WHERE h.symbol IS NULL
            """
        )

        history_rows_for_research_universe = int(
            con.execute(
                """
                SELECT COUNT(*)
                FROM universe_membership_history
                WHERE LOWER(TRIM(universe_name)) = LOWER(TRIM(?))
                """,
                [self.RESEARCH_UNIVERSE_NAME],
            ).fetchone()[0]
        )

        active_history_rows_for_research_universe = int(
            con.execute(
                """
                SELECT COUNT(*)
                FROM universe_membership_history
                WHERE LOWER(TRIM(universe_name)) = LOWER(TRIM(?))
                  AND effective_to IS NULL
                  AND UPPER(TRIM(COALESCE(membership_status, ''))) = 'ACTIVE'
                """,
                [self.RESEARCH_UNIVERSE_NAME],
            ).fetchone()[0]
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
            "research_universe_snapshot_date_rows": 1,
            "research_universe_snapshot_date_yyyymmdd": int(snapshot_price_date.strftime("%Y%m%d")),
            "research_universe_history_rows": history_rows_for_research_universe,
            "research_universe_history_active_rows": active_history_rows_for_research_universe,
            "research_universe_history_changed_rows": changed_membership_rows,
            "research_universe_history_unresolved_rows": unresolved_membership_rows,
        }
