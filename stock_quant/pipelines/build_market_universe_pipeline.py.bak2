from __future__ import annotations

from stock_quant.app.dto.pipeline_result import PipelineResult
# REMOVED: BasePipeline supprimé pendant la consolidation brutale
from stock_quant.shared.exceptions import PipelineError


class BuildMarketUniversePipeline(BasePipeline):
    """
    Construit `market_universe` à partir de `symbol_reference_source_raw`.

    Univers cible
    -------------
    - actions US standard
    - venues autorisées : NASDAQ, NYSE
    - exclusions explicites :
        * OTC / Pink / autres venues non autorisées
        * ETF / ETN
        * ADR / ADS / depositary shares
        * preferred
        * warrant
        * right
        * unit

    Notes
    -----
    - SQL-first
    - pipeline mince
    - repository attendu : objet exposant `con`
    - on fusionne les signaux multi-sources par symbole
    """

    pipeline_name = "build_market_universe"

    def __init__(self, repository, allow_adr: bool = False) -> None:
        self.repository = repository
        self.allow_adr = bool(allow_adr)
        self._metrics: dict[str, int] = {}
        self._written_universe = 0
        self._written_conflicts = 0

    @property
    def con(self):
        con = getattr(self.repository, "con", None)
        if con is None:
            raise PipelineError("active DB connection is required")
        return con

    def extract(self):
        return None

    def transform(self, data):
        return None

    def validate(self, data) -> None:
        raw_candidates = int(
            self.con.execute(
                """
                SELECT COUNT(*)
                FROM symbol_reference_source_raw
                WHERE symbol IS NOT NULL
                  AND TRIM(symbol) <> ''
                """
            ).fetchone()[0]
        )

        if raw_candidates == 0:
            raise PipelineError("no raw candidates available in symbol_reference_source_raw")

    def load(self, data) -> None:
        con = self.con

        raw_candidates = int(
            con.execute(
                """
                SELECT COUNT(*)
                FROM symbol_reference_source_raw
                WHERE symbol IS NOT NULL
                  AND TRIM(symbol) <> ''
                """
            ).fetchone()[0]
        )

        con.execute("DELETE FROM market_universe")
        con.execute("DELETE FROM market_universe_conflicts")
        con.execute("DROP TABLE IF EXISTS tmp_market_universe_raw")
        con.execute("DROP TABLE IF EXISTS tmp_market_universe_agg")
        con.execute("DROP TABLE IF EXISTS tmp_market_universe_final")

        # ------------------------------------------------------------------
        # 1) Normalisation brute par ligne
        # ------------------------------------------------------------------
        con.execute(
            """
            CREATE TEMP TABLE tmp_market_universe_raw AS
            SELECT
                UPPER(TRIM(symbol)) AS symbol,
                NULLIF(TRIM(company_name), '') AS company_name,
                NULLIF(TRIM(cik), '') AS cik,
                NULLIF(TRIM(exchange_raw), '') AS exchange_raw,
                CASE
                    WHEN UPPER(TRIM(COALESCE(exchange_raw, ''))) IN ('NASDAQ', 'NASDAQGS', 'NASDAQGM', 'NASDAQCM') THEN 'NASDAQ'
                    WHEN UPPER(TRIM(COALESCE(exchange_raw, ''))) IN ('NYSE', 'NYQ') THEN 'NYSE'
                    WHEN UPPER(TRIM(COALESCE(exchange_raw, ''))) IN ('NYSE_ARCA', 'NYSEARCA', 'ARCA', 'P') THEN 'NYSE_ARCA'
                    WHEN UPPER(TRIM(COALESCE(exchange_raw, ''))) IN ('NYSE_MKT', 'NYSEMKT', 'AMEX', 'A', 'NYSE_AMERICAN', 'NYSEAMERICAN') THEN 'NYSE_AMERICAN'
                    WHEN UPPER(TRIM(COALESCE(exchange_raw, ''))) IN ('BATS', 'Z') THEN 'BATS'
                    WHEN UPPER(TRIM(COALESCE(exchange_raw, ''))) IN ('IEX', 'V') THEN 'IEX'
                    WHEN UPPER(TRIM(COALESCE(exchange_raw, ''))) LIKE 'OTC%' THEN 'OTC'
                    WHEN UPPER(TRIM(COALESCE(exchange_raw, ''))) LIKE '%PINK%' THEN 'PINK'
                    WHEN NULLIF(TRIM(exchange_raw), '') IS NULL THEN NULL
                    ELSE UPPER(REPLACE(TRIM(exchange_raw), ' ', '_'))
                END AS exchange_normalized,
                NULLIF(TRIM(security_type_raw), '') AS security_type_raw,
                COALESCE(NULLIF(TRIM(source_name), ''), 'unknown_source') AS source_name,
                as_of_date,
                UPPER(
                    COALESCE(TRIM(company_name), '')
                    || ' '
                    || COALESCE(TRIM(security_type_raw), '')
                ) AS evidence_text
            FROM symbol_reference_source_raw
            WHERE symbol IS NOT NULL
              AND TRIM(symbol) <> ''
            """
        )

        # ------------------------------------------------------------------
        # 2) Agrégation par symbole
        # ------------------------------------------------------------------
        con.execute(
            """
            CREATE TEMP TABLE tmp_market_universe_agg AS
            SELECT
                symbol,

                COALESCE(
                    MAX(company_name) FILTER (
                        WHERE source_name = 'sec_company_tickers_exchange'
                          AND company_name IS NOT NULL
                          AND TRIM(company_name) <> ''
                    ),
                    MAX(company_name) FILTER (
                        WHERE source_name = 'nasdaq_symbol_directory'
                          AND company_name IS NOT NULL
                          AND TRIM(company_name) <> ''
                    ),
                    MAX(company_name) FILTER (
                        WHERE company_name IS NOT NULL
                          AND TRIM(company_name) <> ''
                    ),
                    symbol
                ) AS company_name,

                MAX(cik) FILTER (
                    WHERE cik IS NOT NULL
                      AND TRIM(cik) <> ''
                ) AS cik,

                COALESCE(
                    MAX(exchange_raw) FILTER (
                        WHERE exchange_normalized IN ('NASDAQ', 'NYSE')
                    ),
                    MAX(exchange_raw) FILTER (
                        WHERE exchange_raw IS NOT NULL
                          AND TRIM(exchange_raw) <> ''
                    )
                ) AS exchange_raw,

                COALESCE(
                    MAX(exchange_normalized) FILTER (
                        WHERE exchange_normalized = 'NASDAQ'
                    ),
                    MAX(exchange_normalized) FILTER (
                        WHERE exchange_normalized = 'NYSE'
                    ),
                    MAX(exchange_normalized) FILTER (
                        WHERE exchange_normalized IS NOT NULL
                    )
                ) AS exchange_normalized,

                MAX(as_of_date) AS as_of_date,
                COUNT(*) AS source_row_count,
                COUNT(DISTINCT source_name) AS source_name_count,

                BOOL_OR(
                    evidence_text LIKE '%ETF%'
                    OR evidence_text LIKE '%EXCHANGE TRADED FUND%'
                ) AS is_etf,

                BOOL_OR(
                    evidence_text LIKE '%ETN%'
                    OR evidence_text LIKE '%EXCHANGE TRADED NOTE%'
                ) AS is_etn,

                BOOL_OR(
                    evidence_text LIKE '%ADR%'
                    OR evidence_text LIKE '%ADS%'
                    OR evidence_text LIKE '%AMERICAN DEPOSITARY%'
                    OR evidence_text LIKE '%DEPOSITARY SHARE%'
                    OR evidence_text LIKE '%DEPOSITARY SHARES%'
                ) AS is_adr,

                BOOL_OR(
                    evidence_text LIKE '%PREFERRED%'
                    OR evidence_text LIKE '%PREF%'
                ) AS is_preferred,

                BOOL_OR(
                    evidence_text LIKE '%WARRANT%'
                ) AS is_warrant,

                BOOL_OR(
                    evidence_text LIKE '% RIGHTS'
                    OR evidence_text LIKE '% RIGHTS%'
                    OR evidence_text LIKE '% RIGHT '
                ) AS is_right,

                BOOL_OR(
                    evidence_text LIKE '% UNITS'
                    OR evidence_text LIKE '% UNITS%'
                    OR evidence_text LIKE '% UNIT '
                ) AS is_unit,

                BOOL_OR(
                    evidence_text LIKE '%COMMON STOCK%'
                    OR evidence_text LIKE '%COMMON SHARES%'
                    OR evidence_text LIKE '%COMMON SHARE%'
                    OR evidence_text LIKE '%ORDINARY SHARES%'
                    OR evidence_text LIKE '%ORDINARY SHARE%'
                    OR evidence_text LIKE '%CLASS A ORDINARY SHARES%'
                    OR evidence_text LIKE '%CLASS B ORDINARY SHARES%'
                    OR evidence_text LIKE '%CLASS A COMMON STOCK%'
                    OR evidence_text LIKE '%CLASS B COMMON STOCK%'
                    OR evidence_text LIKE '%CLASS C COMMON STOCK%'
                ) AS has_common_equity_signal
            FROM tmp_market_universe_raw
            GROUP BY symbol
            """
        )

        # ------------------------------------------------------------------
        # 3) Décision finale d'inclusion
        #
        # IMPORTANT :
        # - exchange_normalized NULL => EXCLU
        # - seule inclusion possible : NASDAQ / NYSE explicites
        # ------------------------------------------------------------------
        con.execute(
            f"""
            CREATE TEMP TABLE tmp_market_universe_final AS
            SELECT
                symbol,
                company_name,
                cik,
                exchange_raw,
                exchange_normalized,

                CASE
                    WHEN is_etf THEN 'ETF'
                    WHEN is_etn THEN 'ETN'
                    WHEN is_adr THEN 'ADR'
                    WHEN is_preferred THEN 'PREFERRED'
                    WHEN is_warrant THEN 'WARRANT'
                    WHEN is_right THEN 'RIGHT'
                    WHEN is_unit THEN 'UNIT'
                    WHEN has_common_equity_signal THEN 'COMMON_STOCK'
                    WHEN exchange_normalized IN ('NASDAQ', 'NYSE') THEN 'OPERATING_EQUITY'
                    ELSE 'UNKNOWN'
                END AS security_type,

                CASE
                    WHEN exchange_normalized IS NULL THEN FALSE
                    WHEN exchange_normalized NOT IN ('NASDAQ', 'NYSE') THEN FALSE
                    WHEN is_etf THEN FALSE
                    WHEN is_etn THEN FALSE
                    WHEN is_preferred THEN FALSE
                    WHEN is_warrant THEN FALSE
                    WHEN is_right THEN FALSE
                    WHEN is_unit THEN FALSE
                    WHEN is_adr AND {str(self.allow_adr).upper()} THEN TRUE
                    WHEN is_adr AND NOT {str(self.allow_adr).upper()} THEN FALSE
                    ELSE TRUE
                END AS include_in_universe,

                CASE
                    WHEN exchange_normalized IS NULL THEN 'missing_exchange'
                    WHEN exchange_normalized NOT IN ('NASDAQ', 'NYSE') THEN 'exchange_not_allowed'
                    WHEN is_etf THEN 'etf_excluded'
                    WHEN is_etn THEN 'etn_excluded'
                    WHEN is_preferred THEN 'preferred_excluded'
                    WHEN is_warrant THEN 'warrant_excluded'
                    WHEN is_right THEN 'right_excluded'
                    WHEN is_unit THEN 'unit_excluded'
                    WHEN is_adr AND NOT {str(self.allow_adr).upper()} THEN 'adr_excluded'
                    ELSE NULL
                END AS exclusion_reason,

                CASE
                    WHEN exchange_normalized IN ('NASDAQ', 'NYSE')
                     AND NOT is_etf
                     AND NOT is_etn
                     AND NOT is_preferred
                     AND NOT is_warrant
                     AND NOT is_right
                     AND NOT is_unit
                     AND (NOT is_adr OR {str(self.allow_adr).upper()})
                    THEN TRUE
                    ELSE FALSE
                END AS is_common_stock,

                is_adr,
                is_etf,
                is_preferred,
                is_warrant,
                is_right,
                is_unit,
                'symbol_source_merge' AS source_name,
                as_of_date
            FROM tmp_market_universe_agg
            """
        )

        con.execute(
            """
            INSERT INTO market_universe (
                symbol,
                company_name,
                cik,
                exchange_raw,
                exchange_normalized,
                security_type,
                include_in_universe,
                exclusion_reason,
                is_common_stock,
                is_adr,
                is_etf,
                is_preferred,
                is_warrant,
                is_right,
                is_unit,
                source_name,
                as_of_date,
                created_at
            )
            SELECT
                symbol,
                company_name,
                cik,
                exchange_raw,
                exchange_normalized,
                security_type,
                include_in_universe,
                exclusion_reason,
                is_common_stock,
                is_adr,
                is_etf,
                is_preferred,
                is_warrant,
                is_right,
                is_unit,
                source_name,
                as_of_date,
                CURRENT_TIMESTAMP
            FROM tmp_market_universe_final
            ORDER BY symbol
            """
        )

        con.execute(
            """
            INSERT INTO market_universe_conflicts (
                symbol,
                chosen_source,
                rejected_source,
                reason,
                payload_json,
                created_at
            )
            SELECT
                a.symbol,
                'symbol_source_merge' AS chosen_source,
                'multi_source' AS rejected_source,
                'multi_source_symbol_merge' AS reason,
                json_object(
                    'source_row_count', a.source_row_count,
                    'source_name_count', a.source_name_count,
                    'exchange_normalized', a.exchange_normalized,
                    'security_type',
                        CASE
                            WHEN a.is_etf THEN 'ETF'
                            WHEN a.is_etn THEN 'ETN'
                            WHEN a.is_adr THEN 'ADR'
                            WHEN a.is_preferred THEN 'PREFERRED'
                            WHEN a.is_warrant THEN 'WARRANT'
                            WHEN a.is_right THEN 'RIGHT'
                            WHEN a.is_unit THEN 'UNIT'
                            WHEN a.has_common_equity_signal THEN 'COMMON_STOCK'
                            WHEN a.exchange_normalized IN ('NASDAQ', 'NYSE') THEN 'OPERATING_EQUITY'
                            ELSE 'UNKNOWN'
                        END
                ) AS payload_json,
                CURRENT_TIMESTAMP
            FROM tmp_market_universe_agg a
            WHERE a.source_name_count > 1
            """
        )

        final_entries = int(
            con.execute("SELECT COUNT(*) FROM market_universe").fetchone()[0]
        )
        included_final_entries = int(
            con.execute(
                """
                SELECT COUNT(*)
                FROM market_universe
                WHERE include_in_universe = TRUE
                """
            ).fetchone()[0]
        )
        conflicts = int(
            con.execute("SELECT COUNT(*) FROM market_universe_conflicts").fetchone()[0]
        )

        exchange_not_allowed_count = int(
            con.execute(
                """
                SELECT COUNT(*)
                FROM market_universe
                WHERE exclusion_reason = 'exchange_not_allowed'
                """
            ).fetchone()[0]
        )
        missing_exchange_count = int(
            con.execute(
                """
                SELECT COUNT(*)
                FROM market_universe
                WHERE exclusion_reason = 'missing_exchange'
                """
            ).fetchone()[0]
        )
        etf_excluded_count = int(
            con.execute(
                """
                SELECT COUNT(*)
                FROM market_universe
                WHERE exclusion_reason = 'etf_excluded'
                """
            ).fetchone()[0]
        )
        etn_excluded_count = int(
            con.execute(
                """
                SELECT COUNT(*)
                FROM market_universe
                WHERE exclusion_reason = 'etn_excluded'
                """
            ).fetchone()[0]
        )
        adr_excluded_count = int(
            con.execute(
                """
                SELECT COUNT(*)
                FROM market_universe
                WHERE exclusion_reason = 'adr_excluded'
                """
            ).fetchone()[0]
        )
        preferred_excluded_count = int(
            con.execute(
                """
                SELECT COUNT(*)
                FROM market_universe
                WHERE exclusion_reason = 'preferred_excluded'
                """
            ).fetchone()[0]
        )
        warrant_excluded_count = int(
            con.execute(
                """
                SELECT COUNT(*)
                FROM market_universe
                WHERE exclusion_reason = 'warrant_excluded'
                """
            ).fetchone()[0]
        )
        right_excluded_count = int(
            con.execute(
                """
                SELECT COUNT(*)
                FROM market_universe
                WHERE exclusion_reason = 'right_excluded'
                """
            ).fetchone()[0]
        )
        unit_excluded_count = int(
            con.execute(
                """
                SELECT COUNT(*)
                FROM market_universe
                WHERE exclusion_reason = 'unit_excluded'
                """
            ).fetchone()[0]
        )

        self._written_universe = final_entries
        self._written_conflicts = conflicts
        self._metrics = {
            "raw_candidates": raw_candidates,
            "final_entries": final_entries,
            "included_final_entries": included_final_entries,
            "conflicts": conflicts,
            "exchange_not_allowed_count": exchange_not_allowed_count,
            "missing_exchange_count": missing_exchange_count,
            "etf_excluded_count": etf_excluded_count,
            "etn_excluded_count": etn_excluded_count,
            "adr_excluded_count": adr_excluded_count,
            "preferred_excluded_count": preferred_excluded_count,
            "warrant_excluded_count": warrant_excluded_count,
            "right_excluded_count": right_excluded_count,
            "unit_excluded_count": unit_excluded_count,
        }

    def finalize(self, result: PipelineResult) -> PipelineResult:
        result.rows_read = int(self._metrics.get("raw_candidates", 0))
        result.rows_written = self._written_universe
        result.metrics.update(self._metrics)
        return result
