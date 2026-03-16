from __future__ import annotations

from stock_quant.app.dto.pipeline_result import PipelineResult
from stock_quant.infrastructure.db.unit_of_work import DuckDbUnitOfWork
from stock_quant.pipelines.base_pipeline import BasePipeline
from stock_quant.shared.exceptions import PipelineError


class BuildMarketUniversePipeline(BasePipeline):
    pipeline_name = "build_market_universe"

    def __init__(self, repository=None, uow: DuckDbUnitOfWork | None = None, allow_adr: bool = True) -> None:
        if repository is not None:
            self.uow = repository.uow
        else:
            self.uow = uow

        if self.uow is None:
            raise ValueError("BuildMarketUniversePipeline requires repository or uow")

        self.allow_adr = allow_adr
        self._metrics: dict[str, int] = {}
        self._written_universe = 0
        self._written_conflicts = 0

    @property
    def con(self):
        if self.uow.connection is None:
            raise PipelineError("active DB connection is required")
        return self.uow.connection

    def extract(self):
        return None

    def transform(self, data):
        return None

    def validate(self, data) -> None:
        raw_candidates = int(
            self.con.execute("SELECT COUNT(*) FROM symbol_reference_source_raw").fetchone()[0]
        )
        if raw_candidates == 0:
            raise PipelineError("no raw candidates available in symbol_reference_source_raw")

    def load(self, data) -> None:
        con = self.con

        raw_candidates = int(
            con.execute("SELECT COUNT(*) FROM symbol_reference_source_raw").fetchone()[0]
        )

        con.execute("DELETE FROM market_universe")
        con.execute("DELETE FROM market_universe_conflicts")

        con.execute("DROP TABLE IF EXISTS tmp_universe_candidates")
        con.execute(
            f"""
            CREATE TEMP TABLE tmp_universe_candidates AS
            WITH normalized AS (
                SELECT
                    UPPER(TRIM(symbol)) AS symbol,
                    NULLIF(TRIM(company_name), '') AS company_name,
                    NULLIF(TRIM(cik), '') AS cik,
                    NULLIF(TRIM(exchange_raw), '') AS exchange_raw,
                    CASE
                        WHEN UPPER(TRIM(COALESCE(exchange_raw, ''))) IN ('NASDAQ', 'NASDAQGS', 'NASDAQGM', 'NASDAQCM')
                            THEN 'NASDAQ'
                        WHEN UPPER(TRIM(COALESCE(exchange_raw, ''))) IN ('NYSE', 'NYQ')
                            THEN 'NYSE'
                        WHEN UPPER(TRIM(COALESCE(exchange_raw, ''))) IN ('NYSEARCA', 'NYSE ARCA', 'ARCA')
                            THEN 'NYSE_ARCA'
                        WHEN UPPER(TRIM(COALESCE(exchange_raw, ''))) IN ('NYSEAMERICAN', 'NYSE AMERICAN', 'AMEX')
                            THEN 'NYSE_AMERICAN'
                        WHEN UPPER(TRIM(COALESCE(exchange_raw, ''))) LIKE 'OTC%'
                            THEN 'OTC'
                        WHEN NULLIF(TRIM(exchange_raw), '') IS NULL
                            THEN NULL
                        ELSE UPPER(REPLACE(TRIM(exchange_raw), ' ', '_'))
                    END AS exchange_normalized,
                    CASE
                        WHEN UPPER(TRIM(COALESCE(security_type_raw, ''))) LIKE '%WARRANT%' THEN 'WARRANT'
                        WHEN UPPER(TRIM(COALESCE(security_type_raw, ''))) LIKE '%ETF%' THEN 'ETF'
                        WHEN UPPER(TRIM(COALESCE(security_type_raw, ''))) LIKE '%ADR%' THEN 'ADR'
                        WHEN UPPER(TRIM(COALESCE(security_type_raw, ''))) LIKE '%RIGHT%' THEN 'RIGHT'
                        WHEN UPPER(TRIM(COALESCE(security_type_raw, ''))) LIKE '%UNIT%' THEN 'UNIT'
                        WHEN UPPER(TRIM(COALESCE(security_type_raw, ''))) LIKE '%PREFERRED%' THEN 'PREFERRED'
                        WHEN UPPER(TRIM(COALESCE(security_type_raw, ''))) LIKE '%COMMON%' THEN 'COMMON_STOCK'
                        ELSE UPPER(REPLACE(TRIM(COALESCE(security_type_raw, 'UNKNOWN')), ' ', '_'))
                    END AS security_type,
                    CASE
                        WHEN UPPER(TRIM(COALESCE(security_type_raw, ''))) LIKE '%COMMON%' THEN TRUE
                        ELSE FALSE
                    END AS is_common_stock,
                    CASE
                        WHEN UPPER(TRIM(COALESCE(security_type_raw, ''))) LIKE '%ADR%' THEN TRUE
                        ELSE FALSE
                    END AS is_adr,
                    CASE
                        WHEN UPPER(TRIM(COALESCE(security_type_raw, ''))) LIKE '%ETF%' THEN TRUE
                        ELSE FALSE
                    END AS is_etf,
                    CASE
                        WHEN UPPER(TRIM(COALESCE(security_type_raw, ''))) LIKE '%PREFERRED%' THEN TRUE
                        ELSE FALSE
                    END AS is_preferred,
                    CASE
                        WHEN UPPER(TRIM(COALESCE(security_type_raw, ''))) LIKE '%WARRANT%' THEN TRUE
                        ELSE FALSE
                    END AS is_warrant,
                    CASE
                        WHEN UPPER(TRIM(COALESCE(security_type_raw, ''))) LIKE '%RIGHT%' THEN TRUE
                        ELSE FALSE
                    END AS is_right,
                    CASE
                        WHEN UPPER(TRIM(COALESCE(security_type_raw, ''))) LIKE '%UNIT%' THEN TRUE
                        ELSE FALSE
                    END AS is_unit,
                    COALESCE(NULLIF(TRIM(source_name), ''), 'unknown_source') AS source_name,
                    as_of_date
                FROM symbol_reference_source_raw
                WHERE NULLIF(TRIM(symbol), '') IS NOT NULL
            )
            SELECT
                symbol,
                company_name,
                cik,
                exchange_raw,
                exchange_normalized,
                security_type,
                is_common_stock,
                is_adr,
                is_etf,
                is_preferred,
                is_warrant,
                is_right,
                is_unit,
                source_name,
                as_of_date,
                CASE
                    WHEN exchange_normalized NOT IN ('NASDAQ', 'NYSE')
                        THEN FALSE
                    WHEN is_etf
                        THEN FALSE
                    WHEN is_preferred
                        THEN FALSE
                    WHEN is_warrant
                        THEN FALSE
                    WHEN is_right
                        THEN FALSE
                    WHEN is_unit
                        THEN FALSE
                    WHEN is_adr AND {str(self.allow_adr).upper()}
                        THEN TRUE
                    WHEN is_adr AND NOT {str(self.allow_adr).upper()}
                        THEN FALSE
                    WHEN is_common_stock
                        THEN TRUE
                    ELSE FALSE
                END AS include_in_universe,
                CASE
                    WHEN exchange_normalized NOT IN ('NASDAQ', 'NYSE')
                        THEN 'exchange_not_allowed'
                    WHEN is_etf
                        THEN 'etf_excluded'
                    WHEN is_preferred
                        THEN 'preferred_excluded'
                    WHEN is_warrant
                        THEN 'warrant_excluded'
                    WHEN is_right
                        THEN 'right_excluded'
                    WHEN is_unit
                        THEN 'unit_excluded'
                    WHEN is_adr AND NOT {str(self.allow_adr).upper()}
                        THEN 'adr_excluded'
                    WHEN is_adr AND {str(self.allow_adr).upper()}
                        THEN NULL
                    WHEN is_common_stock
                        THEN NULL
                    ELSE 'not_common_stock'
                END AS exclusion_reason
            FROM normalized
            """
        )

        con.execute("DROP TABLE IF EXISTS tmp_universe_ranked")
        con.execute(
            """
            CREATE TEMP TABLE tmp_universe_ranked AS
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY symbol
                    ORDER BY
                        CASE WHEN include_in_universe THEN 1 ELSE 0 END DESC,
                        CASE exchange_normalized
                            WHEN 'NASDAQ' THEN 100
                            WHEN 'NYSE' THEN 95
                            WHEN 'NYSE_ARCA' THEN 85
                            WHEN 'NYSE_AMERICAN' THEN 80
                            WHEN 'BATS' THEN 70
                            WHEN 'IEX' THEN 60
                            WHEN 'OTC' THEN 1
                            ELSE 0
                        END DESC,
                        CASE WHEN cik IS NOT NULL THEN 1 ELSE 0 END DESC,
                        CASE WHEN company_name IS NOT NULL THEN 1 ELSE 0 END DESC,
                        CASE WHEN is_adr THEN 0 ELSE 1 END DESC,
                        source_name ASC
                ) AS rn
            FROM tmp_universe_candidates
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
            FROM tmp_universe_ranked
            WHERE rn = 1
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
                r.symbol,
                b.source_name AS chosen_source,
                r.source_name AS rejected_source,
                'duplicate_symbol_resolution' AS reason,
                json_object(
                    'chosen_exchange', b.exchange_normalized,
                    'rejected_exchange', r.exchange_normalized,
                    'chosen_security_type', b.security_type,
                    'rejected_security_type', r.security_type
                ) AS payload_json,
                CURRENT_TIMESTAMP
            FROM tmp_universe_ranked r
            INNER JOIN tmp_universe_ranked b
              ON r.symbol = b.symbol
             AND b.rn = 1
            WHERE r.rn > 1
            """
        )

        prepared_entries = int(
            con.execute("SELECT COUNT(*) FROM tmp_universe_candidates").fetchone()[0]
        )
        final_entries = int(
            con.execute("SELECT COUNT(*) FROM market_universe").fetchone()[0]
        )
        conflicts = int(
            con.execute("SELECT COUNT(*) FROM market_universe_conflicts").fetchone()[0]
        )
        included_final_entries = int(
            con.execute(
                "SELECT COUNT(*) FROM market_universe WHERE include_in_universe = TRUE"
            ).fetchone()[0]
        )

        self._written_universe = final_entries
        self._written_conflicts = conflicts
        self._metrics = {
            "raw_candidates": raw_candidates,
            "prepared_entries": prepared_entries,
            "skipped_invalid": raw_candidates - prepared_entries,
            "final_entries": final_entries,
            "conflicts": conflicts,
            "included_final_entries": included_final_entries,
            "written_conflicts": conflicts,
        }

    def finalize(self, result: PipelineResult) -> PipelineResult:
        result.rows_read = int(self._metrics.get("raw_candidates", 0))
        result.rows_written = self._written_universe
        result.metrics.update(self._metrics)
        return result
