from __future__ import annotations

from stock_quant.app.dto.pipeline_result import PipelineResult
from stock_quant.infrastructure.db.unit_of_work import DuckDbUnitOfWork
from stock_quant.pipelines.base_pipeline import BasePipeline
from stock_quant.shared.exceptions import PipelineError


class BuildSymbolReferencePipeline(BasePipeline):
    pipeline_name = "build_symbol_reference"

    def __init__(self, repository=None, uow: DuckDbUnitOfWork | None = None) -> None:
        if repository is not None:
            self.uow = repository.uow
        else:
            self.uow = uow

        if self.uow is None:
            raise ValueError("BuildSymbolReferencePipeline requires repository or uow")

        self._metrics: dict[str, int] = {}
        self._rows_written = 0

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
        input_entries = int(
            self.con.execute(
                "SELECT COUNT(*) FROM market_universe WHERE include_in_universe = TRUE"
            ).fetchone()[0]
        )
        if input_entries == 0:
            raise PipelineError("no included market_universe rows available")

    def load(self, data) -> None:
        con = self.con

        input_entries = int(
            con.execute(
                "SELECT COUNT(*) FROM market_universe WHERE include_in_universe = TRUE"
            ).fetchone()[0]
        )

        con.execute("DELETE FROM symbol_reference")

        con.execute("DROP TABLE IF EXISTS tmp_symbol_reference_base")
        con.execute(
            """
            CREATE TEMP TABLE tmp_symbol_reference_base AS
            SELECT
                symbol,
                cik,
                company_name,
                exchange_normalized AS exchange,
                source_name,
                TRIM(
                    UPPER(
                        REGEXP_REPLACE(
                            REGEXP_REPLACE(TRIM(company_name), '[^A-Za-z0-9 ]+', ' ', 'g'),
                            '\s+',
                            ' ',
                            'g'
                        )
                    )
                ) AS company_name_clean
            FROM market_universe
            WHERE include_in_universe = TRUE
              AND NULLIF(TRIM(company_name), '') IS NOT NULL
            """
        )

        con.execute("DROP TABLE IF EXISTS tmp_symbol_reference_alias_seed")
        con.execute(
            """
            CREATE TEMP TABLE tmp_symbol_reference_alias_seed AS
            WITH stripped_once AS (
                SELECT
                    symbol,
                    cik,
                    company_name,
                    company_name_clean,
                    exchange,
                    source_name,
                    TRIM(
                        REGEXP_REPLACE(
                            REGEXP_REPLACE(
                                REGEXP_REPLACE(
                                    REGEXP_REPLACE(
                                        REGEXP_REPLACE(
                                            REGEXP_REPLACE(
                                                REGEXP_REPLACE(
                                                    REGEXP_REPLACE(
                                                        REGEXP_REPLACE(
                                                            REGEXP_REPLACE(
                                                                REGEXP_REPLACE(
                                                                    REGEXP_REPLACE(
                                                                        REGEXP_REPLACE(
                                                                            REGEXP_REPLACE(
                                                                                REGEXP_REPLACE(
                                                                                    REGEXP_REPLACE(
                                                                                        REGEXP_REPLACE(
                                                                                            REGEXP_REPLACE(
                                                                                                REGEXP_REPLACE(
                                                                                                    company_name_clean,
                                                                                                    ' INCORPORATED$', '',
                                                                                                    'g'
                                                                                                ),
                                                                                                ' INC$', '',
                                                                                                'g'
                                                                                            ),
                                                                                            ' CORPORATION$', '',
                                                                                            'g'
                                                                                        ),
                                                                                        ' CORP$', '',
                                                                                        'g'
                                                                                    ),
                                                                                    ' COMPANY$', '',
                                                                                    'g'
                                                                                ),
                                                                                ' CO$', '',
                                                                                'g'
                                                                            ),
                                                                            ' LIMITED$', '',
                                                                            'g'
                                                                        ),
                                                                        ' LTD$', '',
                                                                        'g'
                                                                    ),
                                                                    ' HOLDINGS$', '',
                                                                    'g'
                                                                ),
                                                                ' HOLDING$', '',
                                                                'g'
                                                            ),
                                                            ' GROUP$', '',
                                                            'g'
                                                        ),
                                                        ' PLC$', '',
                                                        'g'
                                                    ),
                                                    ' SA$', '',
                                                    'g'
                                                ),
                                                ' NV$', '',
                                                'g'
                                            ),
                                            ' AG$', '',
                                            'g'
                                        ),
                                        ' LP$', '',
                                        'g'
                                    ),
                                    ' LLC$', '',
                                    'g'
                                ),
                                ' ADR$', '',
                                'g'
                            ),
                            ' ADS$',
                            '',
                            'g'
                        )
                    ) AS stripped_name
                FROM tmp_symbol_reference_base
            )
            SELECT
                symbol,
                cik,
                company_name,
                company_name_clean,
                exchange,
                source_name,
                stripped_name
            FROM stripped_once
            """
        )

        con.execute(
            """
            INSERT INTO symbol_reference (
                symbol,
                cik,
                company_name,
                company_name_clean,
                aliases_json,
                exchange,
                source_name,
                symbol_match_enabled,
                name_match_enabled,
                created_at
            )
            SELECT
                symbol,
                cik,
                company_name,
                company_name_clean,
                CASE
                    WHEN stripped_name IS NOT NULL
                         AND stripped_name <> ''
                         AND stripped_name <> company_name_clean
                    THEN
                        '["'
                        || REPLACE(company_name_clean, '"', '\\"')
                        || '", "'
                        || REPLACE(stripped_name, '"', '\\"')
                        || '"]'
                    ELSE
                        '["'
                        || REPLACE(company_name_clean, '"', '\\"')
                        || '"]'
                END AS aliases_json,
                exchange,
                source_name,
                CASE
                    WHEN symbol IS NOT NULL AND TRIM(symbol) <> '' THEN TRUE
                    ELSE FALSE
                END AS symbol_match_enabled,
                CASE
                    WHEN company_name_clean IS NOT NULL
                         AND LENGTH(company_name_clean) >= 3
                    THEN TRUE
                    ELSE FALSE
                END AS name_match_enabled,
                CURRENT_TIMESTAMP
            FROM tmp_symbol_reference_alias_seed
            """
        )

        output_entries = int(con.execute("SELECT COUNT(*) FROM symbol_reference").fetchone()[0])
        name_match_enabled_count = int(
            con.execute(
                "SELECT COUNT(*) FROM symbol_reference WHERE name_match_enabled = TRUE"
            ).fetchone()[0]
        )
        symbol_match_enabled_count = int(
            con.execute(
                "SELECT COUNT(*) FROM symbol_reference WHERE symbol_match_enabled = TRUE"
            ).fetchone()[0]
        )

        self._rows_written = output_entries
        self._metrics = {
            "input_entries": input_entries,
            "output_entries": output_entries,
            "name_match_enabled_count": name_match_enabled_count,
            "symbol_match_enabled_count": symbol_match_enabled_count,
        }

    def finalize(self, result: PipelineResult) -> PipelineResult:
        result.rows_read = int(self._metrics.get("input_entries", 0))
        result.rows_written = self._rows_written
        result.metrics.update(self._metrics)
        return result
