from __future__ import annotations

from stock_quant.app.dto.pipeline_result import PipelineResult
from stock_quant.infrastructure.db.unit_of_work import DuckDbUnitOfWork
from stock_quant.pipelines.base_pipeline import BasePipeline
from stock_quant.shared.exceptions import PipelineError


class BuildMasterDataPipeline(BasePipeline):
    pipeline_name = "build_master_data"

    def __init__(self, repository=None, uow: DuckDbUnitOfWork | None = None) -> None:
        if repository is not None:
            self.uow = repository.uow
        else:
            self.uow = uow

        if self.uow is None:
            raise ValueError("BuildMasterDataPipeline requires repository or uow")

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
        market_universe_rows = int(
            self.con.execute("SELECT COUNT(*) FROM market_universe").fetchone()[0]
        )
        if market_universe_rows == 0:
            raise PipelineError("no market_universe rows available")

    def load(self, data) -> None:
        con = self.con

        market_universe_rows = int(con.execute("SELECT COUNT(*) FROM market_universe").fetchone()[0])
        symbol_reference_rows = int(con.execute("SELECT COUNT(*) FROM symbol_reference").fetchone()[0])

        con.execute("DELETE FROM company_master")
        con.execute("DELETE FROM instrument_master")
        con.execute("DELETE FROM ticker_history")
        con.execute("DELETE FROM instrument_identifier_map")

        con.execute("DROP TABLE IF EXISTS tmp_master_base")
        con.execute(
            """
            CREATE TEMP TABLE tmp_master_base AS
            SELECT
                mu.symbol,
                mu.company_name,
                mu.cik,
                mu.exchange_normalized AS exchange,
                mu.security_type,
                mu.include_in_universe,
                mu.as_of_date,
                sr.company_name AS sr_company_name,
                sr.exchange AS sr_exchange
            FROM market_universe mu
            LEFT JOIN symbol_reference sr
              ON UPPER(TRIM(mu.symbol)) = UPPER(TRIM(sr.symbol))
            """
        )

        con.execute("DROP TABLE IF EXISTS tmp_company_master_stage")
        con.execute(
            """
            CREATE TEMP TABLE tmp_company_master_stage AS
            SELECT DISTINCT
                CASE
                    WHEN cik IS NOT NULL AND TRIM(cik) <> ''
                    THEN 'COMP:' || TRIM(cik)
                    ELSE 'COMP:HASH:' || SUBSTR(sha1(
                        UPPER(TRIM(COALESCE(company_name, '')))
                        || '|' ||
                        UPPER(TRIM(COALESCE(symbol, '')))
                    ), 1, 16)
                END AS company_id,
                COALESCE(NULLIF(TRIM(company_name), ''), NULLIF(TRIM(sr_company_name), ''), TRIM(symbol)) AS company_name,
                NULLIF(TRIM(cik), '') AS cik,
                UPPER(TRIM(COALESCE(company_name, sr_company_name, symbol))) AS issuer_name_normalized,
                'US' AS country_code
            FROM tmp_master_base
            """
        )

        con.execute(
            """
            INSERT INTO company_master (
                company_id,
                company_name,
                cik,
                issuer_name_normalized,
                country_code,
                created_at
            )
            SELECT
                company_id,
                company_name,
                cik,
                issuer_name_normalized,
                country_code,
                CURRENT_TIMESTAMP
            FROM tmp_company_master_stage
            """
        )

        written_companies = int(con.execute("SELECT COUNT(*) FROM company_master").fetchone()[0])

        con.execute("DROP TABLE IF EXISTS tmp_instrument_master_stage")
        con.execute(
            """
            CREATE TEMP TABLE tmp_instrument_master_stage AS
            SELECT
                'INST:' || SUBSTR(sha1(
                    UPPER(TRIM(COALESCE(b.symbol, '')))
                    || '|' ||
                    UPPER(TRIM(COALESCE(b.exchange, '')))
                    || '|' ||
                    UPPER(TRIM(COALESCE(b.security_type, '')))
                ), 1, 16) AS instrument_id,
                c.company_id,
                b.symbol,
                b.exchange,
                b.security_type,
                NULL AS share_class,
                CAST(b.include_in_universe AS BOOLEAN) AS is_active,
                b.as_of_date
            FROM tmp_master_base b
            INNER JOIN tmp_company_master_stage c
              ON (
                   (b.cik IS NOT NULL AND c.cik = b.cik)
                   OR (
                       b.cik IS NULL
                       AND c.cik IS NULL
                       AND UPPER(TRIM(c.company_name)) = UPPER(TRIM(COALESCE(b.company_name, b.sr_company_name, b.symbol)))
                   )
                 )
            """
        )

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
                instrument_id,
                company_id,
                symbol,
                exchange,
                security_type,
                share_class,
                is_active,
                CURRENT_TIMESTAMP
            FROM tmp_instrument_master_stage
            """
        )

        written_instruments = int(con.execute("SELECT COUNT(*) FROM instrument_master").fetchone()[0])

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
                instrument_id,
                symbol,
                exchange,
                as_of_date AS valid_from,
                NULL AS valid_to,
                is_active AS is_current,
                CURRENT_TIMESTAMP
            FROM tmp_instrument_master_stage
            """
        )

        written_ticker_history = int(con.execute("SELECT COUNT(*) FROM ticker_history").fetchone()[0])

        con.execute(
            """
            INSERT INTO instrument_identifier_map (
                instrument_id,
                company_id,
                identifier_type,
                identifier_value,
                is_primary,
                created_at
            )
            SELECT
                instrument_id,
                company_id,
                'ticker' AS identifier_type,
                symbol AS identifier_value,
                TRUE AS is_primary,
                CURRENT_TIMESTAMP
            FROM tmp_instrument_master_stage
            UNION ALL
            SELECT
                i.instrument_id,
                i.company_id,
                'cik' AS identifier_type,
                c.cik AS identifier_value,
                TRUE AS is_primary,
                CURRENT_TIMESTAMP
            FROM tmp_instrument_master_stage i
            INNER JOIN tmp_company_master_stage c
              ON i.company_id = c.company_id
            WHERE c.cik IS NOT NULL AND TRIM(c.cik) <> ''
            """
        )

        written_identifier_map = int(
            con.execute("SELECT COUNT(*) FROM instrument_identifier_map").fetchone()[0]
        )

        self._rows_written = (
            written_companies
            + written_instruments
            + written_ticker_history
            + written_identifier_map
        )
        self._metrics = {
            "market_universe_rows": market_universe_rows,
            "symbol_reference_rows": symbol_reference_rows,
            "company_master_rows": written_companies,
            "instrument_master_rows": written_instruments,
            "ticker_history_rows": written_ticker_history,
            "identifier_map_rows": written_identifier_map,
            "written_companies": written_companies,
            "written_instruments": written_instruments,
            "written_ticker_history": written_ticker_history,
            "written_identifier_map": written_identifier_map,
        }

    def finalize(self, result: PipelineResult) -> PipelineResult:
        result.rows_read = int(
            self._metrics.get("market_universe_rows", 0)
            + self._metrics.get("symbol_reference_rows", 0)
        )
        result.rows_written = self._rows_written
        result.metrics.update(self._metrics)
        return result
