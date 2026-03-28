"""
listing_history_schema.py

Schéma SQL-first pour la reconstruction historique des listings.
"""

from __future__ import annotations

from stock_quant.infrastructure.db.unit_of_work import DuckDbUnitOfWork


class ListingHistorySchemaManager:
    """
    Initialise le schéma historisé des listings et des dimensions dérivées.
    """

    def __init__(self, uow: DuckDbUnitOfWork) -> None:
        self.uow = uow

    def _require_connection(self):
        """
        Retourne la connexion DuckDB active.
        """
        if self.uow.connection is None:
            raise RuntimeError("DuckDbUnitOfWork has no active connection")
        return self.uow.connection

    def initialize(self) -> None:
        """
        Crée les tables historisées et les vues courantes.
        """
        con = self._require_connection()

        # ======================================================================
        # 1) listing_event_history
        # ======================================================================
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS listing_event_history (
                event_id VARCHAR PRIMARY KEY,
                symbol VARCHAR NOT NULL,
                related_symbol VARCHAR,
                company_id VARCHAR,
                cik VARCHAR,
                event_type VARCHAR NOT NULL,
                event_date DATE NOT NULL,
                effective_date DATE,
                source_name VARCHAR NOT NULL,
                source_table VARCHAR,
                source_ref VARCHAR,
                event_label VARCHAR,
                confidence_level VARCHAR NOT NULL DEFAULT 'MEDIUM',
                is_observed BOOLEAN NOT NULL DEFAULT TRUE,
                is_inferred BOOLEAN NOT NULL DEFAULT FALSE,
                raw_payload JSON,
                notes VARCHAR,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

                CHECK (
                    event_type IN (
                        'LISTING_OBSERVED',
                        'LISTING_ADDED',
                        'LISTING_REMOVED',
                        'DELISTED',
                        'SUSPENDED',
                        'REACTIVATED',
                        'TICKER_CHANGED',
                        'RENAMED',
                        'MERGED',
                        'ACQUIRED',
                        'SEC_FILER_OBSERVED',
                        'IDENTITY_LINKED'
                    )
                ),
                CHECK (confidence_level IN ('HIGH', 'MEDIUM', 'LOW')),
                CHECK (NOT (is_observed = TRUE AND is_inferred = TRUE))
            )
            """
        )
        con.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS ux_listing_event_history_natural
            ON listing_event_history (
                symbol,
                COALESCE(related_symbol, ''),
                event_type,
                event_date,
                COALESCE(effective_date, event_date),
                source_name,
                COALESCE(source_ref, '')
            )
            """
        )
        con.execute(
            """
            CREATE INDEX IF NOT EXISTS ix_listing_event_history_symbol_date
            ON listing_event_history (symbol, event_date)
            """
        )
        con.execute(
            """
            CREATE INDEX IF NOT EXISTS ix_listing_event_history_related_symbol_date
            ON listing_event_history (related_symbol, event_date)
            """
        )
        con.execute(
            """
            CREATE INDEX IF NOT EXISTS ix_listing_event_history_company_date
            ON listing_event_history (company_id, event_date)
            """
        )
        con.execute(
            """
            CREATE INDEX IF NOT EXISTS ix_listing_event_history_cik_date
            ON listing_event_history (cik, event_date)
            """
        )
        con.execute(
            """
            CREATE INDEX IF NOT EXISTS ix_listing_event_history_type_date
            ON listing_event_history (event_type, event_date)
            """
        )

        # ======================================================================
        # 2) market_universe_history
        # ======================================================================
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS market_universe_history (
                snapshot_row_id VARCHAR PRIMARY KEY,
                snapshot_date DATE NOT NULL,
                symbol VARCHAR NOT NULL,
                company_id VARCHAR,
                cik VARCHAR,
                source_name VARCHAR NOT NULL,
                source_table VARCHAR,
                source_ref VARCHAR,
                exchange VARCHAR,
                security_name VARCHAR,
                is_observed BOOLEAN NOT NULL DEFAULT TRUE,
                raw_payload JSON,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

                CHECK (is_observed = TRUE)
            )
            """
        )
        con.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS ux_market_universe_history_natural
            ON market_universe_history (
                snapshot_date,
                symbol,
                source_name,
                COALESCE(source_ref, '')
            )
            """
        )
        con.execute(
            """
            CREATE INDEX IF NOT EXISTS ix_market_universe_history_symbol_date
            ON market_universe_history (symbol, snapshot_date)
            """
        )
        con.execute(
            """
            CREATE INDEX IF NOT EXISTS ix_market_universe_history_company_date
            ON market_universe_history (company_id, snapshot_date)
            """
        )
        con.execute(
            """
            CREATE INDEX IF NOT EXISTS ix_market_universe_history_cik_date
            ON market_universe_history (cik, snapshot_date)
            """
        )

        # ======================================================================
        # 3) symbol_reference_history
        # ======================================================================
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS symbol_reference_history (
                reference_row_id VARCHAR PRIMARY KEY,
                symbol VARCHAR NOT NULL,
                company_id VARCHAR,
                cik VARCHAR,
                company_name VARCHAR,
                security_name VARCHAR,
                exchange VARCHAR,
                asset_type VARCHAR,
                valid_from DATE NOT NULL,
                valid_to_exclusive DATE,
                is_primary_symbol BOOLEAN NOT NULL DEFAULT TRUE,
                reference_status VARCHAR NOT NULL DEFAULT 'ACTIVE_MAPPING',
                source_name VARCHAR NOT NULL,
                source_table VARCHAR,
                source_ref VARCHAR,
                confidence_level VARCHAR NOT NULL DEFAULT 'MEDIUM',
                is_observed BOOLEAN NOT NULL DEFAULT TRUE,
                is_inferred BOOLEAN NOT NULL DEFAULT FALSE,
                raw_payload JSON,
                notes VARCHAR,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

                CHECK (
                    reference_status IN (
                        'ACTIVE_MAPPING',
                        'FORMER_MAPPING',
                        'ALIAS_MAPPING',
                        'CONFLICTED',
                        'UNRESOLVED'
                    )
                ),
                CHECK (confidence_level IN ('HIGH', 'MEDIUM', 'LOW')),
                CHECK (valid_to_exclusive IS NULL OR valid_to_exclusive > valid_from),
                CHECK (NOT (is_observed = TRUE AND is_inferred = TRUE))
            )
            """
        )
        con.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS ux_symbol_reference_history_natural
            ON symbol_reference_history (
                symbol,
                COALESCE(company_id, ''),
                COALESCE(cik, ''),
                valid_from,
                COALESCE(valid_to_exclusive, DATE '9999-12-31'),
                source_name,
                COALESCE(source_ref, '')
            )
            """
        )
        con.execute(
            """
            CREATE INDEX IF NOT EXISTS ix_symbol_reference_history_symbol_valid
            ON symbol_reference_history (symbol, valid_from, valid_to_exclusive)
            """
        )
        con.execute(
            """
            CREATE INDEX IF NOT EXISTS ix_symbol_reference_history_company_valid
            ON symbol_reference_history (company_id, valid_from, valid_to_exclusive)
            """
        )
        con.execute(
            """
            CREATE INDEX IF NOT EXISTS ix_symbol_reference_history_cik_valid
            ON symbol_reference_history (cik, valid_from, valid_to_exclusive)
            """
        )

        # ======================================================================
        # 4) listing_status_history
        # ======================================================================
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS listing_status_history (
                status_row_id VARCHAR PRIMARY KEY,
                symbol VARCHAR NOT NULL,
                company_id VARCHAR,
                cik VARCHAR,
                listing_status VARCHAR NOT NULL,
                status_start_date DATE NOT NULL,
                status_end_date DATE,
                status_end_exclusive DATE,
                is_open_ended BOOLEAN NOT NULL DEFAULT FALSE,
                derived_from_event_count INTEGER NOT NULL DEFAULT 0,
                derived_from_snapshot_count INTEGER NOT NULL DEFAULT 0,
                derivation_method VARCHAR NOT NULL,
                confidence_level VARCHAR NOT NULL DEFAULT 'MEDIUM',
                is_observed_boundary_start BOOLEAN NOT NULL DEFAULT FALSE,
                is_observed_boundary_end BOOLEAN NOT NULL DEFAULT FALSE,
                is_inferred_start BOOLEAN NOT NULL DEFAULT TRUE,
                is_inferred_end BOOLEAN NOT NULL DEFAULT TRUE,
                primary_source_summary VARCHAR,
                audit_batch_id VARCHAR,
                notes VARCHAR,
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

                CHECK (
                    listing_status IN (
                        'ACTIVE',
                        'INACTIVE',
                        'DELISTED',
                        'SUSPENDED',
                        'TRANSITIONED',
                        'UNKNOWN'
                    )
                ),
                CHECK (confidence_level IN ('HIGH', 'MEDIUM', 'LOW')),
                CHECK (
                    status_end_exclusive IS NULL OR status_end_exclusive > status_start_date
                ),
                CHECK (
                    status_end_date IS NULL OR status_end_date >= status_start_date
                ),
                CHECK (
                    NOT (is_open_ended = TRUE AND status_end_exclusive IS NOT NULL)
                ),
                CHECK (
                    derived_from_event_count >= 0 AND derived_from_snapshot_count >= 0
                )
            )
            """
        )
        con.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS ux_listing_status_history_natural
            ON listing_status_history (
                symbol,
                listing_status,
                status_start_date,
                COALESCE(status_end_exclusive, DATE '9999-12-31'),
                COALESCE(company_id, ''),
                COALESCE(cik, '')
            )
            """
        )
        con.execute(
            """
            CREATE INDEX IF NOT EXISTS ix_listing_status_history_symbol_range
            ON listing_status_history (symbol, status_start_date, status_end_exclusive)
            """
        )
        con.execute(
            """
            CREATE INDEX IF NOT EXISTS ix_listing_status_history_company_range
            ON listing_status_history (company_id, status_start_date, status_end_exclusive)
            """
        )
        con.execute(
            """
            CREATE INDEX IF NOT EXISTS ix_listing_status_history_cik_range
            ON listing_status_history (cik, status_start_date, status_end_exclusive)
            """
        )
        con.execute(
            """
            CREATE INDEX IF NOT EXISTS ix_listing_status_history_status_range
            ON listing_status_history (listing_status, status_start_date, status_end_exclusive)
            """
        )

        # ======================================================================
        # 5) history_reconstruction_audit
        # ======================================================================
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS history_reconstruction_audit (
                audit_row_id VARCHAR PRIMARY KEY,
                run_id VARCHAR NOT NULL,
                target_table VARCHAR NOT NULL,
                target_key VARCHAR NOT NULL,
                action_type VARCHAR NOT NULL,
                reason_code VARCHAR NOT NULL,
                rule_name VARCHAR NOT NULL,
                input_source_summary VARCHAR,
                input_row_refs JSON,
                old_values JSON,
                new_values JSON,
                confidence_level VARCHAR NOT NULL DEFAULT 'MEDIUM',
                created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

                CHECK (
                    action_type IN (
                        'INSERT',
                        'UPDATE',
                        'CLOSE_INTERVAL',
                        'OPEN_INTERVAL',
                        'LINK_IDENTITY',
                        'SKIP_CONFLICT',
                        'MERGE_INTERVALS'
                    )
                ),
                CHECK (confidence_level IN ('HIGH', 'MEDIUM', 'LOW'))
            )
            """
        )
        con.execute(
            """
            CREATE INDEX IF NOT EXISTS ix_history_reconstruction_audit_run
            ON history_reconstruction_audit (run_id, created_at)
            """
        )
        con.execute(
            """
            CREATE INDEX IF NOT EXISTS ix_history_reconstruction_audit_target
            ON history_reconstruction_audit (target_table, target_key, created_at)
            """
        )
        con.execute(
            """
            CREATE INDEX IF NOT EXISTS ix_history_reconstruction_audit_reason
            ON history_reconstruction_audit (reason_code, created_at)
            """
        )

        # ======================================================================
        # 6) Vues courantes de compatibilité
        # ======================================================================
        con.execute(
            """
            CREATE OR REPLACE VIEW market_universe_current AS
            SELECT
                snapshot_row_id,
                snapshot_date,
                symbol,
                company_id,
                cik,
                source_name,
                source_table,
                source_ref,
                exchange,
                security_name,
                is_observed,
                raw_payload,
                created_at
            FROM (
                SELECT
                    *,
                    ROW_NUMBER() OVER (
                        PARTITION BY symbol
                        ORDER BY snapshot_date DESC, created_at DESC
                    ) AS rn
                FROM market_universe_history
            )
            WHERE rn = 1
            """
        )

        con.execute(
            """
            CREATE OR REPLACE VIEW symbol_reference_current AS
            SELECT
                reference_row_id,
                symbol,
                company_id,
                cik,
                company_name,
                security_name,
                exchange,
                asset_type,
                valid_from,
                valid_to_exclusive,
                is_primary_symbol,
                reference_status,
                source_name,
                source_table,
                source_ref,
                confidence_level,
                is_observed,
                is_inferred,
                raw_payload,
                notes,
                created_at
            FROM (
                SELECT
                    *,
                    ROW_NUMBER() OVER (
                        PARTITION BY symbol
                        ORDER BY
                            CASE WHEN valid_to_exclusive IS NULL THEN 0 ELSE 1 END,
                            valid_from DESC,
                            created_at DESC
                    ) AS rn
                FROM symbol_reference_history
                WHERE valid_to_exclusive IS NULL
                   OR CURRENT_DATE < valid_to_exclusive
            )
            WHERE rn = 1
            """
        )
