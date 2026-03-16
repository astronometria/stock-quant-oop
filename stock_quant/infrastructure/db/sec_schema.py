from __future__ import annotations

# =============================================================================
# SEC schema manager
# -----------------------------------------------------------------------------
# Objectif
# --------
# Initialiser et faire évoluer le schéma SEC de façon rétrocompatible.
#
# Pourquoi ce fichier est important
# ---------------------------------
# Le repository SEC et les services applicatifs utilisent plus de colonnes que
# le schéma SEC initial. Si on ne réaligne pas le schéma maintenant:
# - les inserts vont casser plus tard
# - certaines informations de traçabilité seront perdues
# - la reconstruction point-in-time (PIT) deviendra fragile
#
# Principes retenus
# -----------------
# - aucune suppression destructive ici
# - CREATE TABLE IF NOT EXISTS pour les nouvelles bases
# - ALTER TABLE ADD COLUMN IF NOT EXISTS pour les bases déjà existantes
# - ajout d'index utiles pour les lectures PIT / historiques
#
# Notes bias / PIT
# ----------------
# - available_at doit être conservé quand possible
# - on sépare la date de période (period_end_date) de la date de disponibilité
# - on conserve accession_number / cik / source_file pour traçabilité
# =============================================================================

from stock_quant.infrastructure.db.unit_of_work import DuckDbUnitOfWork


class SecSchemaManager:
    """
    Gestionnaire du schéma SEC.

    Ce manager peut être appelé:
    - sur une base neuve
    - sur une base existante à migrer en douceur

    Il ne détruit pas les tables existantes.
    """

    def __init__(self, uow: DuckDbUnitOfWork) -> None:
        self.uow = uow

    @property
    def con(self):
        """
        Retourne la connexion DuckDB active.

        On garde ce guard explicite pour éviter les erreurs silencieuses lors
        d'appels de migration sans transaction/connexion active.
        """
        if self.uow.connection is None:
            raise RuntimeError("active DB connection is required")
        return self.uow.connection

    def initialize(self) -> None:
        """
        Point d'entrée principal.

        Ordre:
        1) créer les tables
        2) ajouter les colonnes manquantes si la base existe déjà
        3) créer les index utiles
        """
        self._create_sec_filing_raw_index()
        self._create_sec_filing_raw_document()
        self._create_sec_xbrl_fact_raw()
        self._create_sec_filing()
        self._create_sec_filing_document()
        self._create_sec_fact_normalized()

        self._migrate_sec_filing_raw_index()
        self._migrate_sec_filing_raw_document()
        self._migrate_sec_xbrl_fact_raw()
        self._migrate_sec_filing()
        self._migrate_sec_filing_document()
        self._migrate_sec_fact_normalized()

        self._create_indexes()

    # =========================================================================
    # Table creation
    # =========================================================================
    def _create_sec_filing_raw_index(self) -> None:
        self.con.execute(
            """
            CREATE TABLE IF NOT EXISTS sec_filing_raw_index (
                cik VARCHAR,
                company_name VARCHAR,
                form_type VARCHAR,
                filing_date DATE,
                accepted_at TIMESTAMP,
                accession_number VARCHAR,
                primary_document VARCHAR,
                filing_url VARCHAR,
                source_name VARCHAR,
                source_file VARCHAR,
                ingested_at TIMESTAMP
            )
            """
        )

    def _create_sec_filing_raw_document(self) -> None:
        self.con.execute(
            """
            CREATE TABLE IF NOT EXISTS sec_filing_raw_document (
                accession_number VARCHAR,
                document_type VARCHAR,
                document_url VARCHAR,
                document_path VARCHAR,
                document_text VARCHAR,
                source_name VARCHAR,
                source_file VARCHAR,
                ingested_at TIMESTAMP
            )
            """
        )

    def _create_sec_xbrl_fact_raw(self) -> None:
        self.con.execute(
            """
            CREATE TABLE IF NOT EXISTS sec_xbrl_fact_raw (
                accession_number VARCHAR,
                cik VARCHAR,
                taxonomy VARCHAR,
                concept VARCHAR,
                unit VARCHAR,
                period_end_date DATE,
                period_start_date DATE,
                fiscal_year INTEGER,
                fiscal_period VARCHAR,
                frame VARCHAR,
                value_text VARCHAR,
                value_numeric DOUBLE,
                source_name VARCHAR,
                source_file VARCHAR,
                ingested_at TIMESTAMP
            )
            """
        )

    def _create_sec_filing(self) -> None:
        self.con.execute(
            """
            CREATE TABLE IF NOT EXISTS sec_filing (
                filing_id VARCHAR,
                company_id VARCHAR,
                cik VARCHAR,
                form_type VARCHAR,
                filing_date DATE,
                accepted_at TIMESTAMP,
                accession_number VARCHAR,
                filing_url VARCHAR,
                primary_document VARCHAR,
                available_at TIMESTAMP,
                source_name VARCHAR,
                created_at TIMESTAMP
            )
            """
        )

    def _create_sec_filing_document(self) -> None:
        self.con.execute(
            """
            CREATE TABLE IF NOT EXISTS sec_filing_document (
                filing_id VARCHAR,
                document_type VARCHAR,
                document_url VARCHAR,
                document_path VARCHAR,
                document_text VARCHAR,
                created_at TIMESTAMP
            )
            """
        )

    def _create_sec_fact_normalized(self) -> None:
        self.con.execute(
            """
            CREATE TABLE IF NOT EXISTS sec_fact_normalized (
                filing_id VARCHAR,
                company_id VARCHAR,
                cik VARCHAR,
                taxonomy VARCHAR,
                concept VARCHAR,
                period_end_date DATE,
                unit VARCHAR,
                value_text VARCHAR,
                value_numeric DOUBLE,
                available_at TIMESTAMP,
                source_name VARCHAR,
                created_at TIMESTAMP
            )
            """
        )

    # =========================================================================
    # Backward-compatible migrations
    # =========================================================================
    def _migrate_sec_filing_raw_index(self) -> None:
        self._add_column_if_missing("sec_filing_raw_index", "source_file", "VARCHAR")

    def _migrate_sec_filing_raw_document(self) -> None:
        self._add_column_if_missing("sec_filing_raw_document", "document_path", "VARCHAR")
        self._add_column_if_missing("sec_filing_raw_document", "source_file", "VARCHAR")

    def _migrate_sec_xbrl_fact_raw(self) -> None:
        self._add_column_if_missing("sec_xbrl_fact_raw", "cik", "VARCHAR")
        self._add_column_if_missing("sec_xbrl_fact_raw", "unit", "VARCHAR")
        self._add_column_if_missing("sec_xbrl_fact_raw", "period_start_date", "DATE")
        self._add_column_if_missing("sec_xbrl_fact_raw", "fiscal_year", "INTEGER")
        self._add_column_if_missing("sec_xbrl_fact_raw", "fiscal_period", "VARCHAR")
        self._add_column_if_missing("sec_xbrl_fact_raw", "frame", "VARCHAR")
        self._add_column_if_missing("sec_xbrl_fact_raw", "source_file", "VARCHAR")

    def _migrate_sec_filing(self) -> None:
        self._add_column_if_missing("sec_filing", "available_at", "TIMESTAMP")
        self._add_column_if_missing("sec_filing", "company_id", "VARCHAR")
        self._add_column_if_missing("sec_filing", "created_at", "TIMESTAMP")

    def _migrate_sec_filing_document(self) -> None:
        self._add_column_if_missing("sec_filing_document", "document_path", "VARCHAR")

    def _migrate_sec_fact_normalized(self) -> None:
        self._add_column_if_missing("sec_fact_normalized", "available_at", "TIMESTAMP")
        self._add_column_if_missing("sec_fact_normalized", "created_at", "TIMESTAMP")
        self._add_column_if_missing("sec_fact_normalized", "company_id", "VARCHAR")
        self._add_column_if_missing("sec_fact_normalized", "cik", "VARCHAR")

    # =========================================================================
    # Indexes
    # =========================================================================
    def _create_indexes(self) -> None:
        """
        Indexes utiles pour:
        - chargements incrémentaux
        - recherche par accession_number
        - reconstruction point-in-time via available_at
        - lookup par company_id / cik / concept / période
        """
        self._create_index_if_possible(
            "idx_sec_filing_raw_index_accession",
            "sec_filing_raw_index",
            "accession_number",
        )
        self._create_index_if_possible(
            "idx_sec_filing_raw_index_cik_filing_date",
            "sec_filing_raw_index",
            "cik, filing_date",
        )
        self._create_index_if_possible(
            "idx_sec_filing_filing_id",
            "sec_filing",
            "filing_id",
        )
        self._create_index_if_possible(
            "idx_sec_filing_company_available_at",
            "sec_filing",
            "company_id, available_at",
        )
        self._create_index_if_possible(
            "idx_sec_filing_cik_form_available_at",
            "sec_filing",
            "cik, form_type, available_at",
        )
        self._create_index_if_possible(
            "idx_sec_fact_normalized_company_period",
            "sec_fact_normalized",
            "company_id, period_end_date",
        )
        self._create_index_if_possible(
            "idx_sec_fact_normalized_company_available_at",
            "sec_fact_normalized",
            "company_id, available_at",
        )
        self._create_index_if_possible(
            "idx_sec_fact_normalized_concept_period",
            "sec_fact_normalized",
            "concept, period_end_date",
        )
        self._create_index_if_possible(
            "idx_sec_xbrl_fact_raw_accession_concept",
            "sec_xbrl_fact_raw",
            "accession_number, concept",
        )

    # =========================================================================
    # Helpers
    # =========================================================================
    def _add_column_if_missing(self, table_name: str, column_name: str, column_type: str) -> None:
        """
        Ajoute une colonne si elle n'existe pas déjà.

        Ce helper permet de garder le code de migration lisible et répétable.
        """
        self.con.execute(
            f"""
            ALTER TABLE {table_name}
            ADD COLUMN IF NOT EXISTS {column_name} {column_type}
            """
        )

    def _create_index_if_possible(self, index_name: str, table_name: str, columns_sql: str) -> None:
        """
        Crée un index si possible.

        On reste défensif: DuckDB peut lever si un index existe déjà dans
        certains contextes/versionnements. On ignore volontairement l'erreur.
        """
        try:
            self.con.execute(
                f"""
                CREATE INDEX IF NOT EXISTS {index_name}
                ON {table_name} ({columns_sql})
                """
            )
        except Exception:
            # On ne casse pas l'initialisation du schéma pour un index.
            pass
