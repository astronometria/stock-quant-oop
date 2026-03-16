from __future__ import annotations

# =============================================================================
# DuckDB SEC repository
# -----------------------------------------------------------------------------
# Repository d'accès aux tables SEC dans DuckDB.
#
# Objectifs:
# - encapsuler tout le SQL SEC au même endroit
# - garder les services applicatifs minces
# - préparer les étapes de backfill historique et refresh quotidien
# - conserver une logique PIT-safe via filing_date / accepted_at / available_at
#
# Design:
# - repository orienté domaine
# - méthodes explicites
# - SQL-first pour les lectures/écritures de masse
# - Python mince pour la transformation des objets domaine
#
# Remarque:
# - ce fichier ne crée pas le schéma
# - il suppose que les tables existent déjà
# - une prochaine étape sera d'ajouter / adapter le schéma DB SEC
# =============================================================================

from dataclasses import asdict
from datetime import datetime
from typing import Any, Iterable, Sequence

from stock_quant.domain.entities.sec_filing import (
    SecFactNormalized,
    SecFiling,
    SecFilingDocument,
    SecFilingRawDocument,
    SecFilingRawIndexEntry,
    SecXbrlFactRaw,
)


class DuckDbSecRepository:
    """
    Repository DuckDB pour les objets SEC.

    Pourquoi un repository dédié:
    - éviter de disperser le SQL SEC dans plusieurs scripts CLI
    - centraliser les conventions de colonnes
    - mieux préparer les tests d'intégration
    - rendre la migration future plus simple si le schéma évolue

    Convention:
    - self.con est une connexion DuckDB active
    - les méthodes "upsert"/"insert" font de l'écriture
    - les méthodes "load"/"list" font de la lecture
    """

    def __init__(self, con: Any) -> None:
        """
        Paramètres:
        - con: connexion DuckDB active

        On n'impose pas de type DuckDB précis ici pour garder le code souple
        lors des tests et pour éviter de multiplier les dépendances de type.
        """
        self.con = con

    # =========================================================================
    # Helpers SQL internes
    # =========================================================================
    def _executemany(self, sql: str, rows: Sequence[tuple[Any, ...]]) -> int:
        """
        Exécute un INSERT/UPDATE en masse.

        Retour:
        - nombre de lignes soumises au driver

        Remarque:
        - DuckDB ne retourne pas toujours facilement un rowcount fiable
        - on retourne donc la taille de l'entrée
        """
        if not rows:
            return 0
        self.con.executemany(sql, rows)
        return len(rows)

    @staticmethod
    def _safe_datetime(value: datetime | None) -> datetime | None:
        """
        Petit helper de cohérence pour les timestamps.
        """
        return value

    # =========================================================================
    # RAW INDEX
    # =========================================================================
    def insert_sec_filing_raw_index_entries(
        self,
        entries: Iterable[SecFilingRawIndexEntry],
    ) -> int:
        """
        Insère des entrées raw d'index SEC.

        Table cible attendue:
        - sec_filing_raw_index

        Colonnes attendues:
        - cik
        - company_name
        - form_type
        - filing_date
        - accepted_at
        - accession_number
        - primary_document
        - filing_url
        - source_name
        - source_file
        - ingested_at

        Stratégie:
        - insertion append-only
        - la déduplication plus fine pourra être faite
          soit par contrainte DB, soit par étape de build
        """
        rows: list[tuple[Any, ...]] = []
        for entry in entries:
            rows.append(
                (
                    entry.cik,
                    entry.company_name,
                    entry.form_type,
                    entry.filing_date,
                    self._safe_datetime(entry.accepted_at),
                    entry.accession_number,
                    entry.primary_document,
                    entry.filing_url,
                    entry.source_name,
                    entry.source_file,
                    self._safe_datetime(entry.ingested_at),
                )
            )

        sql = """
            INSERT INTO sec_filing_raw_index (
                cik,
                company_name,
                form_type,
                filing_date,
                accepted_at,
                accession_number,
                primary_document,
                filing_url,
                source_name,
                source_file,
                ingested_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        return self._executemany(sql, rows)

    def load_sec_filing_raw_index_entries(
        self,
        limit: int = 100,
    ) -> list[SecFilingRawIndexEntry]:
        """
        Recharge des entrées raw index depuis la DB.

        Usage:
        - debug
        - tests d'intégration
        - services de build
        """
        sql = """
            SELECT
                cik,
                company_name,
                form_type,
                filing_date,
                accepted_at,
                accession_number,
                primary_document,
                filing_url,
                source_name,
                source_file,
                ingested_at
            FROM sec_filing_raw_index
            ORDER BY COALESCE(accepted_at, CAST(filing_date AS TIMESTAMP)) DESC,
                     accession_number
            LIMIT ?
        """
        rows = self.con.execute(sql, [limit]).fetchall()

        result: list[SecFilingRawIndexEntry] = []
        for row in rows:
            result.append(
                SecFilingRawIndexEntry(
                    cik=row[0],
                    company_name=row[1],
                    form_type=row[2],
                    filing_date=row[3],
                    accepted_at=row[4],
                    accession_number=row[5],
                    primary_document=row[6],
                    filing_url=row[7],
                    source_name=row[8],
                    source_file=row[9],
                    ingested_at=row[10],
                )
            )
        return result

    # =========================================================================
    # RAW DOCUMENTS
    # =========================================================================
    def insert_sec_filing_raw_documents(
        self,
        documents: Iterable[SecFilingRawDocument],
    ) -> int:
        """
        Insère des documents raw SEC.

        Table cible attendue:
        - sec_filing_raw_document
        """
        rows: list[tuple[Any, ...]] = []
        for doc in documents:
            rows.append(
                (
                    doc.accession_number,
                    doc.document_type,
                    doc.document_url,
                    doc.document_path,
                    doc.document_text,
                    doc.source_name,
                    doc.source_file,
                    self._safe_datetime(doc.ingested_at),
                )
            )

        sql = """
            INSERT INTO sec_filing_raw_document (
                accession_number,
                document_type,
                document_url,
                document_path,
                document_text,
                source_name,
                source_file,
                ingested_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """
        return self._executemany(sql, rows)

    # =========================================================================
    # RAW XBRL FACTS
    # =========================================================================
    def insert_sec_xbrl_fact_raw(
        self,
        facts: Iterable[SecXbrlFactRaw],
    ) -> int:
        """
        Insère des faits XBRL raw.

        Table cible attendue:
        - sec_xbrl_fact_raw

        Note:
        - value_text et value_numeric coexistent volontairement
        - certains concepts ne sont pas purement numériques
        """
        rows: list[tuple[Any, ...]] = []
        for fact in facts:
            rows.append(
                (
                    fact.accession_number,
                    fact.cik,
                    fact.taxonomy,
                    fact.concept,
                    fact.unit,
                    fact.period_end_date,
                    fact.period_start_date,
                    fact.fiscal_year,
                    fact.fiscal_period,
                    fact.frame,
                    fact.value_text,
                    fact.value_numeric,
                    fact.source_name,
                    fact.source_file,
                    self._safe_datetime(fact.ingested_at),
                )
            )

        sql = """
            INSERT INTO sec_xbrl_fact_raw (
                accession_number,
                cik,
                taxonomy,
                concept,
                unit,
                period_end_date,
                period_start_date,
                fiscal_year,
                fiscal_period,
                frame,
                value_text,
                value_numeric,
                source_name,
                source_file,
                ingested_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        return self._executemany(sql, rows)

    # =========================================================================
    # NORMALIZED FILINGS
    # =========================================================================
    def upsert_sec_filings(
        self,
        filings: Iterable[SecFiling],
    ) -> int:
        """
        Upsert des filings normalisés.

        Table cible attendue:
        - sec_filing

        Clé logique:
        - filing_id

        Pourquoi MERGE:
        - permet de rejouer un build sans dupliquer
        - utile pour le daily refresh incrémental
        """
        rows: list[tuple[Any, ...]] = []
        for filing in filings:
            rows.append(
                (
                    filing.filing_id,
                    filing.company_id,
                    filing.cik,
                    filing.form_type,
                    filing.filing_date,
                    self._safe_datetime(filing.accepted_at),
                    filing.accession_number,
                    filing.filing_url,
                    filing.primary_document,
                    self._safe_datetime(filing.available_at),
                    filing.source_name,
                    self._safe_datetime(filing.created_at),
                )
            )

        if not rows:
            return 0

        self.con.execute(
            """
            CREATE TEMP TABLE tmp_sec_filing_upsert (
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
        try:
            self.con.executemany(
                """
                INSERT INTO tmp_sec_filing_upsert (
                    filing_id,
                    company_id,
                    cik,
                    form_type,
                    filing_date,
                    accepted_at,
                    accession_number,
                    filing_url,
                    primary_document,
                    available_at,
                    source_name,
                    created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                rows,
            )

            self.con.execute(
                """
                MERGE INTO sec_filing AS target
                USING tmp_sec_filing_upsert AS source
                ON target.filing_id = source.filing_id
                WHEN MATCHED THEN UPDATE SET
                    company_id = source.company_id,
                    cik = source.cik,
                    form_type = source.form_type,
                    filing_date = source.filing_date,
                    accepted_at = source.accepted_at,
                    accession_number = source.accession_number,
                    filing_url = source.filing_url,
                    primary_document = source.primary_document,
                    available_at = source.available_at,
                    source_name = source.source_name,
                    created_at = source.created_at
                WHEN NOT MATCHED THEN INSERT (
                    filing_id,
                    company_id,
                    cik,
                    form_type,
                    filing_date,
                    accepted_at,
                    accession_number,
                    filing_url,
                    primary_document,
                    available_at,
                    source_name,
                    created_at
                )
                VALUES (
                    source.filing_id,
                    source.company_id,
                    source.cik,
                    source.form_type,
                    source.filing_date,
                    source.accepted_at,
                    source.accession_number,
                    source.filing_url,
                    source.primary_document,
                    source.available_at,
                    source.source_name,
                    source.created_at
                )
                """
            )
        finally:
            self.con.execute("DROP TABLE IF EXISTS tmp_sec_filing_upsert")

        return len(rows)

    def load_sec_filings(
        self,
        limit: int = 100,
    ) -> list[SecFiling]:
        """
        Recharge des filings normalisés.
        """
        sql = """
            SELECT
                filing_id,
                company_id,
                cik,
                form_type,
                filing_date,
                accepted_at,
                accession_number,
                filing_url,
                primary_document,
                available_at,
                source_name,
                created_at
            FROM sec_filing
            ORDER BY available_at DESC NULLS LAST, filing_id
            LIMIT ?
        """
        rows = self.con.execute(sql, [limit]).fetchall()

        result: list[SecFiling] = []
        for row in rows:
            result.append(
                SecFiling(
                    filing_id=row[0],
                    company_id=row[1],
                    cik=row[2],
                    form_type=row[3],
                    filing_date=row[4],
                    accepted_at=row[5],
                    accession_number=row[6],
                    filing_url=row[7],
                    primary_document=row[8],
                    available_at=row[9],
                    source_name=row[10],
                    created_at=row[11],
                )
            )
        return result

    # =========================================================================
    # NORMALIZED DOCUMENTS
    # =========================================================================
    def insert_sec_filing_documents(
        self,
        documents: Iterable[SecFilingDocument],
    ) -> int:
        """
        Insère des documents normalisés rattachés à sec_filing.
        """
        rows: list[tuple[Any, ...]] = []
        for doc in documents:
            rows.append(
                (
                    doc.filing_id,
                    doc.document_type,
                    doc.document_url,
                    doc.document_path,
                    doc.document_text,
                    self._safe_datetime(doc.created_at),
                )
            )

        sql = """
            INSERT INTO sec_filing_document (
                filing_id,
                document_type,
                document_url,
                document_path,
                document_text,
                created_at
            )
            VALUES (?, ?, ?, ?, ?, ?)
        """
        return self._executemany(sql, rows)

    # =========================================================================
    # NORMALIZED FACTS
    # =========================================================================
    def insert_sec_fact_normalized(
        self,
        facts: Iterable[SecFactNormalized],
    ) -> int:
        """
        Insère des faits normalisés.

        Table cible attendue:
        - sec_fact_normalized
        """
        rows: list[tuple[Any, ...]] = []
        for fact in facts:
            rows.append(
                (
                    fact.filing_id,
                    fact.company_id,
                    fact.cik,
                    fact.taxonomy,
                    fact.concept,
                    fact.period_end_date,
                    fact.unit,
                    fact.value_text,
                    fact.value_numeric,
                    fact.source_name,
                    self._safe_datetime(fact.created_at),
                )
            )

        sql = """
            INSERT INTO sec_fact_normalized (
                filing_id,
                company_id,
                cik,
                taxonomy,
                concept,
                period_end_date,
                unit,
                value_text,
                value_numeric,
                source_name,
                created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        return self._executemany(sql, rows)

    def load_sec_fact_normalized(
        self,
        limit: int = 100,
    ) -> list[SecFactNormalized]:
        """
        Recharge des faits normalisés pour debug/tests.
        """
        sql = """
            SELECT
                filing_id,
                company_id,
                cik,
                taxonomy,
                concept,
                period_end_date,
                unit,
                value_text,
                value_numeric,
                source_name,
                created_at
            FROM sec_fact_normalized
            ORDER BY created_at DESC NULLS LAST, filing_id
            LIMIT ?
        """
        rows = self.con.execute(sql, [limit]).fetchall()

        result: list[SecFactNormalized] = []
        for row in rows:
            result.append(
                SecFactNormalized(
                    filing_id=row[0],
                    company_id=row[1],
                    cik=row[2],
                    taxonomy=row[3],
                    concept=row[4],
                    period_end_date=row[5],
                    unit=row[6],
                    value_text=row[7],
                    value_numeric=row[8],
                    source_name=row[9],
                    created_at=row[10],
                )
            )
        return result

    # =========================================================================
    # PIT / quant helpers
    # =========================================================================
    def load_latest_available_filing_for_company_and_form(
        self,
        company_id: str,
        form_type: str,
        as_of: datetime,
    ) -> SecFiling | None:
        """
        Retourne le dernier filing disponible pour une compagnie / form
        à un instant donné.

        Cette méthode est importante pour la discipline PIT:
        - on filtre sur available_at <= as_of
        - on ne prend pas un filing publié plus tard

        Elle sera utile plus tard pour:
        - point-in-time fundamentals
        - replay historique
        - validation anti look-ahead bias
        """
        sql = """
            SELECT
                filing_id,
                company_id,
                cik,
                form_type,
                filing_date,
                accepted_at,
                accession_number,
                filing_url,
                primary_document,
                available_at,
                source_name,
                created_at
            FROM sec_filing
            WHERE company_id = ?
              AND form_type = ?
              AND available_at IS NOT NULL
              AND available_at <= ?
            ORDER BY available_at DESC, filing_id DESC
            LIMIT 1
        """
        row = self.con.execute(sql, [company_id, form_type, as_of]).fetchone()
        if row is None:
            return None

        return SecFiling(
            filing_id=row[0],
            company_id=row[1],
            cik=row[2],
            form_type=row[3],
            filing_date=row[4],
            accepted_at=row[5],
            accession_number=row[6],
            filing_url=row[7],
            primary_document=row[8],
            available_at=row[9],
            source_name=row[10],
            created_at=row[11],
        )

    def count_rows(self, table_name: str) -> int:
        """
        Helper simple de debug.

        Note:
        - table_name doit rester interne et contrôlé par le code appelant
        """
        return int(self.con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0])

    def debug_sample(self, table_name: str, limit: int = 20) -> list[dict[str, Any]]:
        """
        Retourne un échantillon générique d'une table sous forme de dict.

        Très utile pour:
        - probes
        - logs
        - validation rapide en terminal
        """
        cur = self.con.execute(f"SELECT * FROM {table_name} LIMIT {int(limit)}")
        columns = [desc[0] for desc in cur.description]
        rows = cur.fetchall()

        result: list[dict[str, Any]] = []
        for row in rows:
            result.append(dict(zip(columns, row)))
        return result
