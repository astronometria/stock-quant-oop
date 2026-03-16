from __future__ import annotations

# =============================================================================
# sec_bulk_disk_loader.py
# -----------------------------------------------------------------------------
# Charge les fichiers bulk SEC déjà extraits sur disque vers les tables raw
# DuckDB:
#   - sec_filing_raw_index
#   - sec_xbrl_fact_raw
#
# Correctif important de cette version:
# - companyfacts n'utilise plus executemany() ligne par ligne
# - les faits sont désormais écrits en CSV temporaire par gros chunks
# - DuckDB charge ensuite ces chunks en bulk
#
# Pourquoi:
# - le profiling a montré que lecture disque + json.loads + itération Python
#   sont très rapides
# - le vrai goulot est l'insertion DuckDB via executemany()
# - COPY / read_csv_auto est beaucoup plus adapté à un volume massif
#
# Philosophie:
# - parsing Python mince
# - staging disque temporaire
# - bulk load SQL-first vers DuckDB
# - progression tqdm visible
# =============================================================================

import csv
import json
import tempfile
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from duckdb import DuckDBPyConnection
from tqdm import tqdm


@dataclass(frozen=True)
class SecBulkRawLoadMetrics:
    submission_files_seen: int
    submission_rows_written: int
    companyfacts_files_seen: int
    xbrl_fact_rows_written: int


class SecBulkDiskLoader:
    """
    Loader bulk SEC disque -> raw DuckDB.

    Entrées disque attendues:
    - data/sec/bulk/submissions/**/CIK*.json
    - data/sec/bulk/companyfacts/**/CIK*.json
    """

    def __init__(
        self,
        *,
        con: DuckDBPyConnection,
        data_root: str | Path,
        insert_batch_size: int = 5000,
        companyfacts_csv_chunk_rows: int = 250_000,
    ) -> None:
        self._con = con
        self._data_root = Path(data_root).expanduser()
        self._submissions_root = self._data_root / "sec" / "bulk" / "submissions"
        self._companyfacts_root = self._data_root / "sec" / "bulk" / "companyfacts"
        self._insert_batch_size = int(insert_batch_size)

        # ---------------------------------------------------------------------
        # Taille de chunk CSV pour companyfacts.
        #
        # Idée:
        # - assez gros pour amortir le coût SQL
        # - pas trop gros pour éviter un staging monstrueux en RAM
        # ---------------------------------------------------------------------
        self._companyfacts_csv_chunk_rows = int(companyfacts_csv_chunk_rows)

    # =========================================================================
    # API publique
    # =========================================================================
    def load_raw_from_disk(
        self,
        *,
        load_submissions: bool = True,
        load_companyfacts: bool = True,
        truncate_before_load: bool = False,
        limit_ciks: int | None = None,
    ) -> SecBulkRawLoadMetrics:
        """
        Charge les fichiers bulk extraits vers les tables raw.

        Paramètres:
        - load_submissions: peuple sec_filing_raw_index
        - load_companyfacts: peuple sec_xbrl_fact_raw
        - truncate_before_load: reset complet des tables raw concernées
        - limit_ciks: utile pour un dry-run / test limité
        """
        if truncate_before_load:
            if load_submissions:
                self._con.execute("DELETE FROM sec_filing_raw_index")
            if load_companyfacts:
                self._con.execute("DELETE FROM sec_xbrl_fact_raw")

        submission_files_seen = 0
        submission_rows_written = 0
        companyfacts_files_seen = 0
        xbrl_fact_rows_written = 0

        if load_submissions:
            submission_files_seen, submission_rows_written = self._load_submissions(limit_ciks=limit_ciks)

        if load_companyfacts:
            companyfacts_files_seen, xbrl_fact_rows_written = self._load_companyfacts(limit_ciks=limit_ciks)

        return SecBulkRawLoadMetrics(
            submission_files_seen=submission_files_seen,
            submission_rows_written=submission_rows_written,
            companyfacts_files_seen=companyfacts_files_seen,
            xbrl_fact_rows_written=xbrl_fact_rows_written,
        )

    # =========================================================================
    # Submissions -> sec_filing_raw_index
    # =========================================================================
    def _load_submissions(self, *, limit_ciks: int | None) -> tuple[int, int]:
        files = sorted(self._submissions_root.rglob("CIK*.json"))
        if limit_ciks is not None:
            files = files[: int(limit_ciks)]

        batch: list[tuple[Any, ...]] = []
        files_seen = 0
        rows_written = 0
        ingested_at = datetime.utcnow()

        for path in tqdm(
            files,
            desc="sec_raw:submissions",
            unit="file",
            dynamic_ncols=True,
            leave=True,
        ):
            files_seen += 1

            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except Exception:
                # On ignore les fichiers illisibles pour ne pas stopper un bulk.
                continue

            cik = self._normalize_cik(payload.get("cik"))
            company_name = self._clean_str(payload.get("name"))
            filings_recent = (((payload.get("filings") or {}).get("recent")) or {})

            accession_numbers = self._safe_list(filings_recent.get("accessionNumber"))
            form_types = self._safe_list(filings_recent.get("form"))
            filing_dates = self._safe_list(filings_recent.get("filingDate"))
            acceptance_datetimes = self._safe_list(filings_recent.get("acceptanceDateTime"))
            primary_documents = self._safe_list(filings_recent.get("primaryDocument"))

            max_len = max(
                len(accession_numbers),
                len(form_types),
                len(filing_dates),
                len(acceptance_datetimes),
                len(primary_documents),
                0,
            )

            for idx in range(max_len):
                accession_number = self._value_at(accession_numbers, idx)
                if not accession_number:
                    continue

                primary_document = self._value_at(primary_documents, idx)
                filing_url = self._build_filing_url(
                    cik=cik,
                    accession_number=accession_number,
                    primary_document=primary_document,
                )

                batch.append(
                    (
                        cik,
                        company_name,
                        self._clean_str(self._value_at(form_types, idx)),
                        self._parse_date(self._value_at(filing_dates, idx)),
                        self._parse_sec_acceptance_datetime(self._value_at(acceptance_datetimes, idx)),
                        self._clean_str(accession_number),
                        self._clean_str(primary_document),
                        filing_url,
                        "sec_bulk_submissions",
                        str(path),
                        ingested_at,
                    )
                )

                if len(batch) >= self._insert_batch_size:
                    rows_written += self._flush_submissions_batch(batch)
                    batch = []

        if batch:
            rows_written += self._flush_submissions_batch(batch)

        return files_seen, rows_written

    def _flush_submissions_batch(self, batch: list[tuple[Any, ...]]) -> int:
        self._con.executemany(
            """
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
            """,
            batch,
        )
        return len(batch)

    # =========================================================================
    # Companyfacts -> sec_xbrl_fact_raw
    # =========================================================================
    def _load_companyfacts(self, *, limit_ciks: int | None) -> tuple[int, int]:
        """
        Charge les companyfacts avec une stratégie bulk:
        - parsing JSON
        - écriture CSV temporaire par chunk
        - INSERT ... SELECT depuis read_csv_auto(...)
        """
        files = sorted(self._companyfacts_root.rglob("CIK*.json"))
        if limit_ciks is not None:
            files = files[: int(limit_ciks)]

        files_seen = 0
        rows_written = 0
        chunk_rows_buffer: list[tuple[Any, ...]] = []
        ingested_at = datetime.utcnow()

        for path in tqdm(
            files,
            desc="sec_raw:companyfacts",
            unit="file",
            dynamic_ncols=True,
            leave=True,
        ):
            files_seen += 1

            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except Exception:
                continue

            cik = self._normalize_cik(payload.get("cik"))
            facts_root = payload.get("facts") or {}

            # -----------------------------------------------------------------
            # Structure companyfacts:
            # facts -> taxonomy -> concept -> units -> unit_name -> [facts...]
            # -----------------------------------------------------------------
            if isinstance(facts_root, dict):
                for taxonomy, taxonomy_payload in facts_root.items():
                    if not isinstance(taxonomy_payload, dict):
                        continue

                    for concept, concept_payload in taxonomy_payload.items():
                        if not isinstance(concept_payload, dict):
                            continue

                        units_payload = concept_payload.get("units") or {}
                        if not isinstance(units_payload, dict):
                            continue

                        for unit_name, fact_items in units_payload.items():
                            if not isinstance(fact_items, list):
                                continue

                            for fact_item in fact_items:
                                if not isinstance(fact_item, dict):
                                    continue

                                value_text, value_numeric = self._split_text_and_numeric_value(
                                    fact_item.get("val")
                                )

                                chunk_rows_buffer.append(
                                    (
                                        self._clean_str(fact_item.get("accn")),
                                        cik,
                                        self._clean_str(taxonomy),
                                        self._clean_str(concept),
                                        self._clean_str(unit_name),
                                        self._parse_date(fact_item.get("end")),
                                        self._parse_date(fact_item.get("start")),
                                        self._parse_int(fact_item.get("fy")),
                                        self._clean_str(fact_item.get("fp")),
                                        self._clean_str(fact_item.get("frame")),
                                        value_text,
                                        value_numeric,
                                        "sec_bulk_companyfacts",
                                        str(path),
                                        ingested_at,
                                    )
                                )

                                if len(chunk_rows_buffer) >= self._companyfacts_csv_chunk_rows:
                                    rows_written += self._flush_companyfacts_csv_chunk(chunk_rows_buffer)
                                    chunk_rows_buffer = []

        if chunk_rows_buffer:
            rows_written += self._flush_companyfacts_csv_chunk(chunk_rows_buffer)

        return files_seen, rows_written

    def _flush_companyfacts_csv_chunk(self, rows: list[tuple[Any, ...]]) -> int:
        """
        Écrit un gros chunk en CSV temporaire puis le charge en masse avec DuckDB.

        C'est ici que se trouve le gain de performance principal:
        - moins d'aller-retours Python -> DuckDB
        - DuckDB lit un flux tabulaire massif optimisé
        """
        if not rows:
            return 0

        with tempfile.NamedTemporaryFile(
            mode="w",
            newline="",
            encoding="utf-8",
            suffix=".csv",
            delete=True,
        ) as tmp:
            writer = csv.writer(tmp)

            # Header explicite pour permettre un read_csv_auto bien stable.
            writer.writerow(
                [
                    "accession_number",
                    "cik",
                    "taxonomy",
                    "concept",
                    "unit",
                    "period_end_date",
                    "period_start_date",
                    "fiscal_year",
                    "fiscal_period",
                    "frame",
                    "value_text",
                    "value_numeric",
                    "source_name",
                    "source_file",
                    "ingested_at",
                ]
            )

            for row in rows:
                writer.writerow(row)

            tmp.flush()

            # -----------------------------------------------------------------
            # Chargement bulk via DuckDB.
            #
            # all_varchar=False:
            # - laisse DuckDB typer les colonnes simples
            # - compatible avec notre projection explicite ci-dessous
            #
            # On fait un INSERT ... SELECT pour contrôler exactement l'ordre
            # et éviter toute surprise si la table évolue.
            # -----------------------------------------------------------------
            self._con.execute(
                """
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
                SELECT
                    accession_number,
                    cik,
                    taxonomy,
                    concept,
                    unit,
                    CAST(period_end_date AS DATE),
                    CAST(period_start_date AS DATE),
                    CAST(fiscal_year AS INTEGER),
                    fiscal_period,
                    frame,
                    value_text,
                    CAST(value_numeric AS DOUBLE),
                    source_name,
                    source_file,
                    CAST(ingested_at AS TIMESTAMP)
                FROM read_csv_auto(
                    ?,
                    header = TRUE,
                    all_varchar = TRUE,
                    ignore_errors = FALSE
                )
                """,
                [tmp.name],
            )

        return len(rows)

    # =========================================================================
    # Helpers
    # =========================================================================
    def _safe_list(self, value: Any) -> list[Any]:
        return value if isinstance(value, list) else []

    def _value_at(self, values: list[Any], idx: int) -> Any:
        if 0 <= idx < len(values):
            return values[idx]
        return None

    def _clean_str(self, value: Any) -> str | None:
        if value is None:
            return None
        text = str(value).strip()
        return text if text else None

    def _normalize_cik(self, value: Any) -> str | None:
        cleaned = self._clean_str(value)
        if cleaned is None:
            return None
        return cleaned.zfill(10)

    def _parse_date(self, value: Any):
        cleaned = self._clean_str(value)
        if cleaned is None:
            return None
        try:
            return datetime.strptime(cleaned[:10], "%Y-%m-%d").date()
        except Exception:
            return None

    def _parse_sec_acceptance_datetime(self, value: Any):
        cleaned = self._clean_str(value)
        if cleaned is None:
            return None

        # Formats observés SEC:
        # - 2024-02-01T16:15:42.000Z
        # - 20240201161542
        try:
            if "T" in cleaned:
                normalized = cleaned.replace("Z", "+00:00")
                return datetime.fromisoformat(normalized)
        except Exception:
            pass

        try:
            return datetime.strptime(cleaned, "%Y%m%d%H%M%S")
        except Exception:
            return None

    def _parse_int(self, value: Any) -> int | None:
        if value is None or value == "":
            return None
        try:
            return int(value)
        except Exception:
            return None

    def _split_text_and_numeric_value(self, value: Any) -> tuple[str | None, float | None]:
        """
        Sépare la représentation texte et numérique.

        Règle:
        - si c'est un nombre natif: value_numeric = float(value)
        - sinon value_text = str(value)
        """
        if value is None:
            return None, None

        if isinstance(value, bool):
            return str(value), None

        if isinstance(value, (int, float)):
            try:
                return None, float(value)
            except Exception:
                return str(value), None

        text = self._clean_str(value)
        if text is None:
            return None, None

        try:
            return None, float(text.replace(",", ""))
        except Exception:
            return text, None

    def _build_filing_url(
        self,
        *,
        cik: str | None,
        accession_number: str | None,
        primary_document: str | None,
    ) -> str | None:
        if not cik or not accession_number or not primary_document:
            return None

        accession_no_dash = accession_number.replace("-", "")
        cik_no_padding = str(int(cik)) if cik.isdigit() else cik

        return (
            "https://www.sec.gov/Archives/edgar/data/"
            f"{cik_no_padding}/{accession_no_dash}/{primary_document}"
        )
