from __future__ import annotations

# =============================================================================
# sec_bulk_disk_loader.py
# -----------------------------------------------------------------------------
# Charge les fichiers bulk SEC déjà extraits sur disque vers les tables raw
# DuckDB:
#   - sec_filing_raw_index
#   - sec_xbrl_fact_raw
#
# Correctifs / objectifs de cette version
# ---------------------------------------
# 1) Éviter le full scan naïf de tous les CIK*.json quand on a déjà un univers
#    boursier dans symbol_reference.
# 2) Garder un fallback "tout scanner" si symbol_reference est vide ou absent.
# 3) Conserver un parsing Python mince.
# 4) Conserver un chargement SQL/bulk pour companyfacts via CSV temporaire.
# 5) Garder tqdm propre et des logs explicites pour audit.
#
# Philosophie
# -----------
# - Python mince pour:
#   - sélectionner les bons fichiers sur disque
#   - parser le JSON SEC
#   - préparer le staging CSV temporaire
# - DuckDB pour:
#   - insérer les données dans les tables raw
#
# Important
# ---------
# - Le mode par défaut est maintenant "universe-first":
#   si symbol_reference contient des CIK valides, on cible seulement les fichiers
#   attendus pour cet univers.
# - Si symbol_reference est vide/inexistant, on retombe sur un comportement
#   de scan global compatible avec l'ancien loader.
# =============================================================================

import csv
import json
import tempfile
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Iterable

from duckdb import DuckDBPyConnection
from tqdm import tqdm


@dataclass(frozen=True)
class SecBulkRawLoadMetrics:
    """
    Métriques simples du chargement bulk raw SEC.

    Les champs historiques sont conservés pour ne pas casser les appelants
    existants qui lisent déjà ces attributs.
    """

    submission_files_seen: int
    submission_rows_written: int
    companyfacts_files_seen: int
    xbrl_fact_rows_written: int
    cik_selection_mode: str
    candidate_ciks_count: int
    submission_files_missing: int
    companyfacts_files_missing: int


class SecBulkDiskLoader:
    """
    Loader bulk SEC disque -> raw DuckDB.

    Entrées disque attendues:
    - data/sec/bulk/submissions/CIK##########.json
    - data/sec/bulk/companyfacts/CIK##########.json

    Notes de design
    ---------------
    - On ne fait PAS ici les couches normalisées / fundamentals.
    - On peuple uniquement les tables raw.
    - Le loader choisit d'abord les CIK à traiter, puis résout les chemins
      de fichiers attendus, au lieu de faire un rglob() massif par défaut.
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

        # Taille du staging CSV temporaire pour companyfacts.
        # Assez gros pour amortir INSERT ... SELECT, sans créer des blobs
        # monstrueux côté disque temporaire.
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

        Paramètres
        ----------
        load_submissions:
            peuple sec_filing_raw_index
        load_companyfacts:
            peuple sec_xbrl_fact_raw
        truncate_before_load:
            reset complet des tables raw concernées
        limit_ciks:
            limite de test / dry-run appliquée APRÈS sélection des CIK cibles

        Stratégie de sélection des CIK
        ------------------------------
        Par défaut:
        1) essaie de lire les CIK distincts depuis symbol_reference
        2) si disponible, ne cible que ces CIK
        3) sinon fallback sur scan global du disque
        """
        if truncate_before_load:
            if load_submissions:
                self._con.execute("DELETE FROM sec_filing_raw_index")
            if load_companyfacts:
                self._con.execute("DELETE FROM sec_xbrl_fact_raw")

        # ---------------------------------------------------------------------
        # Étape 1: déterminer les CIK candidats.
        # ---------------------------------------------------------------------
        universe_ciks = self._load_symbol_reference_ciks()

        if universe_ciks:
            cik_selection_mode = "symbol_reference"
            candidate_ciks = sorted(universe_ciks)
        else:
            cik_selection_mode = "full_scan_fallback"
            candidate_ciks = []

        if limit_ciks is not None:
            if candidate_ciks:
                candidate_ciks = candidate_ciks[: int(limit_ciks)]
            else:
                # En mode fallback full-scan, l'ancienne logique limitait le nombre
                # de fichiers trouvés. On garde cette sémantique dans les sous-méthodes.
                pass

        print(
            "[sec_bulk_disk_loader] selection="
            + json.dumps(
                {
                    "mode": cik_selection_mode,
                    "candidate_ciks_count": len(candidate_ciks),
                    "limit_ciks": limit_ciks,
                    "submissions_root": str(self._submissions_root),
                    "companyfacts_root": str(self._companyfacts_root),
                },
                ensure_ascii=False,
                sort_keys=True,
            ),
            flush=True,
        )

        submission_files_seen = 0
        submission_rows_written = 0
        companyfacts_files_seen = 0
        xbrl_fact_rows_written = 0
        submission_files_missing = 0
        companyfacts_files_missing = 0

        if load_submissions:
            (
                submission_files_seen,
                submission_rows_written,
                submission_files_missing,
            ) = self._load_submissions(
                candidate_ciks=candidate_ciks if candidate_ciks else None,
                limit_ciks=limit_ciks if not candidate_ciks else None,
            )

        if load_companyfacts:
            (
                companyfacts_files_seen,
                xbrl_fact_rows_written,
                companyfacts_files_missing,
            ) = self._load_companyfacts(
                candidate_ciks=candidate_ciks if candidate_ciks else None,
                limit_ciks=limit_ciks if not candidate_ciks else None,
            )

        return SecBulkRawLoadMetrics(
            submission_files_seen=submission_files_seen,
            submission_rows_written=submission_rows_written,
            companyfacts_files_seen=companyfacts_files_seen,
            xbrl_fact_rows_written=xbrl_fact_rows_written,
            cik_selection_mode=cik_selection_mode,
            candidate_ciks_count=len(candidate_ciks),
            submission_files_missing=submission_files_missing,
            companyfacts_files_missing=companyfacts_files_missing,
        )

    # =========================================================================
    # Sélection des CIK / fichiers
    # =========================================================================
    def _load_symbol_reference_ciks(self) -> set[str]:
        """
        Lit les CIK depuis symbol_reference, si la table existe.

        Retour
        ------
        set[str]
            CIK normalisés sur 10 digits.
        """
        if not self._table_exists("symbol_reference"):
            return set()

        column_names = {
            str(row[1]).strip().lower()
            for row in self._con.execute("SELECT * FROM pragma_table_info('symbol_reference')").fetchall()
        }
        if "cik" not in column_names:
            return set()

        rows = self._con.execute(
            """
            SELECT DISTINCT LPAD(TRIM(CAST(cik AS VARCHAR)), 10, '0') AS cik10
            FROM symbol_reference
            WHERE cik IS NOT NULL
              AND TRIM(CAST(cik AS VARCHAR)) <> ''
            """
        ).fetchall()

        normalized: set[str] = set()
        for (cik10,) in rows:
            normalized_cik = self._normalize_cik(cik10)
            if normalized_cik:
                normalized.add(normalized_cik)

        return normalized

    def _resolve_target_files(
        self,
        *,
        root: Path,
        candidate_ciks: list[str] | None,
        limit_ciks: int | None,
    ) -> tuple[list[Path], int]:
        """
        Résout la liste des fichiers à traiter.

        Cas 1: candidate_ciks fourni
            -> résolution directe par chemin attendu
        Cas 2: candidate_ciks absent
            -> fallback ancien mode full-scan via rglob()

        Retour
        ------
        (files, missing_count)
        """
        if candidate_ciks:
            files: list[Path] = []
            missing_count = 0

            for cik in candidate_ciks:
                expected = root / f"CIK{cik}.json"
                if expected.exists():
                    files.append(expected)
                else:
                    missing_count += 1

            return files, missing_count

        files = sorted(root.rglob("CIK*.json"))
        if limit_ciks is not None:
            files = files[: int(limit_ciks)]
        return files, 0

    # =========================================================================
    # Submissions -> sec_filing_raw_index
    # =========================================================================
    def _load_submissions(
        self,
        *,
        candidate_ciks: list[str] | None,
        limit_ciks: int | None,
    ) -> tuple[int, int, int]:
        files, missing_count = self._resolve_target_files(
            root=self._submissions_root,
            candidate_ciks=candidate_ciks,
            limit_ciks=limit_ciks,
        )

        print(
            "[sec_bulk_disk_loader] submissions_probe="
            + json.dumps(
                {
                    "root": str(self._submissions_root),
                    "files_selected": len(files),
                    "files_missing": int(missing_count),
                },
                ensure_ascii=False,
                sort_keys=True,
            ),
            flush=True,
        )

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
                # On ignore les fichiers illisibles pour ne pas stopper un bulk long.
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

            for i in range(max_len):
                accession_number = self._clean_str(self._list_get(accession_numbers, i))
                form_type = self._clean_str(self._list_get(form_types, i))
                filing_date = self._parse_date_like(self._list_get(filing_dates, i))
                accepted_at = self._parse_datetime_like(self._list_get(acceptance_datetimes, i))
                primary_document = self._clean_str(self._list_get(primary_documents, i))
                filing_url = self._build_filing_url(
                    cik=cik,
                    accession_number=accession_number,
                    primary_document=primary_document,
                )

                # On ignore les lignes structurellement vides.
                if not accession_number and not form_type and filing_date is None:
                    continue

                batch.append(
                    (
                        cik,
                        company_name,
                        form_type,
                        filing_date,
                        accepted_at,
                        accession_number,
                        primary_document,
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

        return files_seen, rows_written, missing_count

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
    def _load_companyfacts(
        self,
        *,
        candidate_ciks: list[str] | None,
        limit_ciks: int | None,
    ) -> tuple[int, int, int]:
        """
        Charge les companyfacts avec une stratégie bulk:
        - parsing JSON
        - écriture CSV temporaire par chunk
        - INSERT ... SELECT depuis read_csv_auto(...)
        """
        files, missing_count = self._resolve_target_files(
            root=self._companyfacts_root,
            candidate_ciks=candidate_ciks,
            limit_ciks=limit_ciks,
        )

        print(
            "[sec_bulk_disk_loader] companyfacts_probe="
            + json.dumps(
                {
                    "root": str(self._companyfacts_root),
                    "files_selected": len(files),
                    "files_missing": int(missing_count),
                },
                ensure_ascii=False,
                sort_keys=True,
            ),
            flush=True,
        )

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

            for row in self._iter_companyfacts_rows(
                payload=payload,
                source_file=path,
                ingested_at=ingested_at,
            ):
                chunk_rows_buffer.append(row)

                if len(chunk_rows_buffer) >= self._companyfacts_csv_chunk_rows:
                    rows_written += self._flush_companyfacts_chunk(chunk_rows_buffer)
                    chunk_rows_buffer = []

        if chunk_rows_buffer:
            rows_written += self._flush_companyfacts_chunk(chunk_rows_buffer)

        return files_seen, rows_written, missing_count

    def _iter_companyfacts_rows(
        self,
        *,
        payload: dict[str, Any],
        source_file: Path,
        ingested_at: datetime,
    ) -> Iterable[tuple[Any, ...]]:
        """
        Parse un payload SEC companyfacts JSON et émet des tuples prêts à charger
        vers sec_xbrl_fact_raw.
        """
        cik = self._normalize_cik(payload.get("cik"))
        facts_root = payload.get("facts") or {}

        # Structure attendue:
        # facts -> taxonomy -> concept -> units -> unit_name -> [facts...]
        if not isinstance(facts_root, dict):
            return []

        for taxonomy, taxonomy_payload in facts_root.items():
            if not isinstance(taxonomy_payload, dict):
                continue

            for concept, concept_payload in taxonomy_payload.items():
                if not isinstance(concept_payload, dict):
                    continue

                units_payload = concept_payload.get("units") or {}
                if not isinstance(units_payload, dict):
                    continue

                for unit_name, fact_list in units_payload.items():
                    if not isinstance(fact_list, list):
                        continue

                    for fact in fact_list:
                        if not isinstance(fact, dict):
                            continue

                        accession_number = self._clean_str(fact.get("accn"))
                        period_end_date = self._parse_date_like(fact.get("end"))
                        period_start_date = self._parse_date_like(fact.get("start"))
                        fiscal_year = self._parse_int(fact.get("fy"))
                        fiscal_period = self._clean_str(fact.get("fp"))
                        frame = self._clean_str(fact.get("frame"))
                        value_numeric, value_text = self._split_sec_value(fact.get("val"))

                        yield (
                            accession_number,
                            cik,
                            self._clean_str(taxonomy),
                            self._clean_str(concept),
                            self._clean_str(unit_name),
                            period_end_date.isoformat() if period_end_date else None,
                            period_start_date.isoformat() if period_start_date else None,
                            fiscal_year,
                            fiscal_period,
                            frame,
                            value_text,
                            value_numeric,
                            "sec_bulk_companyfacts",
                            str(source_file),
                            ingested_at.isoformat(sep=" "),
                        )

    def _flush_companyfacts_chunk(self, rows: list[tuple[Any, ...]]) -> int:
        """
        Écrit un chunk CSV temporaire puis charge en bulk dans sec_xbrl_fact_raw.
        """
        if not rows:
            return 0

        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            newline="",
            suffix=".csv",
            delete=True,
        ) as tmp:
            writer = csv.writer(tmp)
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
    # Helpers divers
    # =========================================================================
    def _table_exists(self, table_name: str) -> bool:
        row = self._con.execute(
            """
            SELECT COUNT(*)
            FROM information_schema.tables
            WHERE table_schema = 'main'
              AND table_name = ?
            """,
            [table_name],
        ).fetchone()
        return bool(row and row[0])

    @staticmethod
    def _normalize_cik(value: Any) -> str:
        """
        Normalise un CIK sur 10 digits.

        Retourne '' si la valeur n'est pas exploitable.
        """
        if value is None:
            return ""

        text = str(value).strip()
        if not text:
            return ""

        # Certains payloads peuvent déjà contenir des zéros padding.
        # On garde uniquement les digits.
        digits = "".join(ch for ch in text if ch.isdigit())
        if not digits:
            return ""

        return digits.zfill(10)

    @staticmethod
    def _clean_str(value: Any) -> str | None:
        if value is None:
            return None
        text = str(value).strip()
        return text if text else None

    @staticmethod
    def _safe_list(value: Any) -> list[Any]:
        return value if isinstance(value, list) else []

    @staticmethod
    def _list_get(items: list[Any], index: int) -> Any:
        if 0 <= index < len(items):
            return items[index]
        return None

    @staticmethod
    def _parse_date_like(value: Any) -> date | None:
        """
        Parse une date SEC classique:
        - YYYY-MM-DD
        - ou datetime ISO, dont on extrait la partie date
        """
        if value is None:
            return None

        text = str(value).strip()
        if not text:
            return None

        # Le cas majoritaire SEC.
        try:
            return datetime.strptime(text[:10], "%Y-%m-%d").date()
        except Exception:
            return None

    @staticmethod
    def _parse_datetime_like(value: Any) -> datetime | None:
        """
        Parse une datetime SEC simple.
        Exemples fréquents:
        - 2026-03-19T17:23:45.000Z
        - 20260319172345
        """
        if value is None:
            return None

        text = str(value).strip()
        if not text:
            return None

        # Format SEC fréquent: YYYYMMDDHHMMSS
        if len(text) == 14 and text.isdigit():
            try:
                return datetime.strptime(text, "%Y%m%d%H%M%S")
            except Exception:
                pass

        # Format ISO générique.
        normalized = text.replace("Z", "+00:00")
        try:
            return datetime.fromisoformat(normalized)
        except Exception:
            return None

    @staticmethod
    def _parse_int(value: Any) -> int | None:
        if value is None:
            return None
        try:
            return int(value)
        except Exception:
            return None

    @staticmethod
    def _split_sec_value(value: Any) -> tuple[float | None, str | None]:
        """
        Sépare une valeur SEC en:
        - value_numeric si castable en float
        - value_text sinon

        On conserve aussi le texte pour ne pas perdre d'information.
        """
        if value is None:
            return None, None

        # Booléens -> texte, pour éviter 0/1 non voulus.
        if isinstance(value, bool):
            return None, str(value)

        if isinstance(value, (int, float)):
            return float(value), str(value)

        text = str(value).strip()
        if not text:
            return None, None

        try:
            return float(text), text
        except Exception:
            return None, text

    @staticmethod
    def _build_filing_url(
        *,
        cik: str | None,
        accession_number: str | None,
        primary_document: str | None,
    ) -> str | None:
        """
        Construit une URL Archives SEC quand les composants sont disponibles.
        """
        if not cik or not accession_number:
            return None

        accession_no_dashes = accession_number.replace("-", "").strip()
        if not accession_no_dashes:
            return None

        cik_numeric = str(int(cik)) if str(cik).isdigit() else cik.lstrip("0")
        if not cik_numeric:
            return None

        if primary_document:
            return (
                f"https://www.sec.gov/Archives/edgar/data/"
                f"{cik_numeric}/{accession_no_dashes}/{primary_document}"
            )

        return (
            f"https://www.sec.gov/Archives/edgar/data/"
            f"{cik_numeric}/{accession_no_dashes}/"
        )
