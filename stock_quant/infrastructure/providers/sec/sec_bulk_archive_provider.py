from __future__ import annotations

# =============================================================================
# sec_bulk_archive_provider.py
# -----------------------------------------------------------------------------
# Télécharge et extrait les archives bulk officielles SEC.
#
# Cette version ajoute:
# - une vraie barre de progression tqdm par archive téléchargée
# - un affichage propre même pour de gros fichiers
# - des manifests d'erreur plus utiles
#
# Objectif:
# - garder cette couche mince:
#     * HTTP
#     * écriture disque
#     * extraction ZIP
#     * manifeste léger
# - ne PAS mélanger ici le parsing métier ou le load DB
# =============================================================================

import hashlib
import json
import shutil
import zipfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests
from tqdm import tqdm


@dataclass(frozen=True)
class SecBulkArchiveResult:
    archive_name: str
    archive_path: Path
    extract_dir: Path
    downloaded: bool
    extracted: bool
    file_count: int
    byte_size: int
    sha1: str | None


class SecBulkArchiveProvider:
    """
    Provider de téléchargement des archives bulk SEC.

    Notes:
    - submissions.zip et companyfacts.zip sont les deux entrées bulk principales
    - le téléchargement est idempotent par défaut
    - refresh_existing=True force un re-download complet
    """

    BULK_SUBMISSIONS_URL = "https://www.sec.gov/Archives/edgar/daily-index/bulkdata/submissions.zip"
    BULK_COMPANYFACTS_URL = "https://www.sec.gov/Archives/edgar/daily-index/xbrl/companyfacts.zip"

    def __init__(
        self,
        *,
        data_root: str | Path,
        user_agent: str,
        timeout_seconds: float = 120.0,
        chunk_size_bytes: int = 1024 * 1024,
        session: requests.Session | None = None,
    ) -> None:
        self._data_root = Path(data_root).expanduser()
        self._sec_root = self._data_root / "sec"
        self._bulk_root = self._sec_root / "bulk"
        self._manifest_root = self._sec_root / "manifests"

        self._user_agent = str(user_agent).strip()
        self._timeout_seconds = float(timeout_seconds)
        self._chunk_size_bytes = int(chunk_size_bytes)
        self._session = session or requests.Session()

        self._bulk_root.mkdir(parents=True, exist_ok=True)
        self._manifest_root.mkdir(parents=True, exist_ok=True)

    def download_and_extract(
        self,
        *,
        include_submissions: bool = True,
        include_companyfacts: bool = True,
        refresh_existing: bool = False,
    ) -> dict[str, Any]:
        """
        Télécharge puis extrait les archives bulk demandées.
        """
        results: list[SecBulkArchiveResult] = []

        if include_submissions:
            results.append(
                self._download_one_archive(
                    archive_name="submissions",
                    url=self.BULK_SUBMISSIONS_URL,
                    refresh_existing=refresh_existing,
                )
            )

        if include_companyfacts:
            results.append(
                self._download_one_archive(
                    archive_name="companyfacts",
                    url=self.BULK_COMPANYFACTS_URL,
                    refresh_existing=refresh_existing,
                )
            )

        return {
            "archives_requested": len(results),
            "archives_downloaded": sum(1 for r in results if r.downloaded),
            "archives_extracted": sum(1 for r in results if r.extracted),
            "total_extracted_files": sum(r.file_count for r in results),
            "results": [
                {
                    "archive_name": r.archive_name,
                    "archive_path": str(r.archive_path),
                    "extract_dir": str(r.extract_dir),
                    "downloaded": r.downloaded,
                    "extracted": r.extracted,
                    "file_count": r.file_count,
                    "byte_size": r.byte_size,
                    "sha1": r.sha1,
                }
                for r in results
            ],
        }

    def _download_one_archive(
        self,
        *,
        archive_name: str,
        url: str,
        refresh_existing: bool,
    ) -> SecBulkArchiveResult:
        """
        Télécharge et extrait une archive bulk unique.

        Affichage:
        - tqdm montre la progression en bytes si Content-Length est connu
        - sinon tqdm reste sans total, mais affiche quand même l'avancement
        """
        archive_path = self._bulk_root / f"{archive_name}.zip"
        extract_dir = self._bulk_root / archive_name

        downloaded = False
        extracted = False
        byte_size = 0
        sha1_value: str | None = None

        if refresh_existing and archive_path.exists():
            archive_path.unlink()

        if refresh_existing and extract_dir.exists():
            shutil.rmtree(extract_dir)

        if not archive_path.exists():
            try:
                response = self._session.get(
                    url,
                    headers=self._build_headers(),
                    timeout=self._timeout_seconds,
                    stream=True,
                    allow_redirects=True,
                )

                if response.status_code != 200:
                    self._write_manifest(
                        archive_name=archive_name,
                        archive_path=archive_path,
                        extract_dir=extract_dir,
                        metadata={
                            "url": url,
                            "status": "HTTP_ERROR",
                            "status_code": response.status_code,
                            "reason": response.reason,
                            "response_headers": dict(response.headers),
                            "written_at": self._utc_now_iso(),
                        },
                    )
                    response.raise_for_status()

                total_bytes = self._parse_content_length(response.headers.get("Content-Length"))
                sha1 = hashlib.sha1()
                bytes_written = 0

                # -----------------------------------------------------------------
                # tqdm:
                # - unit="B" pour des bytes
                # - unit_scale=True pour KiB/MiB/GiB
                # - leave=True pour conserver le résultat final visible
                # - dynamic_ncols=True pour s'adapter à la largeur du terminal
                # -----------------------------------------------------------------
                progress = tqdm(
                    total=total_bytes,
                    desc=f"sec_bulk:{archive_name}",
                    unit="B",
                    unit_scale=True,
                    unit_divisor=1024,
                    dynamic_ncols=True,
                    leave=True,
                )

                try:
                    with archive_path.open("wb") as handle:
                        for chunk in response.iter_content(chunk_size=self._chunk_size_bytes):
                            if not chunk:
                                continue
                            handle.write(chunk)
                            sha1.update(chunk)
                            chunk_len = len(chunk)
                            bytes_written += chunk_len
                            progress.update(chunk_len)
                finally:
                    progress.close()

                downloaded = True
                byte_size = bytes_written
                sha1_value = sha1.hexdigest()

            except Exception as exc:
                self._write_manifest(
                    archive_name=archive_name,
                    archive_path=archive_path,
                    extract_dir=extract_dir,
                    metadata={
                        "url": url,
                        "status": "EXCEPTION",
                        "error": str(exc),
                        "written_at": self._utc_now_iso(),
                    },
                )
                raise
        else:
            byte_size = archive_path.stat().st_size
            sha1_value = self._sha1_file(archive_path)
            print(f"[sec_bulk_archive_provider] skip existing archive: {archive_path}")

        if not extract_dir.exists():
            print(f"[sec_bulk_archive_provider] extracting: {archive_path} -> {extract_dir}")
            extract_dir.mkdir(parents=True, exist_ok=True)
            with zipfile.ZipFile(archive_path, "r") as zf:
                zf.extractall(extract_dir)
            extracted = True
        else:
            print(f"[sec_bulk_archive_provider] skip existing extract dir: {extract_dir}")

        file_count = sum(1 for p in extract_dir.rglob("*") if p.is_file())

        self._write_manifest(
            archive_name=archive_name,
            archive_path=archive_path,
            extract_dir=extract_dir,
            metadata={
                "url": url,
                "downloaded": downloaded,
                "extracted": extracted,
                "file_count": file_count,
                "byte_size": byte_size,
                "sha1": sha1_value,
                "written_at": self._utc_now_iso(),
            },
        )

        return SecBulkArchiveResult(
            archive_name=archive_name,
            archive_path=archive_path,
            extract_dir=extract_dir,
            downloaded=downloaded,
            extracted=extracted,
            file_count=file_count,
            byte_size=byte_size,
            sha1=sha1_value,
        )

    def _build_headers(self) -> dict[str, str]:
        """
        Construit les headers HTTP.

        Notes:
        - pas de header Host forcé
        - From ajouté si un email est détecté dans le User-Agent
        """
        email = self._extract_email_from_user_agent()
        headers = {
            "User-Agent": self._user_agent,
            "Accept-Encoding": "gzip, deflate",
            "Accept": "*/*",
            "Referer": "https://www.sec.gov/",
        }
        if email:
            headers["From"] = email
        return headers

    def _extract_email_from_user_agent(self) -> str | None:
        parts = self._user_agent.split()
        for part in parts:
            if "@" in part and "." in part:
                return part.strip(" <>(),;")
        return None

    def _parse_content_length(self, value: str | None) -> int | None:
        """
        Parse Content-Length si disponible.

        Retourne None si absent ou invalide, ce qui laisse tqdm fonctionner
        sans total connu.
        """
        if value is None:
            return None
        try:
            parsed = int(value)
            if parsed >= 0:
                return parsed
        except Exception:
            return None
        return None

    def _write_manifest(
        self,
        *,
        archive_name: str,
        archive_path: Path,
        extract_dir: Path,
        metadata: dict[str, Any],
    ) -> None:
        manifest_path = self._manifest_root / f"bulk_{archive_name}.json"
        payload = {
            "archive_name": archive_name,
            "archive_path": str(archive_path),
            "extract_dir": str(extract_dir),
            "metadata": metadata,
        }
        manifest_path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    def _sha1_file(self, path: Path) -> str:
        sha1 = hashlib.sha1()
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                if not chunk:
                    break
                sha1.update(chunk)
        return sha1.hexdigest()

    def _utc_now_iso(self) -> str:
        return datetime.now(timezone.utc).isoformat()
