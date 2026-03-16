from __future__ import annotations

# =============================================================================
# sec_bulk_history_provider.py
# -----------------------------------------------------------------------------
# Provider de téléchargement historique SEC vers le disque.
#
# Objectif:
# - télécharger les JSON officiels "submissions" et "companyfacts"
# - stocker les fichiers bruts durablement sous data/sec/
# - permettre un bootstrap historique reprenable, idempotent et audit-able
#
# Philosophie:
# - on ne parse pas lourdement ici
# - ce provider reste mince: réseau + stockage + manifeste léger
# - le parsing / staging DB viendra dans le pipeline suivant
#
# Sources officielles ciblées:
# - https://data.sec.gov/submissions/CIK##########.json
# - https://data.sec.gov/api/xbrl/companyfacts/CIK##########.json
#
# Notes:
# - la SEC demande un User-Agent explicite avec contact
# - on applique un rate-limit conservateur
# - on garde les téléchargements existants sauf si refresh demandé
# =============================================================================

import hashlib
import json
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests
from tqdm import tqdm


@dataclass(frozen=True)
class SecDownloadTarget:
    """
    Représente une cible de téléchargement SEC pour un CIK donné.
    """
    cik: str
    company_id: str | None = None
    symbol: str | None = None


class SecBulkHistoryProvider:
    """
    Provider de téléchargement brut SEC.

    Ce provider:
    - télécharge les JSON bruts
    - les écrit sur disque
    - maintient un manifeste léger par fichier

    Il ne fait PAS:
    - le load DB
    - la normalisation métiers
    - la construction des snapshots fundamentals
    """

    SUBMISSIONS_URL_TEMPLATE = "https://data.sec.gov/submissions/CIK{cik}.json"
    COMPANYFACTS_URL_TEMPLATE = "https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"

    def __init__(
        self,
        *,
        data_root: str | Path,
        user_agent: str,
        timeout_seconds: float = 60.0,
        sleep_seconds: float = 0.20,
        session: requests.Session | None = None,
    ) -> None:
        """
        Paramètres:
        - data_root: racine data du projet
        - user_agent: User-Agent explicite demandé par la SEC
        - timeout_seconds: timeout HTTP
        - sleep_seconds: pause conservatrice entre requêtes
        - session: injection optionnelle pour tests
        """
        self._data_root = Path(data_root).expanduser()
        self._sec_root = self._data_root / "sec"
        self._submissions_root = self._sec_root / "submissions"
        self._companyfacts_root = self._sec_root / "companyfacts"
        self._manifest_root = self._sec_root / "manifests"

        self._user_agent = str(user_agent).strip()
        self._timeout_seconds = float(timeout_seconds)
        self._sleep_seconds = float(sleep_seconds)
        self._session = session or requests.Session()

        # ---------------------------------------------------------------------
        # On prépare l'arborescence disque dès l'initialisation pour éviter
        # les erreurs tardives pendant le téléchargement.
        # ---------------------------------------------------------------------
        self._submissions_root.mkdir(parents=True, exist_ok=True)
        self._companyfacts_root.mkdir(parents=True, exist_ok=True)
        self._manifest_root.mkdir(parents=True, exist_ok=True)

    # =========================================================================
    # API publique
    # =========================================================================
    def download_history_for_targets(
        self,
        targets: list[SecDownloadTarget],
        *,
        include_submissions: bool = True,
        include_companyfacts: bool = True,
        refresh_existing: bool = False,
        show_progress: bool = True,
    ) -> dict[str, Any]:
        """
        Télécharge l'historique brut SEC pour une liste de CIKs.

        Retourne des métriques suffisamment détaillées pour:
        - logs
        - monitoring
        - audit d'ingestion
        """
        metrics: dict[str, Any] = {
            "targets_requested": len(targets),
            "submissions_downloaded": 0,
            "submissions_skipped_existing": 0,
            "submissions_failed": 0,
            "companyfacts_downloaded": 0,
            "companyfacts_skipped_existing": 0,
            "companyfacts_failed": 0,
            "files_written": 0,
        }

        iterator = tqdm(
            targets,
            desc="sec_download_history",
            unit="cik",
            leave=False,
            disable=not show_progress,
        )

        for target in iterator:
            cik = self._normalize_cik(target.cik)

            if include_submissions:
                submission_result = self._download_one_json(
                    cik=cik,
                    target_kind="submissions",
                    url=self.SUBMISSIONS_URL_TEMPLATE.format(cik=cik),
                    output_path=self._submissions_root / f"CIK{cik}.json",
                    refresh_existing=refresh_existing,
                    metadata={
                        "cik": cik,
                        "company_id": target.company_id,
                        "symbol": target.symbol,
                        "target_kind": "submissions",
                    },
                )
                metrics[f"submissions_{submission_result}"] += 1
                if submission_result == "downloaded":
                    metrics["files_written"] += 1

            if include_companyfacts:
                companyfacts_result = self._download_one_json(
                    cik=cik,
                    target_kind="companyfacts",
                    url=self.COMPANYFACTS_URL_TEMPLATE.format(cik=cik),
                    output_path=self._companyfacts_root / f"CIK{cik}.json",
                    refresh_existing=refresh_existing,
                    metadata={
                        "cik": cik,
                        "company_id": target.company_id,
                        "symbol": target.symbol,
                        "target_kind": "companyfacts",
                    },
                )
                metrics[f"companyfacts_{companyfacts_result}"] += 1
                if companyfacts_result == "downloaded":
                    metrics["files_written"] += 1

        return metrics

    # =========================================================================
    # Téléchargement d'un fichier JSON
    # =========================================================================
    def _download_one_json(
        self,
        *,
        cik: str,
        target_kind: str,
        url: str,
        output_path: Path,
        refresh_existing: bool,
        metadata: dict[str, Any],
    ) -> str:
        """
        Télécharge un JSON unique.

        Retour possible:
        - "downloaded"
        - "skipped_existing"
        - "failed"
        """
        output_path.parent.mkdir(parents=True, exist_ok=True)

        if output_path.exists() and not refresh_existing:
            # -------------------------------------------------------------
            # Idempotence simple:
            # - si le fichier existe déjà, on ne le retélécharge pas
            # - le refresh explicite permet de forcer une remise à jour
            # -------------------------------------------------------------
            return "skipped_existing"

        try:
            response = self._session.get(
                url,
                headers=self._build_headers(),
                timeout=self._timeout_seconds,
            )

            # -------------------------------------------------------------
            # La SEC peut répondre 404 pour certains CIKs sans companyfacts,
            # ou pour certains cas rares de couverture incomplète.
            # On considère cela comme un échec métier léger, pas une crash.
            # -------------------------------------------------------------
            if response.status_code != 200:
                self._write_manifest(
                    cik=cik,
                    target_kind=target_kind,
                    output_path=output_path,
                    status="HTTP_ERROR",
                    metadata={
                        **metadata,
                        "url": url,
                        "status_code": response.status_code,
                        "reason": response.reason,
                        "downloaded_at": self._utc_now_iso(),
                    },
                )
                self._sleep()
                return "failed"

            # -------------------------------------------------------------
            # On parse pour valider que le contenu reçu est bien du JSON
            # correct avant de l'écrire sur disque.
            # -------------------------------------------------------------
            payload = response.json()

            raw_bytes = json.dumps(
                payload,
                ensure_ascii=False,
                sort_keys=False,
                indent=2,
            ).encode("utf-8")

            output_path.write_bytes(raw_bytes)

            self._write_manifest(
                cik=cik,
                target_kind=target_kind,
                output_path=output_path,
                status="SUCCESS",
                metadata={
                    **metadata,
                    "url": url,
                    "status_code": response.status_code,
                    "byte_size": len(raw_bytes),
                    "sha1": hashlib.sha1(raw_bytes).hexdigest(),
                    "downloaded_at": self._utc_now_iso(),
                },
            )

            self._sleep()
            return "downloaded"

        except Exception as exc:
            self._write_manifest(
                cik=cik,
                target_kind=target_kind,
                output_path=output_path,
                status="EXCEPTION",
                metadata={
                    **metadata,
                    "url": url,
                    "error": str(exc),
                    "downloaded_at": self._utc_now_iso(),
                },
            )
            self._sleep()
            return "failed"

    # =========================================================================
    # Helpers
    # =========================================================================
    def _build_headers(self) -> dict[str, str]:
        """
        Construit les headers HTTP pour la SEC.

        La SEC demande un User-Agent explicite avec un contact.
        """
        return {
            "User-Agent": self._user_agent,
            "Accept-Encoding": "gzip, deflate",
            "Accept": "application/json, text/plain, */*",
            "Host": "data.sec.gov",
        }

    def _write_manifest(
        self,
        *,
        cik: str,
        target_kind: str,
        output_path: Path,
        status: str,
        metadata: dict[str, Any],
    ) -> None:
        """
        Écrit un manifeste léger à côté des fichiers.

        Avantage:
        - audit simple
        - debugging rapide
        - historique des tentatives sans dépendre d'une DB déjà chargée
        """
        manifest_path = self._manifest_root / f"{target_kind}_CIK{cik}.json"
        manifest_payload = {
            "cik": cik,
            "target_kind": target_kind,
            "status": status,
            "output_path": str(output_path),
            "metadata": metadata,
            "written_at": self._utc_now_iso(),
        }
        manifest_path.write_text(
            json.dumps(manifest_payload, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    def _sleep(self) -> None:
        """
        Rate limiting conservateur.

        On préfère rester volontairement un peu lent mais robuste et poli
        vis-à-vis de la SEC.
        """
        if self._sleep_seconds > 0:
            time.sleep(self._sleep_seconds)

    def _normalize_cik(self, cik: str | int) -> str:
        """
        Normalise un CIK au format SEC attendu: 10 chiffres.
        """
        return str(cik).strip().zfill(10)

    def _utc_now_iso(self) -> str:
        """
        Timestamp ISO UTC pour les manifests.
        """
        return datetime.now(timezone.utc).isoformat()
