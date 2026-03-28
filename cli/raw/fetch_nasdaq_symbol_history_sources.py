#!/usr/bin/env python3
from __future__ import annotations

"""
fetch_nasdaq_symbol_history_sources.py

Objectif
--------
Récupérer un maximum de sources Nasdaq utiles à la reconstruction historique,
en combinant :

1. les fichiers locaux déjà présents
2. les endpoints officiels en ligne encore accessibles gratuitement

Sources visées
--------------
A. Snapshots symbol directory officiels :
   - nasdaqlisted.txt
   - otherlisted.txt

B. Événements officiels adds/deletes :
   - TradingSystemAddsDeletes.txt

C. Archives locales déjà présentes :
   - nasdaqlisted_YYYY-MM-DD.csv
   - otherlisted_YYYY-MM-DD.csv

Philosophie
-----------
- non destructif
- SQL-first downstream, mais ici Python mince car :
  * appels réseau
  * orchestration fichiers
- beaucoup de commentaires
- tqdm pour progression propre
- sortie JSON de synthèse
- pas d'hypothèse agressive sur la disponibilité d'archives historiques gratuites

Structure écrite
----------------
Dans le dossier de sortie :

<data-root>/
  current/
    nasdaqlisted_<today>.txt
    otherlisted_<today>.txt
    TradingSystemAddsDeletes_<today>.txt
  manifests/
    fetch_manifest_<timestamp>.json
  local_inventory/
    local_symbol_snapshots_inventory_<timestamp>.json

Important
---------
Ce script ne modifie pas la DB.
Il prépare la matière première locale.
"""

import argparse
import hashlib
import json
import shutil
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from tqdm import tqdm


# ============================================================================
# Définition des sources officielles actuellement accessibles
# ============================================================================

@dataclass(frozen=True)
class SourceSpec:
    """
    Spécification d'une source distante à télécharger.
    """
    name: str
    url: str
    output_prefix: str
    expected_kind: str


OFFICIAL_SOURCES: list[SourceSpec] = [
    SourceSpec(
        name="nasdaqlisted_current",
        url="https://www.nasdaqtrader.com/dynamic/symdir/nasdaqlisted.txt",
        output_prefix="nasdaqlisted",
        expected_kind="symbol_directory_snapshot",
    ),
    SourceSpec(
        name="otherlisted_current",
        url="https://www.nasdaqtrader.com/dynamic/symdir/otherlisted.txt",
        output_prefix="otherlisted",
        expected_kind="symbol_directory_snapshot",
    ),
    SourceSpec(
        name="trading_system_adds_deletes_current",
        url="https://www.nasdaqtrader.com/dynamic/SymDir/TradingSystemAddsDeletes.txt",
        output_prefix="TradingSystemAddsDeletes",
        expected_kind="listing_event_feed",
    ),
]


# ============================================================================
# Helpers CLI / filesystem
# ============================================================================

def parse_args() -> argparse.Namespace:
    """
    Arguments CLI.
    """
    parser = argparse.ArgumentParser(
        description="Fetch local + official online Nasdaq symbol history sources."
    )
    parser.add_argument(
        "--data-root",
        default="~/stock-quant-oop/data/symbol_sources/nasdaq_history",
        help="Destination root for fetched Nasdaq history sources.",
    )
    parser.add_argument(
        "--local-snapshots-root",
        default="~/stock-quant-oop/data/symbol_sources/nasdaq",
        help="Directory containing local nasdaqlisted_*.csv / otherlisted_*.csv snapshots.",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=int,
        default=60,
        help="HTTP timeout in seconds.",
    )
    return parser.parse_args()


def ensure_dir(path: Path) -> None:
    """
    Crée un dossier si nécessaire.
    """
    path.mkdir(parents=True, exist_ok=True)


def sha256_file(path: Path) -> str:
    """
    Calcule le SHA256 d'un fichier.
    """
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def now_utc_compact() -> str:
    """
    Timestamp compact pour fichiers manifest.
    """
    return datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")


def today_iso() -> str:
    """
    Date du jour en UTC au format ISO.
    """
    return datetime.utcnow().strftime("%Y-%m-%d")


# ============================================================================
# Inventaire local
# ============================================================================

def collect_local_snapshot_inventory(local_root: Path) -> list[dict]:
    """
    Recense les snapshots locaux déjà présents.

    On cible explicitement les fichiers du repo déjà identifiés :
    - nasdaqlisted_YYYY-MM-DD.csv
    - otherlisted_YYYY-MM-DD.csv
    """
    inventory: list[dict] = []

    if not local_root.exists() or not local_root.is_dir():
        return inventory

    patterns = ["nasdaqlisted_*.csv", "otherlisted_*.csv"]
    candidates: list[Path] = []
    for pattern in patterns:
        candidates.extend(local_root.glob(pattern))

    for path in sorted(candidates):
        stat = path.stat()
        inventory.append(
            {
                "path": str(path.resolve()),
                "file_name": path.name,
                "size_bytes": int(stat.st_size),
                "sha256": sha256_file(path),
                "modified_at_utc": datetime.utcfromtimestamp(stat.st_mtime).strftime("%Y-%m-%dT%H:%M:%SZ"),
            }
        )

    return inventory


# ============================================================================
# Téléchargement réseau
# ============================================================================

def download_text_file(url: str, destination: Path, timeout_seconds: int) -> dict:
    """
    Télécharge un fichier texte HTTP(S) vers destination.

    Retourne un dictionnaire de métadonnées de téléchargement.
    """
    request = Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) stock-quant-oop/1.0",
            "Accept": "*/*",
        },
    )

    with urlopen(request, timeout=timeout_seconds) as response:
        payload = response.read()

    destination.write_bytes(payload)

    return {
        "path": str(destination.resolve()),
        "size_bytes": int(destination.stat().st_size),
        "sha256": sha256_file(destination),
    }


# ============================================================================
# Main
# ============================================================================

def main() -> int:
    """
    Pipeline principal.

    Étapes :
    1. préparer dossiers
    2. inventorier le local existant
    3. télécharger les sources officielles online
    4. écrire les manifests
    5. afficher un résumé JSON final
    """
    args = parse_args()

    data_root = Path(args.data_root).expanduser().resolve()
    local_snapshots_root = Path(args.local_snapshots_root).expanduser().resolve()

    current_dir = data_root / "current"
    manifests_dir = data_root / "manifests"
    local_inventory_dir = data_root / "local_inventory"

    ensure_dir(data_root)
    ensure_dir(current_dir)
    ensure_dir(manifests_dir)
    ensure_dir(local_inventory_dir)

    run_ts = now_utc_compact()
    today = today_iso()

    manifest: dict = {
        "run_timestamp_utc": run_ts,
        "data_root": str(data_root),
        "local_snapshots_root": str(local_snapshots_root),
        "local_inventory_count": 0,
        "local_inventory_file": None,
        "downloads": [],
        "download_success_count": 0,
        "download_error_count": 0,
        "notes": [
            "This script prepares local source files only; it does not write to DuckDB.",
            "Free official online coverage includes current symbol directories and current TradingSystemAddsDeletes feed.",
            "Full historical Daily List archives may require separate product/data access.",
        ],
    }

    steps = tqdm(total=5, desc="fetch_nasdaq_symbol_history_sources", unit="step")

    try:
        # --------------------------------------------------------------------
        # 1) Dossiers prêts
        # --------------------------------------------------------------------
        steps.set_description("fetch_nasdaq_symbol_history_sources:prepare_dirs")
        steps.update(1)

        # --------------------------------------------------------------------
        # 2) Inventaire local
        # --------------------------------------------------------------------
        steps.set_description("fetch_nasdaq_symbol_history_sources:inventory_local")
        local_inventory = collect_local_snapshot_inventory(local_snapshots_root)
        local_inventory_file = local_inventory_dir / f"local_symbol_snapshots_inventory_{run_ts}.json"
        local_inventory_file.write_text(
            json.dumps(local_inventory, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        manifest["local_inventory_count"] = int(len(local_inventory))
        manifest["local_inventory_file"] = str(local_inventory_file.resolve())
        steps.update(1)

        # --------------------------------------------------------------------
        # 3) Téléchargements online officiels
        # --------------------------------------------------------------------
        steps.set_description("fetch_nasdaq_symbol_history_sources:download_official_online")
        source_bar = tqdm(
            OFFICIAL_SOURCES,
            desc="nasdaq_online_sources",
            unit="file",
            leave=False,
        )

        for source in source_bar:
            destination = current_dir / f"{source.output_prefix}_{today}.txt"

            result: dict = {
                "name": source.name,
                "url": source.url,
                "expected_kind": source.expected_kind,
                "destination": str(destination.resolve()),
                "status": "unknown",
                "error": None,
                "size_bytes": None,
                "sha256": None,
            }

            try:
                download_meta = download_text_file(
                    url=source.url,
                    destination=destination,
                    timeout_seconds=args.timeout_seconds,
                )
                result["status"] = "ok"
                result["size_bytes"] = download_meta["size_bytes"]
                result["sha256"] = download_meta["sha256"]
                manifest["download_success_count"] += 1
            except HTTPError as exc:
                result["status"] = "http_error"
                result["error"] = f"HTTPError code={exc.code} reason={exc.reason}"
                manifest["download_error_count"] += 1
            except URLError as exc:
                result["status"] = "url_error"
                result["error"] = f"URLError reason={exc.reason}"
                manifest["download_error_count"] += 1
            except Exception as exc:
                result["status"] = "exception"
                result["error"] = repr(exc)
                manifest["download_error_count"] += 1

            manifest["downloads"].append(result)

        steps.update(1)

        # --------------------------------------------------------------------
        # 4) Écriture manifest
        # --------------------------------------------------------------------
        steps.set_description("fetch_nasdaq_symbol_history_sources:write_manifest")
        manifest_file = manifests_dir / f"fetch_manifest_{run_ts}.json"
        manifest_file.write_text(
            json.dumps(manifest, indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
        steps.update(1)

        # --------------------------------------------------------------------
        # 5) Résumé final
        # --------------------------------------------------------------------
        steps.set_description("fetch_nasdaq_symbol_history_sources:metrics")
        print(
            json.dumps(
                {
                    "data_root": str(data_root),
                    "local_snapshots_root": str(local_snapshots_root),
                    "local_inventory_count": int(manifest["local_inventory_count"]),
                    "download_success_count": int(manifest["download_success_count"]),
                    "download_error_count": int(manifest["download_error_count"]),
                    "manifest_file": str(manifest_file.resolve()),
                    "local_inventory_file": str(local_inventory_file.resolve()),
                    "mode": "local_plus_official_online_best_effort_fetch",
                },
                indent=2,
            )
        )
        steps.update(1)

        return 0

    finally:
        steps.close()


if __name__ == "__main__":
    raise SystemExit(main())
