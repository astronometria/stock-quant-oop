#!/usr/bin/env python3
from __future__ import annotations

"""
Downloader FINRA raw persistant et incrémental.

Objectif
--------
- Scanner le disque avant tout téléchargement
- Réutiliser les fichiers déjà présents
- Télécharger uniquement les fichiers absents
- Persister le raw sur disque avec une structure stable
- Écrire des manifests CSV pour les loaders

Structure disque cible
----------------------
- data/raw/finra/daily_short_sale_volume/<MARKET>/<FILE>.txt
- data/raw/finra/short_interest/<FILE>
- data/raw/finra/manifests/finra_daily_short_volume_manifest.csv
- data/raw/finra/manifests/finra_short_interest_manifest.csv

Important
---------
- Python mince
- Pas de transformation métier ici
- Beaucoup de commentaires pour maintenir facilement
"""

import argparse
import csv
import hashlib
import re
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen

try:
    from tqdm import tqdm
except Exception:
    def tqdm(iterable, **kwargs):  # type: ignore
        return iterable


USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/123.0 Safari/537.36"
)

DAILY_SHORT_VOLUME_PAGE = (
    "https://www.finra.org/finra-data/browse-catalog/short-sale-volume-data/daily-short-sale-volume-files"
)
SHORT_INTEREST_PAGE = (
    "https://www.finra.org/finra-data/browse-catalog/equity-short-interest/files"
)

# On extrait les liens CDN directs.
CDN_LINK_RE = re.compile(r'https://cdn\.finra\.org[^"\']+', re.IGNORECASE)

# Marchés attendus dans les noms de fichiers Reg SHO FINRA.
KNOWN_MARKETS = ("CNMS", "FNQC", "FNRA", "FNSQ", "FNYX", "FORF")


@dataclass(slots=True)
class ManifestRow:
    dataset: str
    url: str
    filename: str
    local_path: str
    status: str
    size_bytes: int | None
    sha256: str | None
    observed_at: str
    error: str | None = None


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download FINRA raw files incrementally and persistently on disk."
    )
    parser.add_argument(
        "--repo-root",
        default="/home/marty/stock-quant-oop",
        help="Absolute path of the repository root.",
    )
    parser.add_argument(
        "--dataset",
        choices=("daily_short_volume", "short_interest", "all"),
        default="all",
        help="Dataset scope to download.",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=int,
        default=60,
        help="HTTP timeout in seconds.",
    )
    parser.add_argument(
        "--sleep-seconds",
        type=float,
        default=0.20,
        help="Small delay between downloads.",
    )
    return parser.parse_args()


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def http_get_text(url: str, timeout_seconds: int) -> str:
    request = Request(url, headers={"User-Agent": USER_AGENT})
    with urlopen(request, timeout=timeout_seconds) as response:
        return response.read().decode("utf-8", errors="replace")


def http_download_file(url: str, target_path: Path, timeout_seconds: int) -> None:
    """
    Téléchargement atomique:
    - écrit d'abord dans .part
    - renomme ensuite
    """
    request = Request(url, headers={"User-Agent": USER_AGENT})
    tmp_path = target_path.with_suffix(target_path.suffix + ".part")

    with urlopen(request, timeout=timeout_seconds) as response, tmp_path.open("wb") as handle:
        while True:
            chunk = response.read(1024 * 1024)
            if not chunk:
                break
            handle.write(chunk)

    tmp_path.replace(target_path)


def sha256_of_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def extract_cdn_links(html: str) -> list[str]:
    links = CDN_LINK_RE.findall(html)

    # Déduplication stable.
    out: list[str] = []
    seen: set[str] = set()
    for link in links:
        if link in seen:
            continue
        seen.add(link)
        out.append(link)
    return out


def filename_from_url(url: str) -> str:
    return Path(urlparse(url).path).name


def discover_existing_files(root: Path) -> dict[str, Path]:
    """
    Retourne filename -> path.
    Cela permet un skip incrémental simple et robuste.
    """
    out: dict[str, Path] = {}
    if not root.exists():
        return out

    for path in sorted(p for p in root.rglob("*") if p.is_file()):
        out[path.name] = path.resolve()

    return out


def infer_daily_short_market(filename: str) -> str:
    upper = filename.upper()
    for market in KNOWN_MARKETS:
        if upper.startswith(market):
            return market
    return "UNKNOWN"


def build_daily_short_target(raw_root: Path, filename: str) -> Path:
    """
    Exemple:
    CNMSshvol20260320.txt -> data/raw/finra/daily_short_sale_volume/CNMS/CNMSshvol20260320.txt
    """
    market = infer_daily_short_market(filename)
    return raw_root / market / filename


def build_short_interest_target(raw_root: Path, filename: str) -> Path:
    return raw_root / filename


def write_manifest(path: Path, rows: list[ManifestRow]) -> None:
    ensure_dir(path.parent)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "dataset",
                "url",
                "filename",
                "local_path",
                "status",
                "size_bytes",
                "sha256",
                "observed_at",
                "error",
            ]
        )
        for row in rows:
            writer.writerow(
                [
                    row.dataset,
                    row.url,
                    row.filename,
                    row.local_path,
                    row.status,
                    row.size_bytes,
                    row.sha256,
                    row.observed_at,
                    row.error,
                ]
            )


def process_dataset(
    *,
    dataset: str,
    page_url: str,
    target_root: Path,
    manifest_path: Path,
    timeout_seconds: int,
    sleep_seconds: float,
) -> list[ManifestRow]:
    ensure_dir(target_root)

    print(f"[download_finra_raw_persistent] dataset={dataset}", flush=True)
    print(f"[download_finra_raw_persistent] page_url={page_url}", flush=True)
    print(f"[download_finra_raw_persistent] target_root={target_root}", flush=True)

    html = http_get_text(page_url, timeout_seconds)
    links = extract_cdn_links(html)

    # On limite aux formats utiles pour le repo.
    links = [
        link
        for link in links
        if filename_from_url(link).lower().endswith((".txt", ".csv", ".zip"))
    ]

    existing = discover_existing_files(target_root)

    print(
        f"[download_finra_raw_persistent] dataset={dataset} "
        f"discovered_links={len(links)} existing_files={len(existing)}",
        flush=True,
    )

    rows: list[ManifestRow] = []

    for url in tqdm(
        links,
        desc=f"download_{dataset}",
        unit="file",
        dynamic_ncols=True,
        mininterval=0.5,
    ):
        observed_at = now_iso()
        filename = filename_from_url(url)

        if dataset == "daily_short_volume":
            target_path = build_daily_short_target(target_root, filename)
        else:
            target_path = build_short_interest_target(target_root, filename)

        ensure_dir(target_path.parent)

        if filename in existing:
            local_path = existing[filename]
            try:
                size_bytes = local_path.stat().st_size
                sha256 = sha256_of_file(local_path) if size_bytes > 0 else None
                rows.append(
                    ManifestRow(
                        dataset=dataset,
                        url=url,
                        filename=filename,
                        local_path=str(local_path),
                        status="already_present",
                        size_bytes=size_bytes,
                        sha256=sha256,
                        observed_at=observed_at,
                    )
                )
            except Exception as exc:
                rows.append(
                    ManifestRow(
                        dataset=dataset,
                        url=url,
                        filename=filename,
                        local_path=str(local_path),
                        status="already_present_probe_failed",
                        size_bytes=None,
                        sha256=None,
                        observed_at=observed_at,
                        error=str(exc),
                    )
                )
            continue

        try:
            http_download_file(url, target_path, timeout_seconds)
            size_bytes = target_path.stat().st_size
            sha256 = sha256_of_file(target_path) if size_bytes > 0 else None
            rows.append(
                ManifestRow(
                    dataset=dataset,
                    url=url,
                    filename=filename,
                    local_path=str(target_path.resolve()),
                    status="downloaded",
                    size_bytes=size_bytes,
                    sha256=sha256,
                    observed_at=observed_at,
                )
            )
            time.sleep(sleep_seconds)
        except (HTTPError, URLError, OSError, TimeoutError) as exc:
            rows.append(
                ManifestRow(
                    dataset=dataset,
                    url=url,
                    filename=filename,
                    local_path=str(target_path.resolve()),
                    status="error",
                    size_bytes=None,
                    sha256=None,
                    observed_at=observed_at,
                    error=str(exc),
                )
            )

    write_manifest(manifest_path, rows)

    downloaded = sum(1 for row in rows if row.status == "downloaded")
    already_present = sum(1 for row in rows if row.status == "already_present")
    errors = sum(1 for row in rows if row.status == "error")

    print(
        f"[download_finra_raw_persistent] dataset={dataset} downloaded={downloaded} "
        f"already_present={already_present} errors={errors} manifest={manifest_path}",
        flush=True,
    )

    return rows


def main() -> int:
    args = parse_args()
    repo_root = Path(args.repo_root).expanduser().resolve()

    # On garde le root raw existant du repo.
    raw_root = repo_root / "data" / "raw" / "finra"
    manifests_root = raw_root / "manifests"
    daily_short_root = raw_root / "daily_short_sale_volume"
    short_interest_root = raw_root / "short_interest"

    ensure_dir(manifests_root)
    ensure_dir(daily_short_root)
    ensure_dir(short_interest_root)

    all_rows: list[ManifestRow] = []

    if args.dataset in ("daily_short_volume", "all"):
        all_rows.extend(
            process_dataset(
                dataset="daily_short_volume",
                page_url=DAILY_SHORT_VOLUME_PAGE,
                target_root=daily_short_root,
                manifest_path=manifests_root / "finra_daily_short_volume_manifest.csv",
                timeout_seconds=args.timeout_seconds,
                sleep_seconds=args.sleep_seconds,
            )
        )

    if args.dataset in ("short_interest", "all"):
        all_rows.extend(
            process_dataset(
                dataset="short_interest",
                page_url=SHORT_INTEREST_PAGE,
                target_root=short_interest_root,
                manifest_path=manifests_root / "finra_short_interest_manifest.csv",
                timeout_seconds=args.timeout_seconds,
                sleep_seconds=args.sleep_seconds,
            )
        )

    downloaded = sum(1 for row in all_rows if row.status == "downloaded")
    already_present = sum(1 for row in all_rows if row.status == "already_present")
    errors = sum(1 for row in all_rows if row.status == "error")

    print(
        {
            "repo_root": str(repo_root),
            "records": len(all_rows),
            "downloaded": downloaded,
            "already_present": already_present,
            "errors": errors,
        },
        flush=True,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
