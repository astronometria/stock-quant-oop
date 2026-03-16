#!/usr/bin/env python3
from __future__ import annotations

# =============================================================================
# download_finra_short_interest.py
#
# Objectif
# --------
# Télécharger les fichiers FINRA "Short Interest" depuis le CDN officiel :
#
#   https://cdn.finra.org/equity/otcmarket/biweekly/
#
# Ces fichiers sont publiés deux fois par mois :
#
#   - le 15
#   - le dernier jour du mois
#
# Format :
#   shrtYYYYMMDD.csv
#
# Exemple :
#   shrt20240115.csv
#
# Ce script :
#
#   1) génère toutes les dates candidates
#   2) probe les fichiers existants
#   3) télécharge uniquement ceux qui existent
#   4) évite de re-télécharger les fichiers déjà présents
#
# IMPORTANT
# ---------
#
# Tous les fichiers sont stockés sous :
#
#   data/raw/finra/short_interest
#
# Ce dossier est la source canonique disque pour :
#
#   load_finra_short_interest_source_raw.py
#
# Python reste volontairement mince :
#
#   - génération des dates
#   - download HTTP
#   - écriture disque
#
# =============================================================================

import argparse
import calendar
import json
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import requests

try:
    from tqdm import tqdm
except Exception:
    def tqdm(iterable, **kwargs):
        return iterable


# =============================================================================
# CONFIG
# =============================================================================

BASE_URL = "https://cdn.finra.org/equity/otcmarket/biweekly"

DEFAULT_START_DATE = "2017-12-29"
DEFAULT_END_DATE = "2026-02-13"


# =============================================================================
# DATA STRUCTURES
# =============================================================================

@dataclass(frozen=True, slots=True)
class DownloadTarget:
    file_date: date

    @property
    def filename(self) -> str:
        return f"shrt{self.file_date.strftime('%Y%m%d')}.csv"

    @property
    def url(self) -> str:
        return f"{BASE_URL}/{self.filename}"


# =============================================================================
# CLI
# =============================================================================

def parse_args() -> argparse.Namespace:

    parser = argparse.ArgumentParser(
        description="Download FINRA equity short interest CSV files."
    )

    parser.add_argument(
        "--output-dir",
        default="~/stock-quant-oop/data/raw/finra/short_interest",
        help="Directory where FINRA files are stored."
    )

    parser.add_argument(
        "--start-date",
        default=DEFAULT_START_DATE,
        help="Inclusive start date YYYY-MM-DD."
    )

    parser.add_argument(
        "--end-date",
        default=DEFAULT_END_DATE,
        help="Inclusive end date YYYY-MM-DD."
    )

    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing files."
    )

    parser.add_argument(
        "--timeout",
        type=int,
        default=60
    )

    parser.add_argument(
        "--verbose",
        action="store_true"
    )

    return parser.parse_args()


# =============================================================================
# DATE LOGIC
# =============================================================================

def parse_iso_date(value: str) -> date:
    return date.fromisoformat(value)


def month_end(day: date) -> date:
    return date(day.year, day.month, calendar.monthrange(day.year, day.month)[1])


def build_candidate_dates(start_date: date, end_date: date) -> list[date]:

    dates: list[date] = []

    cursor = date(start_date.year, start_date.month, 1)

    while cursor <= end_date:

        d15 = date(cursor.year, cursor.month, 15)
        dend = month_end(cursor)

        if start_date <= d15 <= end_date:
            dates.append(d15)

        if start_date <= dend <= end_date and dend != d15:
            dates.append(dend)

        if cursor.month == 12:
            cursor = date(cursor.year + 1, 1, 1)
        else:
            cursor = date(cursor.year, cursor.month + 1, 1)

    return sorted(set(dates))


# =============================================================================
# HTTP UTILITIES
# =============================================================================

def build_session() -> requests.Session:

    session = requests.Session()

    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (X11; Linux x86_64) "
                "AppleWebKit/537.36 Chrome/122 Safari/537.36"
            )
        }
    )

    return session


def probe_exists(session: requests.Session, url: str, timeout: int) -> bool:

    r = session.get(url, timeout=timeout, stream=True)

    try:
        if r.status_code == 200:
            return True

        if r.status_code == 404:
            return False

        r.raise_for_status()
        return True

    finally:
        r.close()


# =============================================================================
# DOWNLOAD
# =============================================================================

def download_file(session, url, out_path: Path, timeout: int) -> int:

    r = session.get(url, timeout=timeout, stream=True)
    r.raise_for_status()

    try:

        written = 0

        with out_path.open("wb") as f:

            for chunk in r.iter_content(1024 * 256):

                if not chunk:
                    continue

                f.write(chunk)
                written += len(chunk)

        return written

    finally:
        r.close()


# =============================================================================
# MAIN
# =============================================================================

def main() -> int:

    args = parse_args()

    start_date = parse_iso_date(args.start_date)
    end_date = parse_iso_date(args.end_date)

    if end_date < start_date:
        raise SystemExit("Invalid date range")

    output_dir = Path(args.output_dir).expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    session = build_session()

    candidate_dates = build_candidate_dates(start_date, end_date)

    targets = [DownloadTarget(d) for d in candidate_dates]

    if args.verbose:

        print(f"[FINRA] base_url={BASE_URL}")
        print(f"[FINRA] output_dir={output_dir}")
        print(f"[FINRA] candidates={len(targets)}")

    existing_urls = []

    for t in tqdm(targets, desc="probe_finra", unit="file"):

        if probe_exists(session, t.url, args.timeout):
            existing_urls.append(t.url)

    downloaded = 0
    skipped = 0
    bytes_written = 0

    for url in tqdm(existing_urls, desc="download_finra", unit="file"):

        filename = Path(url).name
        out_path = output_dir / filename

        if out_path.exists() and not args.overwrite:
            skipped += 1
            continue

        bytes_written += download_file(
            session,
            url,
            out_path,
            args.timeout
        )

        downloaded += 1

    summary = {
        "downloaded": downloaded,
        "skipped": skipped,
        "bytes_written": bytes_written,
        "local_file_count": len(list(output_dir.glob("*.csv")))
    }

    print(json.dumps(summary, indent=2))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

