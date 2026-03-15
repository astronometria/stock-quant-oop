#!/usr/bin/env python3
from __future__ import annotations

import argparse
import calendar
import json
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

import requests

try:
    from tqdm import tqdm
except Exception:
    def tqdm(iterable, **kwargs):
        return iterable


BASE_URL = "https://cdn.finra.org/equity/otcmarket/biweekly"
DEFAULT_START_DATE = "2017-12-29"
DEFAULT_END_DATE = "2026-02-13"


@dataclass(frozen=True, slots=True)
class DownloadTarget:
    file_date: date

    @property
    def filename(self) -> str:
        return f"shrt{self.file_date.strftime('%Y%m%d')}.csv"

    @property
    def url(self) -> str:
        return f"{BASE_URL}/{self.filename}"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download FINRA equity short interest CSV files from CDN.")
    parser.add_argument(
        "--output-dir",
        default="~/stock-quant-oop/data/raw/finra_short_interest",
        help="Directory where FINRA short interest files will be stored.",
    )
    parser.add_argument(
        "--start-date",
        default=DEFAULT_START_DATE,
        help="Inclusive start date YYYY-MM-DD. Do not set before 2017-12-29.",
    )
    parser.add_argument(
        "--end-date",
        default=DEFAULT_END_DATE,
        help="Inclusive end date YYYY-MM-DD.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing local files.",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=60,
        help="HTTP timeout in seconds.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose output.",
    )
    return parser.parse_args()


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

    dates = sorted(set(dates))
    return dates


def build_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": (
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/122.0 Safari/537.36"
            ),
            "Accept": "text/csv,application/octet-stream,*/*",
        }
    )
    return session


def probe_exists(session: requests.Session, url: str, timeout: int) -> bool:
    response = session.get(url, timeout=timeout, stream=True)
    try:
        if response.status_code == 200:
            return True
        if response.status_code == 404:
            return False
        response.raise_for_status()
        return True
    finally:
        response.close()


def download_file(
    session: requests.Session,
    url: str,
    out_path: Path,
    timeout: int,
) -> int:
    response = session.get(url, timeout=timeout, stream=True)
    response.raise_for_status()
    try:
        bytes_written = 0
        with out_path.open("wb") as handle:
            for chunk in response.iter_content(chunk_size=1024 * 256):
                if not chunk:
                    continue
                handle.write(chunk)
                bytes_written += len(chunk)
        return bytes_written
    finally:
        response.close()


def main() -> int:
    args = parse_args()

    start_date = parse_iso_date(args.start_date)
    end_date = parse_iso_date(args.end_date)

    if start_date < parse_iso_date(DEFAULT_START_DATE):
        raise SystemExit(f"--start-date must be >= {DEFAULT_START_DATE}")
    if end_date < start_date:
        raise SystemExit("--end-date must be >= --start-date")

    output_dir = Path(args.output_dir).expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    session = build_session()
    candidate_dates = build_candidate_dates(start_date=start_date, end_date=end_date)
    targets = [DownloadTarget(file_date=d) for d in candidate_dates]

    if args.verbose:
        print(f"[download_finra_short_interest] base_url={BASE_URL}")
        print(f"[download_finra_short_interest] output_dir={output_dir}")
        print(f"[download_finra_short_interest] start_date={start_date}")
        print(f"[download_finra_short_interest] end_date={end_date}")
        print(f"[download_finra_short_interest] candidate_count={len(targets)}")

    existing_urls: list[str] = []
    missing_urls: list[str] = []
    failed_probe_urls: list[str] = []

    for target in tqdm(
        targets,
        desc="probe_finra_short_interest",
        unit="file",
        dynamic_ncols=True,
        mininterval=0.5,
    ):
        try:
            if probe_exists(session=session, url=target.url, timeout=args.timeout):
                existing_urls.append(target.url)
            else:
                missing_urls.append(target.url)
        except Exception:
            failed_probe_urls.append(target.url)

    downloaded_files = 0
    skipped_existing = 0
    failed_downloads = 0
    bytes_written = 0
    failed_download_urls: list[str] = []

    for url in tqdm(
        existing_urls,
        desc="download_finra_short_interest",
        unit="file",
        dynamic_ncols=True,
        mininterval=0.5,
    ):
        filename = Path(url).name
        out_path = output_dir / filename

        if out_path.exists() and not args.overwrite:
            skipped_existing += 1
            continue

        try:
            bytes_written += download_file(
                session=session,
                url=url,
                out_path=out_path,
                timeout=args.timeout,
            )
            downloaded_files += 1
        except Exception:
            failed_downloads += 1
            failed_download_urls.append(url)

    local_files = sorted(p.name for p in output_dir.iterdir() if p.is_file())

    print(
        json.dumps(
            {
                "base_url": BASE_URL,
                "start_date": str(start_date),
                "end_date": str(end_date),
                "candidate_count": len(targets),
                "existing_url_count": len(existing_urls),
                "missing_url_count": len(missing_urls),
                "failed_probe_count": len(failed_probe_urls),
                "downloaded_files": downloaded_files,
                "skipped_existing": skipped_existing,
                "failed_downloads": failed_downloads,
                "bytes_written": bytes_written,
                "output_dir": str(output_dir),
                "local_file_count": len(local_files),
                "sample_existing_urls": existing_urls[:20],
                "sample_missing_urls": missing_urls[:20],
                "sample_failed_probe_urls": failed_probe_urls[:20],
                "sample_failed_download_urls": failed_download_urls[:20],
                "sample_local_files": local_files[:20],
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0 if not failed_probe_urls and failed_downloads == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
