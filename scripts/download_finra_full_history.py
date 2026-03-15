#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import logging
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path

import requests

try:
    from tqdm import tqdm
except Exception:
    def tqdm(iterable=None, **kwargs):
        return iterable if iterable is not None else []

USER_AGENT = "stock-quant-oop/finra-fast-downloader"
DAILY_BASE = "https://cdn.finra.org/equity/regsho/daily"
DEFAULT_PREFIXES = ("CNMS", "FNQC", "FNRA", "FNSQ", "FNYX")
_thread_local = threading.local()


@dataclass(frozen=True)
class DownloadJob:
    dataset: str
    url: str
    path: Path


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Fast deterministic FINRA non-OTC daily downloader.")
    p.add_argument("--output-dir", default="~/stock-quant-oop/data/finra", help="Destination root")
    p.add_argument("--start-date", default="2009-11-09", help="YYYY-MM-DD")
    p.add_argument("--end-date", default=str(date.today()), help="YYYY-MM-DD")
    p.add_argument("--max-workers", type=int, default=8, help="Parallel download workers")
    p.add_argument("--timeout", type=int, default=45, help="HTTP timeout seconds")
    p.add_argument("--retries", type=int, default=2, help="Retries for transient failures")
    p.add_argument("--prefixes", default=",".join(DEFAULT_PREFIXES), help="Comma-separated FINRA daily prefixes")
    p.add_argument("--overwrite", action="store_true", help="Re-download files even if they already exist")
    p.add_argument("--verbose", action="store_true", help="Verbose logging")
    return p.parse_args()


def configure_logging(verbose: bool) -> None:
    logging.basicConfig(
        level=logging.INFO if verbose else logging.WARNING,
        format="%(asctime)s %(levelname)s %(message)s",
    )


def get_session() -> requests.Session:
    session = getattr(_thread_local, "session", None)
    if session is None:
        session = requests.Session()
        session.headers.update({"User-Agent": USER_AGENT})
        _thread_local.session = session
    return session


def parse_iso_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def iter_weekdays(start_date: date, end_date: date):
    current = start_date
    while current <= end_date:
        if current.weekday() < 5:
            yield current
        current += timedelta(days=1)


def build_jobs(output_dir: Path, start_date: date, end_date: date, prefixes: list[str]) -> list[DownloadJob]:
    jobs: list[DownloadJob] = []
    daily_root = output_dir / "daily_short_sale_volume"
    for d in iter_weekdays(start_date, end_date):
        ymd = d.strftime("%Y%m%d")
        for prefix in prefixes:
            filename = f"{prefix}shvol{ymd}.txt"
            url = f"{DAILY_BASE}/{filename}"
            path = daily_root / prefix / filename
            jobs.append(DownloadJob(dataset="daily_short_sale_volume", url=url, path=path))
    return jobs


def download_one(job: DownloadJob, timeout: int, retries: int, overwrite: bool) -> dict:
    job.path.parent.mkdir(parents=True, exist_ok=True)

    if job.path.exists() and job.path.stat().st_size > 0 and not overwrite:
        return {
            "dataset": job.dataset,
            "url": job.url,
            "path": str(job.path),
            "status": "skipped_existing",
            "bytes": job.path.stat().st_size,
        }

    session = get_session()
    tmp_path = job.path.with_suffix(job.path.suffix + ".part")

    for attempt in range(retries + 1):
        try:
            with session.get(job.url, timeout=timeout, stream=True, allow_redirects=True) as resp:
                if resp.status_code == 404:
                    return {
                        "dataset": job.dataset,
                        "url": job.url,
                        "path": str(job.path),
                        "status": "missing_remote",
                        "bytes": 0,
                    }
                resp.raise_for_status()
                total = 0
                with open(tmp_path, "wb") as fh:
                    for chunk in resp.iter_content(chunk_size=1024 * 1024):
                        if not chunk:
                            continue
                        fh.write(chunk)
                        total += len(chunk)
                tmp_path.replace(job.path)
                return {
                    "dataset": job.dataset,
                    "url": job.url,
                    "path": str(job.path),
                    "status": "downloaded",
                    "bytes": total,
                }
        except requests.HTTPError as exc:
            code = getattr(exc.response, "status_code", None)
            if code == 404:
                return {
                    "dataset": job.dataset,
                    "url": job.url,
                    "path": str(job.path),
                    "status": "missing_remote",
                    "bytes": 0,
                }
            if attempt >= retries:
                try:
                    if tmp_path.exists():
                        tmp_path.unlink()
                except Exception:
                    pass
                return {
                    "dataset": job.dataset,
                    "url": job.url,
                    "path": str(job.path),
                    "status": f"failed_http_{code or 'unknown'}",
                    "bytes": 0,
                }
        except Exception as exc:
            if attempt >= retries:
                try:
                    if tmp_path.exists():
                        tmp_path.unlink()
                except Exception:
                    pass
                return {
                    "dataset": job.dataset,
                    "url": job.url,
                    "path": str(job.path),
                    "status": f"failed:{type(exc).__name__}",
                    "bytes": 0,
                }

    return {
        "dataset": job.dataset,
        "url": job.url,
        "path": str(job.path),
        "status": "failed_unknown",
        "bytes": 0,
    }


def write_manifest(rows: list[dict], output_dir: Path) -> Path:
    manifest_path = output_dir / "finra_daily_non_otc_manifest.csv"
    with open(manifest_path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=["dataset", "url", "path", "status", "bytes"],
        )
        writer.writeheader()
        writer.writerows(rows)
    return manifest_path


def main() -> int:
    args = parse_args()
    configure_logging(args.verbose)

    output_dir = Path(args.output_dir).expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    start_date = parse_iso_date(args.start_date)
    end_date = parse_iso_date(args.end_date)
    prefixes = [p.strip().upper() for p in args.prefixes.split(",") if p.strip()]

    jobs = build_jobs(
        output_dir=output_dir,
        start_date=start_date,
        end_date=end_date,
        prefixes=prefixes,
    )

    print(f"[download_finra_full_history] output_dir={output_dir}")
    print(f"[download_finra_full_history] start_date={start_date}")
    print(f"[download_finra_full_history] end_date={end_date}")
    print(f"[download_finra_full_history] prefixes={','.join(prefixes)}")
    print(f"[download_finra_full_history] max_workers={args.max_workers}")
    print(f"[download_finra_full_history] total_jobs={len(jobs)}")

    manifest_rows: list[dict] = []
    downloaded_files = 0
    skipped_existing = 0
    missing_remote = 0
    failed = 0
    bytes_written = 0

    with ThreadPoolExecutor(max_workers=max(1, args.max_workers)) as executor:
        futures = [
            executor.submit(
                download_one,
                job,
                args.timeout,
                args.retries,
                args.overwrite,
            )
            for job in jobs
        ]

        for future in tqdm(as_completed(futures), total=len(futures), desc="daily_finra", unit="file", dynamic_ncols=True):
            result = future.result()
            manifest_rows.append(result)
            status = result["status"]
            if status == "downloaded":
                downloaded_files += 1
                bytes_written += int(result["bytes"])
            elif status == "skipped_existing":
                skipped_existing += 1
            elif status == "missing_remote":
                missing_remote += 1
            else:
                failed += 1

    manifest_path = write_manifest(manifest_rows, output_dir)

    print("===== SUMMARY =====")
    print(f"downloaded_files={downloaded_files}")
    print(f"skipped_existing={skipped_existing}")
    print(f"missing_remote={missing_remote}")
    print(f"failed={failed}")
    print(f"bytes_written={bytes_written}")
    print(f"manifest_path={manifest_path}")

    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
