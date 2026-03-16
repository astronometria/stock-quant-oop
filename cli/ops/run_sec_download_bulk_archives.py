#!/usr/bin/env python3
from __future__ import annotations

# =============================================================================
# run_sec_download_bulk_archives.py
# -----------------------------------------------------------------------------
# Runner CLI pour télécharger les archives bulk officielles SEC.
#
# Étape visée:
# - construire une couche raw durable sur disque pour tout le marché
#
# Sortie disque:
#   data/sec/bulk/submissions.zip
#   data/sec/bulk/submissions/
#   data/sec/bulk/companyfacts.zip
#   data/sec/bulk/companyfacts/
# =============================================================================

import argparse
import json
from pathlib import Path

from stock_quant.infrastructure.providers.sec.sec_bulk_archive_provider import SecBulkArchiveProvider


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Download SEC bulk archives to disk.")
    parser.add_argument(
        "--data-root",
        default="~/stock-quant-oop/data",
        help="Project data root.",
    )
    parser.add_argument(
        "--user-agent",
        required=True,
        help="SEC-compliant User-Agent, e.g. 'stock-quant-oop you@example.com'",
    )
    parser.add_argument(
        "--refresh-existing",
        action="store_true",
        help="Force re-download and re-extract existing archives.",
    )
    parser.add_argument(
        "--only-submissions",
        action="store_true",
        help="Download only submissions.zip.",
    )
    parser.add_argument(
        "--only-companyfacts",
        action="store_true",
        help="Download only companyfacts.zip.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    include_submissions = not args.only_companyfacts
    include_companyfacts = not args.only_submissions

    data_root = Path(args.data_root).expanduser()

    print(f"[run_sec_download_bulk_archives] data_root={data_root}")
    print(f"[run_sec_download_bulk_archives] include_submissions={include_submissions}")
    print(f"[run_sec_download_bulk_archives] include_companyfacts={include_companyfacts}")
    print(f"[run_sec_download_bulk_archives] refresh_existing={bool(args.refresh_existing)}")

    provider = SecBulkArchiveProvider(
        data_root=data_root,
        user_agent=args.user_agent,
    )

    metrics = provider.download_and_extract(
        include_submissions=include_submissions,
        include_companyfacts=include_companyfacts,
        refresh_existing=bool(args.refresh_existing),
    )

    print("[run_sec_download_bulk_archives] metrics=" + json.dumps(metrics, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
