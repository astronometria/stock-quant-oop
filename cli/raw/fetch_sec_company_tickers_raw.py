#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
from datetime import date

from stock_quant.infrastructure.config.settings_loader import build_app_config
from stock_quant.infrastructure.providers.symbols.sec_company_ticker_loader import (
    SecCompanyTickerLoader,
)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Download SEC company ticker mapping raw file."
    )
    parser.add_argument("--data-dir", default=None)
    parser.add_argument("--verbose", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    config = build_app_config()
    config.ensure_directories()

    data_dir = (
        Path(args.data_dir).expanduser().resolve()
        if args.data_dir
        else Path(config.project_root) / "data" / "symbol_sources" / "sec"
    )

    data_dir.mkdir(parents=True, exist_ok=True)

    loader = SecCompanyTickerLoader()
    frame = loader.download_frame()

    snapshot_date = date.today().isoformat()

    path = data_dir / f"sec_company_tickers_{snapshot_date}.csv"
    frame.to_csv(path, index=False)

    if args.verbose:
        print(f"[fetch_sec_company_tickers_raw] wrote {path}")

    print(f"downloaded rows={len(frame)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
