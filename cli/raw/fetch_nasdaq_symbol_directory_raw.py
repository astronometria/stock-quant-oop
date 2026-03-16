#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
from datetime import date

from stock_quant.infrastructure.config.settings_loader import build_app_config
from stock_quant.infrastructure.providers.symbols.nasdaq_symbol_directory_loader import (
    NasdaqSymbolDirectoryLoader,
)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Download NASDAQ symbol directory raw files."
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
        else Path(config.project_root) / "data" / "symbol_sources" / "nasdaq"
    )

    data_dir.mkdir(parents=True, exist_ok=True)

    loader = NasdaqSymbolDirectoryLoader()
    frames = loader.download_frames()

    snapshot_date = date.today().isoformat()

    written = 0

    for name, frame in frames.items():
        path = data_dir / f"{name}_{snapshot_date}.csv"
        frame.to_csv(path, index=False)
        written += len(frame)

        if args.verbose:
            print(f"[fetch_nasdaq_symbol_directory_raw] wrote {path}")

    print(f"downloaded rows={written}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
