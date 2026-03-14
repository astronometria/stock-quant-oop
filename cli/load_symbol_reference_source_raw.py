#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json

from stock_quant.infrastructure.providers.symbols.symbol_source_loader import SymbolSourceLoader


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load symbol raw files through provider layer.")
    parser.add_argument(
        "--source",
        action="append",
        dest="sources",
        default=[],
        help="CSV source path. Repeat this flag for multiple inputs.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    loader = SymbolSourceLoader(source_paths=args.sources)

    frame = loader.fetch_symbols_frame()

    result = {
        "rows": int(len(frame)),
        "columns": list(frame.columns),
        "sample": frame.head(10).to_dict(orient="records"),
    }
    print(json.dumps(result, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
