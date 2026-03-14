#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

from stock_quant.infrastructure.providers.finra.finra_provider import FinraProvider


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Load FINRA raw short interest files through provider layer.")
    parser.add_argument(
        "--source",
        action="append",
        dest="sources",
        default=[],
        help="CSV or ZIP source path. Repeat this flag for multiple inputs.",
    )
    parser.add_argument("--symbol", action="append", dest="symbols", default=[], help="Optional symbol filter.")
    parser.add_argument("--start-date", default=None, help="Optional start date YYYY-MM-DD.")
    parser.add_argument("--end-date", default=None, help="Optional end date YYYY-MM-DD.")
    return parser.parse_args()


def parse_date(value: str | None):
    if value is None:
        return None
    from datetime import date
    return date.fromisoformat(value)


def main() -> int:
    args = parse_args()
    provider = FinraProvider(source_paths=args.sources)

    frame = provider.fetch_short_interest_frame(
        symbols=args.symbols or None,
        start_date=parse_date(args.start_date),
        end_date=parse_date(args.end_date),
    )

    result = {
        "rows": int(len(frame)),
        "columns": list(frame.columns),
        "sample": frame.head(10).to_dict(orient="records"),
    }
    print(json.dumps(result, indent=2, default=str))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
