#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
from pathlib import Path

try:
    from tqdm import tqdm
except Exception:
    def tqdm(iterable, **kwargs):
        return iterable


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export extracted Stooq txt files to one normalized CSV for bulk DuckDB load."
    )
    parser.add_argument(
        "--root-dir",
        default="~/stock-quant/data/extracted/stooq/data/daily/us",
        help="Root extracted Stooq daily US directory.",
    )
    parser.add_argument(
        "--output-csv",
        default="~/stock-quant/data/normalized/stooq_us_daily_normalized.csv",
        help="Output normalized CSV path.",
    )
    parser.add_argument(
        "--limit-files",
        type=int,
        default=0,
        help="Maximum number of txt files to parse. 0 means no limit.",
    )
    parser.add_argument(
        "--limit-rows",
        type=int,
        default=0,
        help="Maximum number of rows to export. 0 means no limit.",
    )
    parser.add_argument(
        "--exclude-etfs",
        action="store_true",
        help="Skip ETF directories.",
    )
    parser.add_argument(
        "--symbols",
        default="",
        help="Comma-separated symbols to include, e.g. AAPL,MSFT,BABA",
    )
    parser.add_argument(
        "--symbols-file",
        default="",
        help="Optional file with one symbol per line.",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Enable verbose output.",
    )
    return parser.parse_args()


def normalize_symbol(raw: str) -> str:
    symbol = raw.strip().upper()
    if symbol.endswith(".US"):
        symbol = symbol[:-3]
    return symbol


def infer_asset_class(source_path: str) -> str:
    low = source_path.lower()
    if "etfs" in low:
        return "ETF"
    if "stocks" in low:
        return "STOCK"
    return "UNKNOWN"


def infer_venue_group(source_path: str) -> str:
    low = source_path.lower()
    if "nasdaq" in low:
        return "NASDAQ"
    if "nysemkt" in low:
        return "NYSEMKT"
    if "nyse" in low:
        return "NYSE"
    return "UNKNOWN"


def load_symbol_filter(args: argparse.Namespace) -> set[str]:
    symbols: set[str] = set()

    if args.symbols.strip():
        for item in args.symbols.split(","):
            sym = item.strip().upper()
            if sym:
                symbols.add(sym)

    if args.symbols_file.strip():
        path = Path(args.symbols_file).expanduser().resolve()
        if not path.exists():
            raise SystemExit(f"symbols file not found: {path}")
        for line in path.read_text(encoding="utf-8").splitlines():
            sym = line.strip().upper()
            if sym:
                symbols.add(sym)

    return symbols


def iter_txt_files(root_dir: Path, exclude_etfs: bool, symbol_filter: set[str]) -> list[Path]:
    files = sorted(root_dir.rglob("*.txt"))

    if exclude_etfs:
        files = [p for p in files if "etfs" not in str(p).lower()]

    if symbol_filter:
        filtered: list[Path] = []
        for path in files:
            sym = normalize_symbol(path.stem)
            if sym in symbol_filter:
                filtered.append(path)
        files = filtered

    return files


def main() -> int:
    args = parse_args()
    root_dir = Path(args.root_dir).expanduser().resolve()
    output_csv = Path(args.output_csv).expanduser().resolve()

    if not root_dir.exists():
        raise SystemExit(f"root dir not found: {root_dir}")

    output_csv.parent.mkdir(parents=True, exist_ok=True)
    symbol_filter = load_symbol_filter(args)

    files = iter_txt_files(root_dir, args.exclude_etfs, symbol_filter)
    if args.limit_files > 0:
        files = files[:args.limit_files]

    exported_rows = 0
    parsed_files = 0
    distinct_symbols: set[str] = set()

    with output_csv.open("w", newline="", encoding="utf-8") as f_out:
        writer = csv.writer(f_out)
        writer.writerow([
            "symbol",
            "price_date",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "source_name",
            "source_path",
            "asset_class",
            "venue_group",
        ])

        for path in tqdm(files, desc="export stooq csv", unit="file", dynamic_ncols=True):
            try:
                with path.open("r", encoding="utf-8", errors="replace") as f_in:
                    reader = csv.DictReader(f_in)
                    source_path = str(path)
                    asset_class = infer_asset_class(source_path)
                    venue_group = infer_venue_group(source_path)

                    wrote_any = False
                    for record in reader:
                        try:
                            period = (record.get("<PER>") or "").strip().upper()
                            if period != "D":
                                continue

                            symbol = normalize_symbol(record["<TICKER>"])
                            yyyymmdd = (record["<DATE>"] or "").strip()
                            if len(yyyymmdd) != 8 or not yyyymmdd.isdigit():
                                continue

                            price_date = f"{yyyymmdd[:4]}-{yyyymmdd[4:6]}-{yyyymmdd[6:8]}"
                            open_price = float(record["<OPEN>"])
                            high_price = float(record["<HIGH>"])
                            low_price = float(record["<LOW>"])
                            close_price = float(record["<CLOSE>"])
                            volume = int(float(record["<VOL>"]))
                        except Exception:
                            continue

                        writer.writerow([
                            symbol,
                            price_date,
                            open_price,
                            high_price,
                            low_price,
                            close_price,
                            volume,
                            "stooq_dir_full",
                            source_path,
                            asset_class,
                            venue_group,
                        ])

                        exported_rows += 1
                        distinct_symbols.add(symbol)
                        wrote_any = True

                        if args.limit_rows > 0 and exported_rows >= args.limit_rows:
                            break

                    if wrote_any:
                        parsed_files += 1

                if args.limit_rows > 0 and exported_rows >= args.limit_rows:
                    break
            except Exception:
                continue

    if args.verbose:
        print(f"[export_stooq_dir_to_normalized_csv] root_dir={root_dir}")
        print(f"[export_stooq_dir_to_normalized_csv] output_csv={output_csv}")
        print(f"[export_stooq_dir_to_normalized_csv] symbol_filter_count={len(symbol_filter)}")
        print(f"[export_stooq_dir_to_normalized_csv] parsed_files={parsed_files}")
        print(f"[export_stooq_dir_to_normalized_csv] distinct_symbols={len(distinct_symbols)}")
        print(f"[export_stooq_dir_to_normalized_csv] exported_rows={exported_rows}")

    print(
        "Exported normalized Stooq CSV: "
        f"parsed_files={parsed_files} distinct_symbols={len(distinct_symbols)} "
        f"exported_rows={exported_rows} output_csv={output_csv}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
