#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
import duckdb

from stock_quant.infrastructure.db.symbol_normalization_schema import SymbolNormalizationSchemaManager


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--db-path", required=True)
    return p.parse_args()


def main():
    args = parse_args()

    con = duckdb.connect(Path(args.db_path).expanduser().resolve().as_posix())

    manager = SymbolNormalizationSchemaManager(con)
    manager.create_table()
    manager.insert_default_rules()

    print("[init_symbol_normalization] done")

    con.close()


if __name__ == "__main__":
    raise SystemExit(main())
