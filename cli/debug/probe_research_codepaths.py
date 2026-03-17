#!/usr/bin/env python3
from __future__ import annotations

from pathlib import Path


def show_file(path: str, start: int = 1, end: int = 260) -> None:
    p = Path(path)
    print(f"\n===== FILE: {p} =====")
    if not p.exists():
        print("MISSING")
        return
    lines = p.read_text(encoding="utf-8").splitlines()
    for idx, line in enumerate(lines[start - 1:end], start=start):
        print(f"{idx:04d}: {line}")


def main() -> int:
    files = [
        "cli/core/build_feature_engine.py",
        "cli/core/build_label_engine.py",
        "cli/core/build_dataset_builder.py",
        "stock_quant/pipelines/build_feature_engine_pipeline.py",
        "stock_quant/pipelines/build_label_engine_pipeline.py",
        "stock_quant/research/feature_engine/technical_features_engine.py",
        "stock_quant/research/label_engine/forward_returns_labeler.py",
        "stock_quant/infrastructure/repositories/duckdb_feature_engine_repository.py",
        "stock_quant/infrastructure/repositories/duckdb_label_engine_repository.py",
        "stock_quant/infrastructure/repositories/duckdb_dataset_builder_repository.py",
    ]
    for file_path in files:
        show_file(file_path, 1, 260)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
