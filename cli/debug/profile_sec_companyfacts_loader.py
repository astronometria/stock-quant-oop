#!/usr/bin/env python3
from __future__ import annotations

# =============================================================================
# profile_sec_companyfacts_loader.py
# -----------------------------------------------------------------------------
# Probe de performance pour comprendre pourquoi le chargement companyfacts SEC
# est lent.
#
# Ce script NE modifie PAS la DB.
#
# Il mesure, pour un petit nombre de fichiers:
# - taille fichier
# - temps de lecture disque
# - temps de json.loads
# - temps d'itération des faits
# - nombre total de lignes fact générées
# - nombre de flushs théoriques selon insert_batch_size
#
# But:
# - identifier si le problème est surtout:
#   1) lecture disque
#   2) parsing JSON
#   3) explosion du nombre de facts
#   4) stratégie d'insertion batch
# =============================================================================

import argparse
import json
import math
import time
from pathlib import Path
from typing import Any

from tqdm import tqdm


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Profile SEC companyfacts loader performance without writing to DB.")
    parser.add_argument(
        "--data-root",
        default="~/stock-quant-oop/data",
        help="Project data root.",
    )
    parser.add_argument(
        "--limit-files",
        type=int,
        default=10,
        help="Number of companyfacts files to inspect.",
    )
    parser.add_argument(
        "--insert-batch-size",
        type=int,
        default=5000,
        help="Hypothetical batch size used by the loader.",
    )
    parser.add_argument(
        "--show-top-concepts",
        type=int,
        default=10,
        help="How many biggest files to summarize at the end.",
    )
    return parser.parse_args()


def clean_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text if text else None


def count_companyfacts_rows(payload: dict[str, Any]) -> tuple[int, int, int, int]:
    """
    Compte les lignes théoriques générées pour sec_xbrl_fact_raw.

    Retourne:
    - taxonomy_count
    - concept_count
    - unit_bucket_count
    - fact_item_count
    """
    facts_root = payload.get("facts") or {}

    taxonomy_count = 0
    concept_count = 0
    unit_bucket_count = 0
    fact_item_count = 0

    if not isinstance(facts_root, dict):
        return 0, 0, 0, 0

    for taxonomy, taxonomy_payload in facts_root.items():
        if not isinstance(taxonomy_payload, dict):
            continue
        taxonomy_count += 1

        for concept, concept_payload in taxonomy_payload.items():
            if not isinstance(concept_payload, dict):
                continue
            concept_count += 1

            units_payload = concept_payload.get("units") or {}
            if not isinstance(units_payload, dict):
                continue

            for unit_name, fact_items in units_payload.items():
                if not isinstance(fact_items, list):
                    continue
                unit_bucket_count += 1
                fact_item_count += len(fact_items)

    return taxonomy_count, concept_count, unit_bucket_count, fact_item_count


def main() -> int:
    args = parse_args()

    companyfacts_root = Path(args.data_root).expanduser() / "sec" / "bulk" / "companyfacts"
    files = sorted(companyfacts_root.rglob("CIK*.json"))[: int(args.limit_files)]

    print(f"[profile_sec_companyfacts_loader] companyfacts_root={companyfacts_root}")
    print(f"[profile_sec_companyfacts_loader] files_found={len(files)}")
    print(f"[profile_sec_companyfacts_loader] insert_batch_size={args.insert_batch_size}")

    if not files:
        print("[profile_sec_companyfacts_loader] no files found")
        return 0

    summary_rows: list[dict[str, Any]] = []
    total_bytes = 0
    total_fact_rows = 0
    total_read_seconds = 0.0
    total_json_load_seconds = 0.0
    total_iter_seconds = 0.0

    for path in tqdm(files, desc="profile_companyfacts", unit="file", dynamic_ncols=True, leave=True):
        file_size = path.stat().st_size
        total_bytes += file_size

        # ---------------------------------------------------------------------
        # Temps lecture brute du fichier.
        # ---------------------------------------------------------------------
        t0 = time.perf_counter()
        raw_text = path.read_text(encoding="utf-8")
        t1 = time.perf_counter()

        # ---------------------------------------------------------------------
        # Temps json.loads.
        # ---------------------------------------------------------------------
        payload = json.loads(raw_text)
        t2 = time.perf_counter()

        # ---------------------------------------------------------------------
        # Temps d'itération métier pure, sans écriture DB.
        # ---------------------------------------------------------------------
        taxonomy_count, concept_count, unit_bucket_count, fact_item_count = count_companyfacts_rows(payload)
        t3 = time.perf_counter()

        total_fact_rows += fact_item_count
        total_read_seconds += t1 - t0
        total_json_load_seconds += t2 - t1
        total_iter_seconds += t3 - t2

        flush_count = math.ceil(fact_item_count / int(args.insert_batch_size)) if fact_item_count else 0

        summary_rows.append(
            {
                "file": path.name,
                "size_mb": round(file_size / (1024 * 1024), 2),
                "read_s": round(t1 - t0, 4),
                "json_load_s": round(t2 - t1, 4),
                "iterate_s": round(t3 - t2, 4),
                "taxonomy_count": taxonomy_count,
                "concept_count": concept_count,
                "unit_bucket_count": unit_bucket_count,
                "fact_item_count": fact_item_count,
                "flush_count_at_batch_size": flush_count,
            }
        )

    print()
    print("===== PER FILE =====")
    for row in summary_rows:
        print(json.dumps(row, ensure_ascii=False))

    print()
    print("===== TOTALS =====")
    print(f"total_files={len(summary_rows)}")
    print(f"total_bytes={total_bytes}")
    print(f"total_size_mb={round(total_bytes / (1024 * 1024), 2)}")
    print(f"total_fact_rows={total_fact_rows}")
    print(f"total_read_seconds={round(total_read_seconds, 4)}")
    print(f"total_json_load_seconds={round(total_json_load_seconds, 4)}")
    print(f"total_iter_seconds={round(total_iter_seconds, 4)}")

    if summary_rows:
        slowest = sorted(summary_rows, key=lambda x: x["fact_item_count"], reverse=True)[: int(args.show_top_concepts)]
        print()
        print("===== BIGGEST FACT FILES =====")
        for row in slowest:
            print(
                f'{row["file"]} size_mb={row["size_mb"]} '
                f'fact_item_count={row["fact_item_count"]} '
                f'flush_count={row["flush_count_at_batch_size"]} '
                f'read_s={row["read_s"]} json_load_s={row["json_load_s"]} iterate_s={row["iterate_s"]}'
            )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
