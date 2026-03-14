from __future__ import annotations

from datetime import datetime
from typing import Any

from stock_quant.domain.entities.research_dataset import ResearchDatasetDaily


class DatasetBuilder:
    def build(
        self,
        dataset_name: str,
        dataset_version: str,
        feature_rows: list[dict[str, Any]],
        return_label_rows: list[dict[str, Any]],
        volatility_label_rows: list[dict[str, Any]],
    ) -> tuple[list[ResearchDatasetDaily], dict[str, int]]:
        return_map = {
            (row["instrument_id"], row["as_of_date"]): row
            for row in return_label_rows
        }
        vol_map = {
            (row["instrument_id"], row["as_of_date"]): row
            for row in volatility_label_rows
        }

        out: list[ResearchDatasetDaily] = []

        for row in feature_rows:
            key = (row["instrument_id"], row["as_of_date"])
            ret = return_map.get(key, {})
            vol = vol_map.get(key, {})

            out.append(
                ResearchDatasetDaily(
                    dataset_name=dataset_name,
                    dataset_version=dataset_version,
                    instrument_id=row["instrument_id"],
                    company_id=row.get("company_id"),
                    symbol=row.get("symbol") or "",
                    as_of_date=row["as_of_date"],
                    close_to_sma_20=row.get("close_to_sma_20"),
                    rsi_14=row.get("rsi_14"),
                    revenue=row.get("revenue"),
                    net_income=row.get("net_income"),
                    net_margin=row.get("net_margin"),
                    debt_to_equity=row.get("debt_to_equity"),
                    return_on_assets=row.get("return_on_assets"),
                    short_interest=row.get("short_interest"),
                    days_to_cover=row.get("days_to_cover"),
                    short_volume_ratio=row.get("short_volume_ratio"),
                    article_count_1d=row.get("article_count_1d"),
                    unique_cluster_count_1d=row.get("unique_cluster_count_1d"),
                    avg_link_confidence=row.get("avg_link_confidence"),
                    fwd_return_1d=ret.get("fwd_return_1d"),
                    fwd_return_5d=ret.get("fwd_return_5d"),
                    fwd_return_20d=ret.get("fwd_return_20d"),
                    direction_1d=ret.get("direction_1d"),
                    direction_5d=ret.get("direction_5d"),
                    direction_20d=ret.get("direction_20d"),
                    realized_vol_20d=vol.get("realized_vol_20d"),
                    created_at=datetime.utcnow(),
                )
            )

        return out, {"research_dataset_rows": len(out)}
