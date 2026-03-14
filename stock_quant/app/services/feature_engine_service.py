from __future__ import annotations

from datetime import datetime
from typing import Any

from stock_quant.domain.entities.research_feature import ResearchFeatureDaily


class FeatureEngineService:
    def merge_features(
        self,
        technical_rows: list[dict[str, Any]],
        fundamental_rows: list[dict[str, Any]],
        short_rows: list[dict[str, Any]],
        news_rows: list[dict[str, Any]],
    ) -> tuple[list[ResearchFeatureDaily], dict[str, int]]:
        merged: dict[tuple[str, object], dict[str, Any]] = {}

        def ensure_row(instrument_id: str, as_of_date, symbol: str | None = None, company_id: str | None = None):
            key = (instrument_id, as_of_date)
            if key not in merged:
                merged[key] = {
                    "instrument_id": instrument_id,
                    "company_id": company_id,
                    "symbol": symbol or "",
                    "as_of_date": as_of_date,
                }
            else:
                if symbol and not merged[key].get("symbol"):
                    merged[key]["symbol"] = symbol
                if company_id and not merged[key].get("company_id"):
                    merged[key]["company_id"] = company_id
            return merged[key]

        for row in technical_rows:
            entry = ensure_row(row["instrument_id"], row["as_of_date"], row.get("symbol"), row.get("company_id"))
            entry["close_to_sma_20"] = row.get("close_to_sma_20")
            entry["rsi_14"] = row.get("rsi_14")

        for row in fundamental_rows:
            company_id = row.get("company_id")
            as_of_date = row.get("as_of_date")
            for key, entry in merged.items():
                if entry.get("company_id") == company_id and entry.get("as_of_date") == as_of_date:
                    entry["revenue"] = row.get("revenue")
                    entry["net_income"] = row.get("net_income")
                    entry["net_margin"] = row.get("net_margin")
                    entry["debt_to_equity"] = row.get("debt_to_equity")
                    entry["return_on_assets"] = row.get("return_on_assets")

        for row in short_rows:
            entry = ensure_row(row["instrument_id"], row["as_of_date"], row.get("symbol"), row.get("company_id"))
            entry["short_interest"] = row.get("short_interest")
            entry["days_to_cover"] = row.get("days_to_cover")
            entry["short_volume_ratio"] = row.get("short_volume_ratio")

        for row in news_rows:
            entry = ensure_row(row["instrument_id"], row["as_of_date"], row.get("symbol"), row.get("company_id"))
            entry["article_count_1d"] = row.get("article_count_1d")
            entry["unique_cluster_count_1d"] = row.get("unique_cluster_count_1d")
            entry["avg_link_confidence"] = row.get("avg_link_confidence")

        out: list[ResearchFeatureDaily] = []
        for _, row in sorted(merged.items(), key=lambda x: (x[1].get("symbol", ""), x[1].get("as_of_date"))):
            out.append(
                ResearchFeatureDaily(
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
                    source_name="research",
                    created_at=datetime.utcnow(),
                )
            )

        return out, {"research_feature_rows": len(out)}
