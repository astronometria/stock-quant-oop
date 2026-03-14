from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from typing import Any

from stock_quant.domain.entities.fundamental import FundamentalFeatureDaily, FundamentalSnapshot


NUMERIC_CONCEPT_MAP: dict[str, str] = {
    "REVENUES": "revenue",
    "SALESREVENUENET": "revenue",
    "NETINCOMELOSS": "net_income",
    "ASSETS": "assets",
    "LIABILITIES": "liabilities",
    "STOCKHOLDERSEQUITY": "equity",
    "STOCKHOLDERSEQUITYINCLUDINGPORTIONATTRIBUTABLETONONCONTROLLINGINTEREST": "equity",
    "NETCASHPROVIDEDBYUSEDINOPERATINGACTIVITIES": "operating_cash_flow",
    "COMMONSTOCKSHARESOUTSTANDING": "shares_outstanding",
}


class FundamentalsService:
    def build_snapshots(
        self,
        sec_fact_rows: list[dict[str, Any]],
        sec_filing_rows: list[dict[str, Any]],
    ) -> tuple[list[FundamentalSnapshot], list[FundamentalSnapshot], list[FundamentalSnapshot], dict[str, int]]:
        filing_map: dict[str, dict[str, Any]] = {
            str(row.get("filing_id")): row for row in sec_filing_rows
        }

        grouped: dict[tuple[str, str, object], dict[str, Any]] = defaultdict(dict)

        for row in sec_fact_rows:
            filing_id = str(row.get("filing_id", "")).strip()
            if not filing_id:
                continue

            filing = filing_map.get(filing_id)
            if filing is None:
                continue

            company_id = filing.get("company_id")
            cik = str(filing.get("cik", "")).strip()
            period_end_date = row.get("period_end_date")
            form_type = str(filing.get("form_type", "")).strip().upper()
            period_type = self._infer_period_type(form_type)

            if not company_id or not cik or period_end_date is None:
                continue

            concept = str(row.get("concept", "")).strip().upper()
            target_field = NUMERIC_CONCEPT_MAP.get(concept)
            if target_field is None:
                continue

            key = (company_id, period_type, period_end_date)
            grouped[key]["company_id"] = company_id
            grouped[key]["cik"] = cik
            grouped[key]["period_type"] = period_type
            grouped[key]["period_end_date"] = period_end_date
            grouped[key]["available_at"] = filing.get("available_at")
            grouped[key]["source_name"] = row.get("source_name") or "sec"
            grouped[key][target_field] = row.get("value_numeric")

        quarterly: list[FundamentalSnapshot] = []
        annual: list[FundamentalSnapshot] = []

        for _, payload in grouped.items():
            snapshot = FundamentalSnapshot(
                company_id=payload["company_id"],
                cik=payload["cik"],
                period_type=payload["period_type"],
                period_end_date=payload["period_end_date"],
                available_at=payload.get("available_at"),
                revenue=payload.get("revenue"),
                net_income=payload.get("net_income"),
                assets=payload.get("assets"),
                liabilities=payload.get("liabilities"),
                equity=payload.get("equity"),
                operating_cash_flow=payload.get("operating_cash_flow"),
                shares_outstanding=payload.get("shares_outstanding"),
                source_name=payload.get("source_name"),
                created_at=datetime.utcnow(),
            )
            if snapshot.period_type == "ANNUAL":
                annual.append(snapshot)
            else:
                quarterly.append(snapshot)

        ttm = self._build_ttm(quarterly)

        metrics = {
            "sec_fact_rows": len(sec_fact_rows),
            "sec_filing_rows": len(sec_filing_rows),
            "quarterly_rows": len(quarterly),
            "annual_rows": len(annual),
            "ttm_rows": len(ttm),
        }
        return quarterly, annual, ttm, metrics

    def build_features_daily(
        self,
        quarterly_rows: list[FundamentalSnapshot],
        annual_rows: list[FundamentalSnapshot],
        ttm_rows: list[FundamentalSnapshot],
    ) -> tuple[list[FundamentalFeatureDaily], dict[str, int]]:
        features: list[FundamentalFeatureDaily] = []

        for row in quarterly_rows + annual_rows + ttm_rows:
            net_margin = None
            if row.revenue not in (None, 0) and row.net_income is not None:
                net_margin = row.net_income / row.revenue

            debt_to_equity = None
            if row.equity not in (None, 0) and row.liabilities is not None:
                debt_to_equity = row.liabilities / row.equity

            return_on_assets = None
            if row.assets not in (None, 0) and row.net_income is not None:
                return_on_assets = row.net_income / row.assets

            features.append(
                FundamentalFeatureDaily(
                    company_id=row.company_id,
                    as_of_date=row.period_end_date,
                    period_type=row.period_type,
                    revenue=row.revenue,
                    net_income=row.net_income,
                    assets=row.assets,
                    liabilities=row.liabilities,
                    equity=row.equity,
                    operating_cash_flow=row.operating_cash_flow,
                    shares_outstanding=row.shares_outstanding,
                    net_margin=net_margin,
                    debt_to_equity=debt_to_equity,
                    return_on_assets=return_on_assets,
                    source_name=row.source_name,
                    created_at=datetime.utcnow(),
                )
            )

        return features, {"fundamental_feature_rows": len(features)}

    def _infer_period_type(self, form_type: str) -> str:
        if form_type in {"10-K", "20-F", "40-F"}:
            return "ANNUAL"
        return "QUARTERLY"

    def _build_ttm(self, quarterly_rows: list[FundamentalSnapshot]) -> list[FundamentalSnapshot]:
        by_company: dict[str, list[FundamentalSnapshot]] = defaultdict(list)
        for row in quarterly_rows:
            by_company[row.company_id].append(row)

        ttm_rows: list[FundamentalSnapshot] = []

        for company_id, rows in by_company.items():
            ordered = sorted(
                [row for row in rows if row.period_end_date is not None],
                key=lambda x: x.period_end_date,
            )

            for idx in range(3, len(ordered)):
                window = ordered[idx - 3: idx + 1]
                latest = window[-1]

                ttm_rows.append(
                    FundamentalSnapshot(
                        company_id=company_id,
                        cik=latest.cik,
                        period_type="TTM",
                        period_end_date=latest.period_end_date,
                        available_at=latest.available_at,
                        revenue=self._sum_attr(window, "revenue"),
                        net_income=self._sum_attr(window, "net_income"),
                        assets=latest.assets,
                        liabilities=latest.liabilities,
                        equity=latest.equity,
                        operating_cash_flow=self._sum_attr(window, "operating_cash_flow"),
                        shares_outstanding=latest.shares_outstanding,
                        source_name=latest.source_name,
                        created_at=datetime.utcnow(),
                    )
                )

        return ttm_rows

    def _sum_attr(self, rows: list[FundamentalSnapshot], attr: str) -> float | None:
        values = [getattr(row, attr) for row in rows if getattr(row, attr) is not None]
        if not values:
            return None
        return float(sum(values))
