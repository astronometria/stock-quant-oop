from __future__ import annotations

# =============================================================================
# Fundamentals service
# -----------------------------------------------------------------------------
# Ajustements importants dans cette version:
# - réduction du risque de look-ahead / PIT bias
# - sélection déterministe des faits quand plusieurs filings existent
# - as_of_date des features = date de disponibilité réelle (available_at)
#   et non plus period_end_date
# - TTM disponible seulement quand la dernière fenêtre trimestrielle est
#   réellement disponible
# =============================================================================

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
        """
        Construit:
        - quarterly snapshots
        - annual snapshots
        - TTM snapshots

        Règles anti-bias importantes:
        - available_at est pris depuis le filing
        - si plusieurs faits existent pour la même compagnie / période / concept,
          on garde la version la plus récente de manière déterministe
        """
        filing_map: dict[str, dict[str, Any]] = {
            str(row.get("filing_id")): row for row in sec_filing_rows if row.get("filing_id")
        }

        # ---------------------------------------------------------------------
        # On garde le "meilleur" fait pour chaque clé conceptuelle:
        # (company_id, period_type, period_end_date, concept)
        #
        # Priorité:
        # 1) available_at la plus récente
        # 2) accepted_at la plus récente
        # 3) filing_date la plus récente
        #
        # Cela évite qu'un amendement ou une version plus récente soit écrasé
        # aléatoirement par l'ordre de lecture.
        # ---------------------------------------------------------------------
        best_fact_by_key: dict[tuple[Any, ...], dict[str, Any]] = {}

        skipped_missing_filing = 0
        skipped_missing_identity = 0
        skipped_unknown_concept = 0

        for row in sec_fact_rows:
            filing_id = str(row.get("filing_id", "")).strip()
            if not filing_id:
                skipped_missing_filing += 1
                continue

            filing = filing_map.get(filing_id)
            if filing is None:
                skipped_missing_filing += 1
                continue

            company_id = filing.get("company_id")
            cik = str(filing.get("cik", "")).strip()
            form_type = str(filing.get("form_type", "")).strip().upper()
            filing_date = filing.get("filing_date")
            accepted_at = filing.get("accepted_at")
            available_at = filing.get("available_at")
            period_end_date = row.get("period_end_date")

            if not company_id or not cik or period_end_date is None:
                skipped_missing_identity += 1
                continue

            concept = str(row.get("concept", "")).strip().upper()
            target_field = NUMERIC_CONCEPT_MAP.get(concept)
            if target_field is None:
                skipped_unknown_concept += 1
                continue

            period_type = self._infer_period_type(form_type)

            candidate = {
                "company_id": company_id,
                "cik": cik,
                "period_type": period_type,
                "period_end_date": period_end_date,
                "concept": concept,
                "target_field": target_field,
                "value_numeric": row.get("value_numeric"),
                "source_name": row.get("source_name") or "sec",
                "available_at": available_at,
                "accepted_at": accepted_at,
                "filing_date": filing_date,
            }

            key = (company_id, period_type, period_end_date, concept)
            current = best_fact_by_key.get(key)

            if current is None or self._is_better_candidate(candidate, current):
                best_fact_by_key[key] = candidate

        # ---------------------------------------------------------------------
        # Maintenant on regroupe les meilleurs faits conceptuels par snapshot.
        # ---------------------------------------------------------------------
        grouped: dict[tuple[str, str, object], dict[str, Any]] = defaultdict(dict)

        for payload in best_fact_by_key.values():
            key = (
                payload["company_id"],
                payload["period_type"],
                payload["period_end_date"],
            )

            grouped[key]["company_id"] = payload["company_id"]
            grouped[key]["cik"] = payload["cik"]
            grouped[key]["period_type"] = payload["period_type"]
            grouped[key]["period_end_date"] = payload["period_end_date"]
            grouped[key]["source_name"] = payload["source_name"]

            # available_at du snapshot = max available_at des faits retenus
            current_available_at = grouped[key].get("available_at")
            payload_available_at = payload.get("available_at")
            if current_available_at is None:
                grouped[key]["available_at"] = payload_available_at
            elif payload_available_at is not None and payload_available_at > current_available_at:
                grouped[key]["available_at"] = payload_available_at

            grouped[key][payload["target_field"]] = payload.get("value_numeric")

        quarterly: list[FundamentalSnapshot] = []
        annual: list[FundamentalSnapshot] = []

        for payload in grouped.values():
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
            "skipped_missing_filing": skipped_missing_filing,
            "skipped_missing_identity": skipped_missing_identity,
            "skipped_unknown_concept": skipped_unknown_concept,
        }
        return quarterly, annual, ttm, metrics

    def build_features_daily(
        self,
        quarterly_rows: list[FundamentalSnapshot],
        annual_rows: list[FundamentalSnapshot],
        ttm_rows: list[FundamentalSnapshot],
    ) -> tuple[list[FundamentalFeatureDaily], dict[str, int]]:
        """
        Construit les features daily fondamentales.

        Correction PIT importante:
        - as_of_date = date(available_at) quand disponible
        - on ne doit pas rendre une feature disponible à period_end_date
          car cela introduit un look-ahead bias
        """
        features: list[FundamentalFeatureDaily] = []

        for row in quarterly_rows + annual_rows + ttm_rows:
            if row.available_at is not None:
                as_of_date = row.available_at.date()
            else:
                # fallback de sécurité; à surveiller dans les metrics/probes
                as_of_date = row.period_end_date

            if as_of_date is None:
                continue

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
                    as_of_date=as_of_date,
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
        """
        Déduit le type de période à partir du type de formulaire SEC.

        On garde une logique simple et déterministe:
        - 10-K / 20-F / 40-F -> ANNUAL
        - le reste -> QUARTERLY

        Cela évite de surinterpréter des cas ambigus à ce stade.
        """
        if form_type in {"10-K", "20-F", "40-F"}:
            return "ANNUAL"
        return "QUARTERLY"

    def _build_ttm(self, quarterly_rows: list[FundamentalSnapshot]) -> list[FundamentalSnapshot]:
        """
        Construit les snapshots TTM à partir de fenêtres glissantes de 4 trimestres.

        Hypothèses anti-bias:
        - une ligne TTM n'existe que si les 4 trimestres de la fenêtre existent
        - available_at du TTM = max(available_at) de la fenêtre
          car la feature TTM n'est exploitable qu'une fois le dernier trimestre
          réellement disponible
        - pour les métriques de stock (assets, liabilities, equity, shares),
          on prend le snapshot le plus récent de la fenêtre
        """
        by_company: dict[str, list[FundamentalSnapshot]] = defaultdict(list)
        for row in quarterly_rows:
            if row.period_end_date is not None:
                by_company[row.company_id].append(row)

        ttm_rows: list[FundamentalSnapshot] = []

        for company_id, rows in by_company.items():
            ordered = sorted(rows, key=lambda x: x.period_end_date)

            for idx in range(3, len(ordered)):
                window = ordered[idx - 3 : idx + 1]
                latest = window[-1]

                # -------------------------------------------------------------
                # available_at du TTM = date réelle de disponibilité de la
                # fenêtre entière, donc max available_at connue dans la fenêtre.
                # Si aucune available_at n'est présente, on laisse None et
                # build_features_daily tombera sur le fallback conservateur.
                # -------------------------------------------------------------
                available_candidates = [row.available_at for row in window if row.available_at is not None]
                ttm_available_at = max(available_candidates) if available_candidates else None

                ttm_rows.append(
                    FundamentalSnapshot(
                        company_id=company_id,
                        cik=latest.cik,
                        period_type="TTM",
                        period_end_date=latest.period_end_date,
                        available_at=ttm_available_at,
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
        """
        Somme un attribut sur une fenêtre.

        Note:
        - retourne None si aucune valeur n'est présente
        - sinon retourne la somme des valeurs disponibles
        """
        values = [getattr(row, attr) for row in rows if getattr(row, attr) is not None]
        if not values:
            return None
        return float(sum(values))

    def _is_better_candidate(self, candidate: dict[str, Any], current: dict[str, Any]) -> bool:
        """
        Compare deux faits candidats pour la même clé conceptuelle.

        Ordre de priorité déterministe:
        1) available_at la plus récente
        2) accepted_at la plus récente
        3) filing_date la plus récente

        En cas d'égalité complète, on retourne False pour conserver le courant
        et garder un comportement stable.
        """
        candidate_rank = (
            self._sort_key_datetime(candidate.get("available_at")),
            self._sort_key_datetime(candidate.get("accepted_at")),
            self._sort_key_datetime(candidate.get("filing_date")),
        )
        current_rank = (
            self._sort_key_datetime(current.get("available_at")),
            self._sort_key_datetime(current.get("accepted_at")),
            self._sort_key_datetime(current.get("filing_date")),
        )
        return candidate_rank > current_rank

    def _sort_key_datetime(self, value: Any) -> tuple[int, Any]:
        """
        Transforme une valeur de date/datetime potentiellement None en clé triable.

        Convention:
        - None < toute date réelle
        - les dates réelles conservent leur ordre naturel
        """
        if value is None:
            return (0, datetime.min)
        return (1, value)
