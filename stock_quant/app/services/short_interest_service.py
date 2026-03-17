from __future__ import annotations

"""
Canonical FINRA short-interest service.

Responsabilités :
- transformer les raw records FINRA en records normalisés
- appliquer la policy marché si demandée
- calculer les champs dérivés métier
- préparer aussi les lignes de métadonnées source

Important :
- aucun accès SQL ici
- aucun side-effect DB ici
- logique métier uniquement
"""

from datetime import datetime
from typing import Iterable

from stock_quant.domain.entities.short_interest import (
    RawShortInterestRecord,
    ShortInterestRecord,
    ShortInterestSourceFile,
)
from stock_quant.domain.policies.finra_market_selection_policy import (
    FinraMarketSelectionPolicy,
)


class ShortInterestService:
    """
    Service canonique short interest.

    Ce service garde une structure simple pour rester maintenable :
    - input  : RawShortInterestRecord
    - output : ShortInterestRecord + ShortInterestSourceFile
    """

    def __init__(
        self,
        repository=None,
        market_policy: FinraMarketSelectionPolicy | None = None,
    ) -> None:
        self.repository = repository
        self.market_policy = market_policy or FinraMarketSelectionPolicy()

    def build_history_entries_from_raw(
        self,
        raw_records: Iterable[RawShortInterestRecord],
        *,
        source_market: str = "both",
    ) -> tuple[list[ShortInterestRecord], list[ShortInterestSourceFile], dict[str, int]]:
        """
        Transforme les raw records en records normalisés.

        Notes PIT / quant research :
        - on préserve settlement_date tel quel
        - aucune projection vers un universe courant ici
        - aucune suppression d'historique ici
        """
        rows = list(raw_records)

        history_entries: list[ShortInterestRecord] = []
        source_file_counts: dict[tuple[str, str, object], int] = {}

        metrics = {
            "raw_record_count": len(rows),
            "accepted_records": 0,
            "accepted_source_files": 0,
            "skipped_market_mismatch": 0,
            "skipped_missing_symbol": 0,
            "skipped_missing_settlement_date": 0,
            "skipped_missing_source_file": 0,
        }

        for row in rows:
            normalized_symbol = str(row.symbol or "").strip().upper()
            normalized_source_file = str(row.source_file or "").strip()
            normalized_source_market = str(row.source_market or "unknown").strip().lower()

            if not normalized_symbol:
                metrics["skipped_missing_symbol"] += 1
                continue

            if row.settlement_date is None:
                metrics["skipped_missing_settlement_date"] += 1
                continue

            if not normalized_source_file:
                metrics["skipped_missing_source_file"] += 1
                continue

            if source_market != "both" and not self._matches_market(
                record_market=normalized_source_market,
                expected_market=source_market,
            ):
                metrics["skipped_market_mismatch"] += 1
                continue

            short_interest = int(row.short_interest or 0)
            previous_short_interest = int(row.previous_short_interest or 0)
            avg_daily_volume = float(row.avg_daily_volume or 0.0)

            shares_float = None
            if row.shares_float is not None:
                try:
                    shares_float = int(row.shares_float)
                except Exception:
                    shares_float = None

            days_to_cover = None
            if avg_daily_volume > 0:
                days_to_cover = short_interest / avg_daily_volume

            short_interest_pct_float = None
            if shares_float is not None and shares_float > 0:
                short_interest_pct_float = short_interest / shares_float

            history_entries.append(
                ShortInterestRecord(
                    symbol=normalized_symbol,
                    settlement_date=row.settlement_date,
                    short_interest=short_interest,
                    previous_short_interest=previous_short_interest,
                    avg_daily_volume=avg_daily_volume,
                    days_to_cover=days_to_cover,
                    shares_float=shares_float,
                    short_interest_pct_float=short_interest_pct_float,
                    revision_flag=row.revision_flag,
                    source_market=normalized_source_market,
                    source_file=normalized_source_file,
                    ingested_at=datetime.utcnow(),
                )
            )

            source_key = (
                normalized_source_file,
                normalized_source_market,
                row.source_date,
            )
            source_file_counts[source_key] = source_file_counts.get(source_key, 0) + 1

            metrics["accepted_records"] += 1

        source_entries = [
            ShortInterestSourceFile(
                source_file=source_file,
                source_market=source_market_value,
                source_date=source_date,
                row_count=row_count,
                loaded_at=datetime.utcnow(),
            )
            for (source_file, source_market_value, source_date), row_count
            in sorted(source_file_counts.items())
        ]

        metrics["accepted_source_files"] = len(source_entries)

        return history_entries, source_entries, metrics

    def _matches_market(self, record_market: str | None, expected_market: str) -> bool:
        record = (record_market or "").strip().lower()
        expected = (expected_market or "").strip().lower()

        if expected == "both":
            return True

        return record == expected
