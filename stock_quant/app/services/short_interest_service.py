from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from typing import Iterable

from stock_quant.domain.entities.short_interest import RawShortInterestRecord, ShortInterestRecord, ShortInterestSourceFile
from stock_quant.domain.policies.finra_market_selection_policy import FinraMarketSelectionPolicy


class ShortInterestService:
    def __init__(self, market_policy: FinraMarketSelectionPolicy) -> None:
        self.market_policy = market_policy

    def build(
        self,
        raw_records: Iterable[RawShortInterestRecord],
        *,
        allowed_symbols: set[str],
        source_market: str,
    ) -> tuple[list[ShortInterestRecord], list[ShortInterestSourceFile], dict[str, int]]:
        accepted: list[ShortInterestRecord] = []
        skipped_not_in_universe = 0
        skipped_market_mismatch = 0
        skipped_invalid = 0

        grouped_sources: dict[tuple[str, str, object], int] = defaultdict(int)

        for raw in raw_records:
            if not self.market_policy.include_source_market(source_market, raw.source_market):
                skipped_market_mismatch += 1
                continue

            if raw.symbol not in allowed_symbols:
                skipped_not_in_universe += 1
                continue

            if raw.settlement_date is None or raw.short_interest is None:
                skipped_invalid += 1
                continue

            short_interest = int(raw.short_interest)
            previous_short_interest = int(raw.previous_short_interest or 0)
            avg_daily_volume = float(raw.avg_daily_volume or 0.0)
            shares_float = int(raw.shares_float) if raw.shares_float is not None else None

            if short_interest < 0 or previous_short_interest < 0 or avg_daily_volume < 0:
                skipped_invalid += 1
                continue

            days_to_cover = None
            if avg_daily_volume > 0:
                days_to_cover = short_interest / avg_daily_volume

            short_interest_pct_float = None
            if shares_float and shares_float > 0:
                short_interest_pct_float = short_interest / shares_float

            accepted.append(
                ShortInterestRecord(
                    symbol=raw.symbol,
                    settlement_date=raw.settlement_date,
                    short_interest=short_interest,
                    previous_short_interest=previous_short_interest,
                    avg_daily_volume=avg_daily_volume,
                    days_to_cover=days_to_cover,
                    shares_float=shares_float,
                    short_interest_pct_float=short_interest_pct_float,
                    revision_flag=raw.revision_flag,
                    source_market=raw.source_market,
                    source_file=raw.source_file,
                    ingested_at=datetime.utcnow(),
                )
            )

            grouped_sources[(raw.source_file, raw.source_market, raw.source_date)] += 1

        source_files = [
            ShortInterestSourceFile(
                source_file=source_file,
                source_market=row_source_market,
                source_date=source_date,
                row_count=row_count,
                loaded_at=datetime.utcnow(),
            )
            for (source_file, row_source_market, source_date), row_count in grouped_sources.items()
        ]

        metrics = {
            "raw_records": len(raw_records) if isinstance(raw_records, list) else len(list(raw_records)),
            "accepted_records": len(accepted),
            "accepted_source_files": len(source_files),
            "allowed_symbols": len(allowed_symbols),
            "skipped_not_in_universe": skipped_not_in_universe,
            "skipped_market_mismatch": skipped_market_mismatch,
            "skipped_invalid": skipped_invalid,
        }
        return accepted, source_files, metrics
