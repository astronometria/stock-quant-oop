from __future__ import annotations

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
    Backward-compatible short-interest service.

    Supports:
    - legacy unit tests via build(...)
    - newer repository-driven flows via load_raw/build_history/refresh_latest
    """

    def __init__(self, repository=None, market_policy: FinraMarketSelectionPolicy | None = None):
        self.repository = repository
        self.market_policy = market_policy or FinraMarketSelectionPolicy()

    def build(
        self,
        raw_records: Iterable[RawShortInterestRecord],
        *,
        allowed_symbols: set[str] | None,
        source_market: str,
    ) -> tuple[list[ShortInterestRecord], list[ShortInterestSourceFile], dict[str, int]]:
        rows = list(raw_records)
        allowed = {str(x).strip().upper() for x in (allowed_symbols or set())}

        history: list[ShortInterestRecord] = []
        source_counts: dict[tuple[str, str, object], int] = {}
        metrics = {
            "accepted_records": 0,
            "accepted_source_files": 0,
            "skipped_market_mismatch": 0,
            "skipped_not_in_universe": 0,
        }

        for row in rows:
            symbol = str(row.symbol).strip().upper()

            if source_market != "both" and not self._matches_market(row.source_market, source_market):
                metrics["skipped_market_mismatch"] += 1
                continue

            if allowed and symbol not in allowed:
                metrics["skipped_not_in_universe"] += 1
                continue

            short_interest = int(row.short_interest or 0)
            previous_short_interest = int(row.previous_short_interest or 0)
            avg_daily_volume = float(row.avg_daily_volume or 0.0)
            shares_float = int(row.shares_float) if row.shares_float is not None else None

            days_to_cover = None
            if avg_daily_volume > 0:
                days_to_cover = short_interest / avg_daily_volume

            short_interest_pct_float = None
            if shares_float and shares_float > 0:
                short_interest_pct_float = short_interest / shares_float

            history.append(
                ShortInterestRecord(
                    symbol=symbol,
                    settlement_date=row.settlement_date,
                    short_interest=short_interest,
                    previous_short_interest=previous_short_interest,
                    avg_daily_volume=avg_daily_volume,
                    days_to_cover=days_to_cover,
                    shares_float=shares_float,
                    short_interest_pct_float=short_interest_pct_float,
                    revision_flag=row.revision_flag,
                    source_market=row.source_market,
                    source_file=row.source_file,
                    ingested_at=datetime.utcnow(),
                )
            )

            key = (row.source_file, row.source_market, row.source_date)
            source_counts[key] = source_counts.get(key, 0) + 1
            metrics["accepted_records"] += 1

        sources = [
            ShortInterestSourceFile(
                source_file=source_file,
                source_market=source_market_value,
                source_date=source_date,
                row_count=row_count,
                loaded_at=datetime.utcnow(),
            )
            for (source_file, source_market_value, source_date), row_count in sorted(source_counts.items())
        ]
        metrics["accepted_source_files"] = len(sources)

        return history, sources, metrics

    def load_raw(self):
        if self.repository is None:
            raise RuntimeError("repository is required for load_raw()")
        rows = self.repository.load_raw()
        return len(rows)

    def build_history(self):
        if self.repository is None:
            raise RuntimeError("repository is required for build_history()")
        raw = self.repository.load_raw()
        normalized = []

        for r in raw:
            normalized.append(
                {
                    "symbol": r["symbol"],
                    "as_of_date": r["as_of_date"],
                    "short_interest": r["short_interest"],
                    "short_volume": r.get("short_volume"),
                    "available_at": r["available_at"],
                }
            )

        return self.repository.insert_history(normalized)

    def refresh_latest(self):
        if self.repository is None:
            raise RuntimeError("repository is required for refresh_latest()")
        return self.repository.rebuild_latest()

    def _matches_market(self, record_market: str | None, expected_market: str) -> bool:
        record = (record_market or "").strip().lower()
        expected = (expected_market or "").strip().lower()

        if expected == "both":
            return True
        return record == expected
