from __future__ import annotations

from typing import Any

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
    Thin application service for the canonical SQL-first FINRA pipeline.

    Main rule:
    - keep orchestration here
    - keep heavy row processing in DuckDB SQL inside the repository

    Compatibility:
    - legacy methods are retained where useful
    - canonical path should use:
        * get_build_state()
        * load_raw()
        * build_history()
        * refresh_latest()
    """

    def __init__(
        self,
        repository=None,
        market_policy: FinraMarketSelectionPolicy | None = None,
    ) -> None:
        self.repository = repository
        self.market_policy = market_policy or FinraMarketSelectionPolicy()
        self._loaded_raw_rows: list[dict[str, Any]] | None = None

    # -------------------------------------------------------------------------
    # Canonical SQL-first path
    # -------------------------------------------------------------------------

    def get_build_state(self):
        if self.repository is None:
            raise RuntimeError("repository is required for get_build_state()")
        return self.repository.get_build_state()

    def load_raw(self) -> int:
        """
        Lightweight stage-count/load probe.

        In the SQL-first path we do not transform these rows in Python. This call
        remains useful for metrics / contract stability and to preserve a familiar
        pipeline step boundary.
        """
        if self.repository is None:
            raise RuntimeError("repository is required for load_raw()")

        self._loaded_raw_rows = list(self.repository.load_raw())
        return len(self._loaded_raw_rows)

    def build_history(self) -> dict[str, int]:
        """
        Canonical history build: raw -> history in SQL only.
        """
        if self.repository is None:
            raise RuntimeError("repository is required for build_history()")
        return self.repository.insert_history_from_raw_sql_first()

    def refresh_latest(self) -> dict[str, int]:
        """
        Canonical latest rebuild: history -> latest in SQL only.
        """
        if self.repository is None:
            raise RuntimeError("repository is required for refresh_latest()")
        return self.repository.rebuild_latest_from_history_sql_first()

    # -------------------------------------------------------------------------
    # Legacy compatibility path kept for older tests
    # -------------------------------------------------------------------------

    def build(
        self,
        raw_records: list[RawShortInterestRecord],
        *,
        allowed_symbols: set[str] | None,
        source_market: str,
    ) -> tuple[list[ShortInterestRecord], list[ShortInterestSourceFile], dict[str, int]]:
        """
        Backward-compatible object path.

        Not used by the canonical SQL-first pipeline, but kept because some tests
        may still rely on it.
        """
        allowed = {str(x).strip().upper() for x in (allowed_symbols or set())}

        history: list[ShortInterestRecord] = []
        source_counts: dict[tuple[str, str, object], int] = {}

        metrics = {
            "accepted_records": 0,
            "accepted_source_files": 0,
            "skipped_market_mismatch": 0,
            "skipped_not_in_universe": 0,
        }

        for row in raw_records:
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

            days_to_cover = None
            if avg_daily_volume > 0:
                days_to_cover = short_interest / avg_daily_volume

            shares_float = int(row.shares_float) if row.shares_float is not None else None

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
                    ingested_at=row.source_date if hasattr(row, "source_date") else None,
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
                loaded_at=None,
            )
            for (source_file, source_market_value, source_date), row_count in sorted(source_counts.items())
        ]
        metrics["accepted_source_files"] = len(sources)

        return history, sources, metrics

    # -------------------------------------------------------------------------
    # Small helpers
    # -------------------------------------------------------------------------

    def _matches_market(self, record_market: str | None, expected_market: str) -> bool:
        record = (record_market or "").strip().lower()
        expected = (expected_market or "").strip().lower()

        if expected == "both":
            return True
        return record == expected
