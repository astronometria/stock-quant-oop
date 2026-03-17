from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from typing import Optional


@dataclass
class PriceRefreshWindowResult:
    effective_start_date: Optional[date]
    effective_end_date: Optional[date]

    gap_days: int
    is_noop: bool
    requires_range_fetch: bool

    window_reason: str
    catchup_capped: bool

    # debug / audit
    last_any_date: Optional[date]
    last_complete_date: Optional[date]
    expected_count: int
    observed_latest_count: int


class PriceRefreshWindowService:
    def __init__(self, repo, *, catchup_max_days: int = 30):
        self._repo = repo
        self._catchup_max_days = catchup_max_days

    def resolve_window(
        self,
        *,
        requested_start_date: Optional[date],
        requested_end_date: Optional[date],
        as_of: Optional[date],
        lookback_days: Optional[int],
        symbols: Optional[list[str]],
        today: date,
    ) -> PriceRefreshWindowResult:

        # ------------------------------------------------------------------
        # explicit user
        # ------------------------------------------------------------------
        if as_of:
            return PriceRefreshWindowResult(
                as_of, as_of, 1, False, False,
                "explicit_as_of", False,
                None, None, 0, 0
            )

        if requested_start_date or requested_end_date:
            return PriceRefreshWindowResult(
                requested_start_date, requested_end_date, 1, False, False,
                "explicit_dates", False,
                None, None, 0, 0
            )

        # ------------------------------------------------------------------
        # coverage-aware probe
        # ------------------------------------------------------------------
        last_complete, last_any, expected, observed = \
            self._repo.get_latest_complete_price_date(symbols)

        effective_end = today

        # ------------------------------------------------------------------
        # bootstrap
        # ------------------------------------------------------------------
        if last_complete is None:
            if not lookback_days:
                return PriceRefreshWindowResult(
                    None, None, 0, True, False,
                    "no_history", False,
                    last_any, last_complete, expected, observed
                )

            start = today - timedelta(days=lookback_days - 1)

            return PriceRefreshWindowResult(
                start, today, lookback_days, False, True,
                "bootstrap", False,
                last_any, last_complete, expected, observed
            )

        # ------------------------------------------------------------------
        # detect partial latest
        # ------------------------------------------------------------------
        if last_any and last_any > last_complete:
            candidate_start = last_complete + timedelta(days=1)
            gap_days = (effective_end - candidate_start).days + 1

            return self._finalize(
                candidate_start,
                effective_end,
                gap_days,
                "partial_latest_detected",
                last_any,
                last_complete,
                expected,
                observed
            )

        # ------------------------------------------------------------------
        # normal incremental
        # ------------------------------------------------------------------
        candidate_start = last_complete + timedelta(days=1)

        if candidate_start > effective_end:
            return PriceRefreshWindowResult(
                last_complete,
                last_complete,
                0,
                True,
                False,
                "already_up_to_date",
                False,
                last_any,
                last_complete,
                expected,
                observed
            )

        gap_days = (effective_end - candidate_start).days + 1

        return self._finalize(
            candidate_start,
            effective_end,
            gap_days,
            "incremental_catchup",
            last_any,
            last_complete,
            expected,
            observed
        )

    # ------------------------------------------------------------------
    # finalize (cap + flags)
    # ------------------------------------------------------------------
    def _finalize(
        self,
        start: date,
        end: date,
        gap_days: int,
        reason: str,
        last_any,
        last_complete,
        expected,
        observed,
    ):
        capped = False

        if gap_days > self._catchup_max_days:
            start = end - timedelta(days=self._catchup_max_days - 1)
            gap_days = self._catchup_max_days
            capped = True

        return PriceRefreshWindowResult(
            start,
            end,
            gap_days,
            False,
            gap_days > 1,
            reason,
            capped,
            last_any,
            last_complete,
            expected,
            observed
        )
