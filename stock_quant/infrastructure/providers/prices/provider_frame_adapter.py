from __future__ import annotations

"""
ProviderFrameAdapter

Responsabilité :
- router vers :
    - single-day fetch (as_of)
    - range fetch (start/end)

IMPORTANT :
- ne PAS écraser une plage en as_of
"""

from datetime import date


class ProviderFrameAdapter:
    def __init__(self, provider):
        self._provider = provider

    def fetch_prices(
        self,
        *,
        symbols: list[str],
        start_date: date,
        end_date: date,
        requires_range_fetch: bool,
    ):

        # ------------------------------------------------------------------
        # SINGLE DAY
        # ------------------------------------------------------------------
        if not requires_range_fetch:
            return self._provider.fetch_daily_prices_frame(
                symbols=symbols,
                as_of=start_date,
            )

        # ------------------------------------------------------------------
        # RANGE
        # ------------------------------------------------------------------
        return self._provider.fetch_daily_prices_range_frame(
            symbols=symbols,
            start_date=start_date,
            end_date=end_date,
        )
