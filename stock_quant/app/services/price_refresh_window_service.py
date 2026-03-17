from __future__ import annotations

"""
PriceRefreshWindowService

Responsabilité :
- Déterminer la fenêtre effective de refresh des prix en mode incremental
- Gérer :
    - catch-up multi-jours
    - borne max (anti backfill implicite)
    - noop propre
    - distinction single-day vs range

IMPORTANT (quant research) :
- utilise UNIQUEMENT price_history (canonique)
- ne touche jamais price_latest (serving)
- pas de survivorship bias implicite
"""

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


class PriceRefreshWindowService:
    def __init__(
        self,
        price_repository,
        *,
        catchup_max_days: int = 30,
    ):
        self._repo = price_repository
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
        # 1) Cas explicite utilisateur
        # ------------------------------------------------------------------
        if as_of is not None:
            return PriceRefreshWindowResult(
                effective_start_date=as_of,
                effective_end_date=as_of,
                gap_days=1,
                is_noop=False,
                requires_range_fetch=False,
                window_reason="explicit_as_of",
                catchup_capped=False,
            )

        if requested_start_date or requested_end_date:
            return PriceRefreshWindowResult(
                effective_start_date=requested_start_date,
                effective_end_date=requested_end_date,
                gap_days=1,
                is_noop=False,
                requires_range_fetch=False,
                window_reason="explicit_dates",
                catchup_capped=False,
            )

        # ------------------------------------------------------------------
        # 2) Mode incremental auto
        # ------------------------------------------------------------------
        last_date = self._repo.get_max_price_date(symbols=symbols)

        effective_end = today

        # Aucun historique → bootstrap borné
        if last_date is None:
            if not lookback_days:
                return PriceRefreshWindowResult(
                    None,
                    None,
                    0,
                    True,
                    False,
                    "no_history_no_lookback",
                    False,
                )

            start = today - timedelta(days=lookback_days - 1)

            return PriceRefreshWindowResult(
                effective_start_date=start,
                effective_end_date=today,
                gap_days=lookback_days,
                is_noop=False,
                requires_range_fetch=True,
                window_reason="bootstrap_lookback",
                catchup_capped=False,
            )

        # ------------------------------------------------------------------
        # 3) Calcul gap réel
        # ------------------------------------------------------------------
        candidate_start = last_date + timedelta(days=1)

        if candidate_start > effective_end:
            return PriceRefreshWindowResult(
                None,
                None,
                0,
                True,
                False,
                "already_up_to_date",
                False,
            )

        gap_days = (effective_end - candidate_start).days + 1

        # ------------------------------------------------------------------
        # 4) Cap anti backfill implicite
        # ------------------------------------------------------------------
        catchup_capped = False

        if gap_days > self._catchup_max_days:
            candidate_start = effective_end - timedelta(days=self._catchup_max_days - 1)
            gap_days = self._catchup_max_days
            catchup_capped = True

        requires_range = gap_days > 1

        return PriceRefreshWindowResult(
            effective_start_date=candidate_start,
            effective_end_date=effective_end,
            gap_days=gap_days,
            is_noop=False,
            requires_range_fetch=requires_range,
            window_reason="incremental_catchup",
            catchup_capped=catchup_capped,
        )
