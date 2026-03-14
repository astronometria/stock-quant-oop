from __future__ import annotations

from collections.abc import Iterable
from datetime import date
from typing import Any

from stock_quant.domain.ports.providers import ShortInterestSourcePort


class FinraProvider(ShortInterestSourcePort):
    def fetch_short_interest(
        self,
        symbols: list[str] | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> Iterable[Any]:
        raise NotImplementedError("À brancher en passe 4.")
