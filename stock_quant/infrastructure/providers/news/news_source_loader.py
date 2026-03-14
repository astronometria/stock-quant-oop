from __future__ import annotations

from collections.abc import Iterable
from datetime import date
from typing import Any

from stock_quant.domain.ports.providers import NewsSourcePort


class NewsSourceLoader(NewsSourcePort):
    def fetch_news(
        self,
        start_date: date | None = None,
        end_date: date | None = None,
        limit: int | None = None,
    ) -> Iterable[Any]:
        raise NotImplementedError("À brancher en passe 4.")
