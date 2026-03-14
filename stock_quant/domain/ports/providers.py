from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Iterable
from datetime import date
from typing import Any


class PriceSourcePort(ABC):
    @abstractmethod
    def fetch_daily_prices(self, symbols: list[str], as_of: date | None = None) -> Iterable[Any]:
        raise NotImplementedError

    @abstractmethod
    def fetch_history(
        self,
        symbols: list[str],
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> Iterable[Any]:
        raise NotImplementedError


class ShortInterestSourcePort(ABC):
    @abstractmethod
    def fetch_short_interest(
        self,
        symbols: list[str] | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> Iterable[Any]:
        raise NotImplementedError


class NewsSourcePort(ABC):
    @abstractmethod
    def fetch_news(
        self,
        start_date: date | None = None,
        end_date: date | None = None,
        limit: int | None = None,
    ) -> Iterable[Any]:
        raise NotImplementedError


class SymbolSourcePort(ABC):
    @abstractmethod
    def fetch_symbols(self) -> Iterable[Any]:
        raise NotImplementedError
