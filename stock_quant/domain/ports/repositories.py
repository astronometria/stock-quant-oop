from __future__ import annotations

from abc import ABC, abstractmethod

from stock_quant.domain.entities.news import NewsArticleRaw, NewsSymbolCandidate, RawNewsSourceRecord
from stock_quant.domain.entities.prices import PriceBar, RawPriceBar
from stock_quant.domain.entities.short_interest import RawShortInterestRecord, ShortInterestRecord, ShortInterestSourceFile
from stock_quant.domain.entities.symbol_reference import SymbolReferenceEntry
from stock_quant.domain.entities.universe import RawUniverseCandidate, UniverseConflict, UniverseEntry


class UniverseRepositoryPort(ABC):
    @abstractmethod
    def replace_universe(self, entries: list[UniverseEntry]) -> int:
        raise NotImplementedError

    @abstractmethod
    def replace_conflicts(self, conflicts: list[UniverseConflict]) -> int:
        raise NotImplementedError

    @abstractmethod
    def fetch_all_universe_entries(self) -> list[UniverseEntry]:
        raise NotImplementedError

    @abstractmethod
    def load_raw_candidates(self) -> list[RawUniverseCandidate]:
        raise NotImplementedError


class SymbolReferenceRepositoryPort(ABC):
    @abstractmethod
    def replace_symbol_reference(self, entries: list[SymbolReferenceEntry]) -> int:
        raise NotImplementedError

    @abstractmethod
    def load_included_universe_entries(self) -> list[UniverseEntry]:
        raise NotImplementedError


class PriceRepositoryPort(ABC):
    @abstractmethod
    def load_raw_price_bars(self) -> list[RawPriceBar]:
        raise NotImplementedError

    @abstractmethod
    def load_included_symbols(self) -> set[str]:
        raise NotImplementedError

    @abstractmethod
    def replace_price_history(self, entries: list[PriceBar]) -> int:
        raise NotImplementedError

    @abstractmethod
    def rebuild_price_latest(self) -> int:
        raise NotImplementedError


class ShortInterestRepositoryPort(ABC):
    @abstractmethod
    def load_raw_short_interest_records(self) -> list[RawShortInterestRecord]:
        raise NotImplementedError

    @abstractmethod
    def load_included_symbols(self) -> set[str]:
        raise NotImplementedError

    @abstractmethod
    def replace_short_interest_history(self, entries: list[ShortInterestRecord]) -> int:
        raise NotImplementedError

    @abstractmethod
    def rebuild_short_interest_latest(self) -> int:
        raise NotImplementedError

    @abstractmethod
    def replace_short_interest_sources(self, entries: list[ShortInterestSourceFile]) -> int:
        raise NotImplementedError


class NewsRepositoryPort(ABC):
    @abstractmethod
    def load_raw_news_source_records(self) -> list[RawNewsSourceRecord]:
        raise NotImplementedError

    @abstractmethod
    def replace_news_articles_raw(self, entries: list[NewsArticleRaw]) -> int:
        raise NotImplementedError

    @abstractmethod
    def load_news_articles_raw(self) -> list[NewsArticleRaw]:
        raise NotImplementedError

    @abstractmethod
    def load_symbol_reference_entries(self) -> list[SymbolReferenceEntry]:
        raise NotImplementedError

    @abstractmethod
    def replace_news_symbol_candidates(self, entries: list[NewsSymbolCandidate]) -> int:
        raise NotImplementedError
