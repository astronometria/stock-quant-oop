from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(slots=True)
class SymbolReferenceEntry:
    symbol: str
    cik: str | None
    company_name: str
    company_name_clean: str
    aliases_json: str
    exchange: str | None
    source_name: str
    symbol_match_enabled: bool
    name_match_enabled: bool
    created_at: datetime
