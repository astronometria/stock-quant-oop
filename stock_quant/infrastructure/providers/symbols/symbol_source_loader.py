from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from stock_quant.domain.ports.providers import SymbolSourcePort


class SymbolSourceLoader(SymbolSourcePort):
    def fetch_symbols(self) -> Iterable[Any]:
        raise NotImplementedError("À brancher en passe 4.")
