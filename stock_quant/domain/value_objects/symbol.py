from __future__ import annotations

import re
from dataclasses import dataclass

from stock_quant.shared.exceptions import ValidationError


_VALID_SYMBOL_RE = re.compile(r"^[A-Z0-9.\-]{1,15}$")


@dataclass(frozen=True, slots=True)
class Symbol:
    value: str

    @classmethod
    def from_raw(cls, raw: str | None) -> "Symbol":
        if raw is None:
            raise ValidationError("symbol is required")
        value = raw.strip().upper()
        if not value:
            raise ValidationError("symbol is empty")
        if not _VALID_SYMBOL_RE.fullmatch(value):
            raise ValidationError(f"invalid symbol format: {raw!r}")
        return cls(value=value)
