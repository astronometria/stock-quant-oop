from __future__ import annotations


class ExchangeNormalizationPolicy:
    _MAP = {
        "NASDAQ": "NASDAQ",
        "NASDAQGS": "NASDAQ",
        "NASDAQGM": "NASDAQ",
        "NASDAQCM": "NASDAQ",
        "XNAS": "NASDAQ",
        "NYSE": "NYSE",
        "XNYS": "NYSE",
        "NYSEARCA": "NYSE_ARCA",
        "ARCA": "NYSE_ARCA",
        "NYSE AMERICAN": "NYSE_AMERICAN",
        "NYSEAMERICAN": "NYSE_AMERICAN",
        "AMEX": "NYSE_AMERICAN",
        "BATS": "BATS",
        "CBOE": "BATS",
        "IEX": "IEX",
        "OTC": "OTC",
        "OTCMKTS": "OTC",
        "OTCQX": "OTC",
        "OTCQB": "OTC",
        "PINK": "OTC",
    }

    def normalize(self, raw_exchange: str | None) -> str | None:
        if raw_exchange is None:
            return None
        cleaned = " ".join(raw_exchange.strip().upper().replace("-", " ").split())
        if not cleaned:
            return None
        return self._MAP.get(cleaned, cleaned)
