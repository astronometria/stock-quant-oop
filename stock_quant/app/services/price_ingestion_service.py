from __future__ import annotations

from datetime import datetime
from typing import Iterable

from stock_quant.domain.entities.prices import PriceBar, RawPriceBar


class PriceIngestionService:
    def build(
        self,
        raw_bars: Iterable[RawPriceBar],
        *,
        allowed_symbols: set[str],
    ) -> tuple[list[PriceBar], dict[str, int]]:
        results: list[PriceBar] = []
        skipped_not_in_universe = 0
        skipped_invalid = 0

        for raw in raw_bars:
            if raw.symbol not in allowed_symbols:
                skipped_not_in_universe += 1
                continue

            if raw.price_date is None or raw.close is None:
                skipped_invalid += 1
                continue

            open_price = float(raw.open if raw.open is not None else raw.close)
            high_price = float(raw.high if raw.high is not None else raw.close)
            low_price = float(raw.low if raw.low is not None else raw.close)
            close_price = float(raw.close)
            volume = int(raw.volume if raw.volume is not None else 0)

            if high_price < low_price:
                skipped_invalid += 1
                continue

            if any(x < 0 for x in [open_price, high_price, low_price, close_price, volume]):
                skipped_invalid += 1
                continue

            results.append(
                PriceBar(
                    symbol=raw.symbol,
                    price_date=raw.price_date,
                    open=open_price,
                    high=high_price,
                    low=low_price,
                    close=close_price,
                    volume=volume,
                    source_name=raw.source_name,
                    ingested_at=datetime.utcnow(),
                )
            )

        metrics = {
            "raw_bars": len(raw_bars) if isinstance(raw_bars, list) else len(list(raw_bars)),
            "accepted_bars": len(results),
            "skipped_not_in_universe": skipped_not_in_universe,
            "skipped_invalid": skipped_invalid,
            "allowed_symbols": len(allowed_symbols),
        }
        return results, metrics
