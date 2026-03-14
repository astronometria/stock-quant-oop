from __future__ import annotations

from datetime import datetime
from typing import Any

from stock_quant.domain.entities.short_data import (
    DailyShortVolumeSnapshot,
    ShortFeatureDaily,
    ShortInterestSnapshot,
)


class ShortDataService:
    def build_short_interest_history(
        self,
        raw_rows: list[dict[str, Any]],
        symbol_map: dict[str, dict[str, str | None]],
    ) -> tuple[list[ShortInterestSnapshot], dict[str, int]]:
        out: list[ShortInterestSnapshot] = []

        for row in raw_rows:
            symbol = str(row.get("symbol", "")).strip().upper()
            settlement_date = row.get("settlement_date")
            if not symbol or settlement_date is None:
                continue

            mapping = symbol_map.get(symbol, {})
            out.append(
                ShortInterestSnapshot(
                    instrument_id=str(mapping.get("instrument_id") or ""),
                    company_id=mapping.get("company_id"),
                    symbol=symbol,
                    settlement_date=settlement_date,
                    short_interest=row.get("short_interest"),
                    previous_short_interest=row.get("previous_short_interest"),
                    avg_daily_volume=row.get("avg_daily_volume"),
                    days_to_cover=row.get("days_to_cover"),
                    source_name=row.get("source_file") or "finra",
                    created_at=datetime.utcnow(),
                )
            )

        out = [row for row in out if row.instrument_id]
        return out, {
            "raw_short_interest_rows": len(raw_rows),
            "short_interest_history_rows": len(out),
        }

    def build_daily_short_volume_history(
        self,
        raw_rows: list[dict[str, Any]],
        symbol_map: dict[str, dict[str, str | None]],
    ) -> tuple[list[DailyShortVolumeSnapshot], dict[str, int]]:
        out: list[DailyShortVolumeSnapshot] = []

        for row in raw_rows:
            symbol = str(row.get("symbol", "")).strip().upper()
            trade_date = row.get("trade_date")
            if not symbol or trade_date is None:
                continue

            mapping = symbol_map.get(symbol, {})
            short_volume = row.get("short_volume")
            total_volume = row.get("total_volume")

            ratio = None
            if total_volume not in (None, 0) and short_volume is not None:
                ratio = short_volume / total_volume

            out.append(
                DailyShortVolumeSnapshot(
                    instrument_id=str(mapping.get("instrument_id") or ""),
                    company_id=mapping.get("company_id"),
                    symbol=symbol,
                    trade_date=trade_date,
                    short_volume=short_volume,
                    total_volume=total_volume,
                    short_volume_ratio=ratio,
                    source_name=row.get("source_name") or "finra",
                    created_at=datetime.utcnow(),
                )
            )

        out = [row for row in out if row.instrument_id]
        return out, {
            "raw_daily_short_volume_rows": len(raw_rows),
            "daily_short_volume_history_rows": len(out),
        }

    def build_short_features_daily(
        self,
        short_interest_rows: list[ShortInterestSnapshot],
        daily_short_volume_rows: list[DailyShortVolumeSnapshot],
    ) -> tuple[list[ShortFeatureDaily], dict[str, int]]:
        latest_short_by_symbol: dict[str, ShortInterestSnapshot] = {}
        prev_short_by_symbol: dict[str, ShortInterestSnapshot] = {}
        latest_volume_by_symbol: dict[str, DailyShortVolumeSnapshot] = {}

        ordered_short = sorted(short_interest_rows, key=lambda x: (x.symbol, x.settlement_date))
        for row in ordered_short:
            if row.symbol in latest_short_by_symbol:
                prev_short_by_symbol[row.symbol] = latest_short_by_symbol[row.symbol]
            latest_short_by_symbol[row.symbol] = row

        ordered_volume = sorted(daily_short_volume_rows, key=lambda x: (x.symbol, x.trade_date))
        for row in ordered_volume:
            latest_volume_by_symbol[row.symbol] = row

        symbols = sorted(set(latest_short_by_symbol) | set(latest_volume_by_symbol))
        features: list[ShortFeatureDaily] = []

        for symbol in symbols:
            s = latest_short_by_symbol.get(symbol)
            p = prev_short_by_symbol.get(symbol)
            v = latest_volume_by_symbol.get(symbol)

            instrument_id = (s.instrument_id if s else v.instrument_id) if (s or v) else ""
            company_id = (s.company_id if s else v.company_id) if (s or v) else None
            as_of_date = None
            if v is not None:
                as_of_date = v.trade_date
            elif s is not None:
                as_of_date = s.settlement_date

            if not instrument_id or as_of_date is None:
                continue

            short_interest_change = None
            if s is not None and p is not None and s.short_interest is not None and p.short_interest is not None:
                short_interest_change = s.short_interest - p.short_interest

            features.append(
                ShortFeatureDaily(
                    instrument_id=instrument_id,
                    company_id=company_id,
                    symbol=symbol,
                    as_of_date=as_of_date,
                    short_interest=s.short_interest if s else None,
                    avg_daily_volume=s.avg_daily_volume if s else None,
                    days_to_cover=s.days_to_cover if s else None,
                    short_volume=v.short_volume if v else None,
                    total_volume=v.total_volume if v else None,
                    short_volume_ratio=v.short_volume_ratio if v else None,
                    short_interest_change=short_interest_change,
                    source_name="finra",
                    created_at=datetime.utcnow(),
                )
            )

        return features, {"short_feature_rows": len(features)}
