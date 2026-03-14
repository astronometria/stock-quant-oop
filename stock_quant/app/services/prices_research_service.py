from __future__ import annotations

from datetime import datetime
from typing import Any

import pandas as pd

from stock_quant.domain.entities.corporate_action import PriceQualityFlag


class PricesResearchService:
    def build_unadjusted_bars(
        self,
        price_history_rows: list[dict[str, Any]],
        instrument_map: dict[str, str],
    ) -> tuple[pd.DataFrame, dict[str, int]]:
        frame = pd.DataFrame(price_history_rows)
        if frame.empty:
            return self._empty_price_frame(), {
                "input_price_rows": 0,
                "unadjusted_rows": 0,
                "missing_instrument_map": 0,
            }

        frame["symbol"] = frame["symbol"].astype(str).str.strip().str.upper()
        frame["instrument_id"] = frame["symbol"].map(instrument_map)
        missing_instrument_map = int(frame["instrument_id"].isna().sum())

        frame = frame.dropna(subset=["instrument_id"]).copy()
        if frame.empty:
            return self._empty_price_frame(), {
                "input_price_rows": len(price_history_rows),
                "unadjusted_rows": 0,
                "missing_instrument_map": missing_instrument_map,
            }

        frame = frame.rename(columns={"price_date": "bar_date"})
        frame["source_name"] = frame["source_name"].fillna("unknown").astype(str)
        frame["created_at"] = datetime.utcnow()

        out = frame.loc[
            :,
            [
                "instrument_id",
                "symbol",
                "bar_date",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "source_name",
                "created_at",
            ],
        ].sort_values(["instrument_id", "bar_date"]).reset_index(drop=True)

        metrics = {
            "input_price_rows": len(price_history_rows),
            "unadjusted_rows": len(out),
            "missing_instrument_map": missing_instrument_map,
        }
        return out, metrics

    def build_adjusted_bars(
        self,
        unadjusted_bars: pd.DataFrame,
    ) -> tuple[pd.DataFrame, dict[str, int]]:
        if unadjusted_bars.empty:
            return self._empty_adjusted_frame(), {"adjusted_rows": 0}

        out = unadjusted_bars.copy()
        out["adj_open"] = out["open"]
        out["adj_high"] = out["high"]
        out["adj_low"] = out["low"]
        out["adj_close"] = out["close"]
        out["adjustment_factor"] = 1.0

        out = out.loc[
            :,
            [
                "instrument_id",
                "symbol",
                "bar_date",
                "adj_open",
                "adj_high",
                "adj_low",
                "adj_close",
                "volume",
                "adjustment_factor",
                "source_name",
                "created_at",
            ],
        ].sort_values(["instrument_id", "bar_date"]).reset_index(drop=True)

        return out, {"adjusted_rows": len(out)}

    def build_quality_flags(
        self,
        unadjusted_bars: pd.DataFrame,
    ) -> tuple[list[PriceQualityFlag], dict[str, int]]:
        if unadjusted_bars.empty:
            return [], {"quality_flags": 0}

        flags: list[PriceQualityFlag] = []
        now = datetime.utcnow()

        for row in unadjusted_bars.itertuples(index=False):
            if row.high < row.low:
                flags.append(
                    PriceQualityFlag(
                        instrument_id=row.instrument_id,
                        price_date=row.bar_date,
                        flag_type="high_below_low",
                        flag_value="true",
                        source_name=row.source_name,
                        created_at=now,
                    )
                )
            if row.volume is not None and int(row.volume) < 0:
                flags.append(
                    PriceQualityFlag(
                        instrument_id=row.instrument_id,
                        price_date=row.bar_date,
                        flag_type="negative_volume",
                        flag_value="true",
                        source_name=row.source_name,
                        created_at=now,
                    )
                )

        return flags, {"quality_flags": len(flags)}

    def _empty_price_frame(self) -> pd.DataFrame:
        return pd.DataFrame(
            columns=[
                "instrument_id",
                "symbol",
                "bar_date",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "source_name",
                "created_at",
            ]
        )

    def _empty_adjusted_frame(self) -> pd.DataFrame:
        return pd.DataFrame(
            columns=[
                "instrument_id",
                "symbol",
                "bar_date",
                "adj_open",
                "adj_high",
                "adj_low",
                "adj_close",
                "volume",
                "adjustment_factor",
                "source_name",
                "created_at",
            ]
        )
