from __future__ import annotations

from datetime import datetime

import pandas as pd

from stock_quant.domain.entities.research_feature import TechnicalFeatureDaily


class TechnicalFeaturesEngine:
    def build(self, price_rows: list[dict]) -> tuple[list[TechnicalFeatureDaily], dict[str, int]]:
        frame = pd.DataFrame(price_rows)
        if frame.empty:
            return [], {"technical_feature_rows": 0}

        frame = frame.sort_values(["instrument_id", "bar_date"]).reset_index(drop=True)
        frame["close"] = pd.to_numeric(frame["adj_close"], errors="coerce")

        out: list[TechnicalFeatureDaily] = []

        for instrument_id, group in frame.groupby("instrument_id", sort=False):
            working = group.copy()
            working["sma_20"] = working["close"].rolling(20, min_periods=5).mean()

            delta = working["close"].diff()
            gain = delta.clip(lower=0.0)
            loss = (-delta).clip(lower=0.0)
            avg_gain = gain.rolling(14, min_periods=14).mean()
            avg_loss = loss.rolling(14, min_periods=14).mean()
            rs = avg_gain / avg_loss.replace(0.0, pd.NA)
            working["rsi_14"] = 100 - (100 / (1 + rs))

            working["close_to_sma_20"] = (working["close"] / working["sma_20"]) - 1.0

            for row in working.itertuples(index=False):
                out.append(
                    TechnicalFeatureDaily(
                        instrument_id=row.instrument_id,
                        company_id=None,
                        symbol=row.symbol,
                        as_of_date=row.bar_date,
                        close_to_sma_20=None if pd.isna(row.close_to_sma_20) else float(row.close_to_sma_20),
                        rsi_14=None if pd.isna(row.rsi_14) else float(row.rsi_14),
                        source_name="prices",
                        created_at=datetime.utcnow(),
                    )
                )

        return out, {"technical_feature_rows": len(out)}
