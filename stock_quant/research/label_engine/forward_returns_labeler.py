from __future__ import annotations

from datetime import datetime

import pandas as pd

from stock_quant.domain.entities.research_label import ReturnLabelDaily, VolatilityLabelDaily


class ForwardReturnsLabeler:
    def build(
        self,
        price_rows: list[dict],
        instrument_company_map: dict[str, str | None],
    ) -> tuple[list[ReturnLabelDaily], list[VolatilityLabelDaily], dict[str, int]]:
        frame = pd.DataFrame(price_rows)
        if frame.empty:
            return [], [], {
                "return_label_rows": 0,
                "volatility_label_rows": 0,
            }

        frame = frame.sort_values(["instrument_id", "bar_date"]).reset_index(drop=True)
        frame["adj_close"] = pd.to_numeric(frame["adj_close"], errors="coerce")

        return_rows: list[ReturnLabelDaily] = []
        vol_rows: list[VolatilityLabelDaily] = []

        for instrument_id, group in frame.groupby("instrument_id", sort=False):
            working = group.copy()
            working["ret_1"] = working["adj_close"].shift(-1) / working["adj_close"] - 1.0
            working["ret_5"] = working["adj_close"].shift(-5) / working["adj_close"] - 1.0
            working["ret_20"] = working["adj_close"].shift(-20) / working["adj_close"] - 1.0

            working["daily_ret"] = working["adj_close"].pct_change()
            working["realized_vol_20d"] = working["daily_ret"].rolling(20, min_periods=10).std()

            company_id = instrument_company_map.get(instrument_id)

            for row in working.itertuples(index=False):
                fwd_1d = None if pd.isna(row.ret_1) else float(row.ret_1)
                fwd_5d = None if pd.isna(row.ret_5) else float(row.ret_5)
                fwd_20d = None if pd.isna(row.ret_20) else float(row.ret_20)
                rv20 = None if pd.isna(row.realized_vol_20d) else float(row.realized_vol_20d)

                return_rows.append(
                    ReturnLabelDaily(
                        instrument_id=row.instrument_id,
                        company_id=company_id,
                        symbol=row.symbol,
                        as_of_date=row.bar_date,
                        fwd_return_1d=fwd_1d,
                        fwd_return_5d=fwd_5d,
                        fwd_return_20d=fwd_20d,
                        direction_1d=self._direction(fwd_1d),
                        direction_5d=self._direction(fwd_5d),
                        direction_20d=self._direction(fwd_20d),
                        source_name="labels",
                        created_at=datetime.utcnow(),
                    )
                )

                vol_rows.append(
                    VolatilityLabelDaily(
                        instrument_id=row.instrument_id,
                        company_id=company_id,
                        symbol=row.symbol,
                        as_of_date=row.bar_date,
                        realized_vol_20d=rv20,
                        source_name="labels",
                        created_at=datetime.utcnow(),
                    )
                )

        metrics = {
            "return_label_rows": len(return_rows),
            "volatility_label_rows": len(vol_rows),
        }
        return return_rows, vol_rows, metrics

    def _direction(self, value: float | None) -> int | None:
        if value is None:
            return None
        if value > 0:
            return 1
        if value < 0:
            return -1
        return 0
