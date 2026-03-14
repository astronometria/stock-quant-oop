from __future__ import annotations

from datetime import datetime

import pandas as pd

from stock_quant.domain.entities.backtest_result import BacktestResultSummary, ExperimentResultDaily


class SignalBacktester:
    def run_simple_momentum_backtest(
        self,
        dataset_rows: list[dict],
        experiment_name: str,
        backtest_name: str,
        dataset_name: str,
        dataset_version: str,
        min_signal: float = 0.0,
    ) -> tuple[list[ExperimentResultDaily], BacktestResultSummary, dict[str, int | float]]:
        frame = pd.DataFrame(dataset_rows)
        if frame.empty:
            summary = BacktestResultSummary(
                backtest_name=backtest_name,
                dataset_name=dataset_name,
                dataset_version=dataset_version,
                observations=0,
                selected_observations=0,
                mean_return_1d=None,
                hit_rate_1d=None,
                cumulative_return_proxy=None,
                created_at=datetime.utcnow(),
            )
            return [], summary, {
                "experiment_rows": 0,
                "selected_rows": 0,
            }

        frame["signal_value"] = pd.to_numeric(frame["close_to_sma_20"], errors="coerce")
        frame["realized_return_1d"] = pd.to_numeric(frame["fwd_return_1d"], errors="coerce")
        frame["selected_flag"] = frame["signal_value"] > min_signal

        experiment_rows: list[ExperimentResultDaily] = []
        for row in frame.itertuples(index=False):
            experiment_rows.append(
                ExperimentResultDaily(
                    experiment_name=experiment_name,
                    dataset_name=dataset_name,
                    dataset_version=dataset_version,
                    instrument_id=row.instrument_id,
                    company_id=row.company_id,
                    symbol=row.symbol,
                    as_of_date=row.as_of_date,
                    signal_value=None if pd.isna(row.signal_value) else float(row.signal_value),
                    selected_flag=bool(row.selected_flag),
                    realized_return_1d=None if pd.isna(row.realized_return_1d) else float(row.realized_return_1d),
                    created_at=datetime.utcnow(),
                )
            )

        selected = frame[frame["selected_flag"] & frame["realized_return_1d"].notna()].copy()

        mean_return = None
        hit_rate = None
        cumulative_return_proxy = None
        if not selected.empty:
            mean_return = float(selected["realized_return_1d"].mean())
            hit_rate = float((selected["realized_return_1d"] > 0).mean())
            cumulative_return_proxy = float((1.0 + selected["realized_return_1d"]).prod() - 1.0)

        summary = BacktestResultSummary(
            backtest_name=backtest_name,
            dataset_name=dataset_name,
            dataset_version=dataset_version,
            observations=int(len(frame)),
            selected_observations=int(len(selected)),
            mean_return_1d=mean_return,
            hit_rate_1d=hit_rate,
            cumulative_return_proxy=cumulative_return_proxy,
            created_at=datetime.utcnow(),
        )

        metrics = {
            "experiment_rows": int(len(experiment_rows)),
            "selected_rows": int(len(selected)),
        }
        return experiment_rows, summary, metrics
