from __future__ import annotations

import duckdb


class ResearchBacktestSchemaManager:
    def __init__(self, con: duckdb.DuckDBPyConnection) -> None:
        self.con = con

    def ensure_tables(self) -> None:
        self.con.execute("""
            CREATE TABLE IF NOT EXISTS research_backtest (
                backtest_id VARCHAR,
                dataset_id VARCHAR,
                split_id VARCHAR,
                partition_name VARCHAR,
                signal_name VARCHAR,
                transaction_cost_bps DOUBLE,
                gross_return DOUBLE,
                total_cost DOUBLE,
                net_return DOUBLE,
                avg_return DOUBLE,
                volatility DOUBLE,
                sharpe DOUBLE,
                turnover DOUBLE,
                n_obs BIGINT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
