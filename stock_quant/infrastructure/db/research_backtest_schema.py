from __future__ import annotations

import duckdb


class ResearchBacktestSchemaManager:
    def __init__(self, con: duckdb.DuckDBPyConnection) -> None:
        self.con = con

    def ensure_tables(self) -> None:
        # Résultat agrégé du backtest
        self.con.execute("""
            CREATE TABLE IF NOT EXISTS research_backtest (
                backtest_id VARCHAR,
                dataset_id VARCHAR,
                signal_name VARCHAR,
                avg_return DOUBLE,
                volatility DOUBLE,
                sharpe DOUBLE,
                total_return DOUBLE,
                n_obs BIGINT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
