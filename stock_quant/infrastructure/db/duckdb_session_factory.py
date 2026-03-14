from __future__ import annotations

from pathlib import Path

import duckdb

from stock_quant.shared.exceptions import RepositoryError


class DuckDbSessionFactory:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path.expanduser().resolve()

    def create(self) -> duckdb.DuckDBPyConnection:
        try:
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
            con = duckdb.connect(str(self.db_path))
            con.execute("PRAGMA enable_progress_bar=false")
            return con
        except Exception as exc:
            raise RepositoryError(f"failed to open DuckDB at {self.db_path}: {exc}") from exc
