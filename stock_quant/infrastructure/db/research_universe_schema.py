from __future__ import annotations

from stock_quant.infrastructure.db.unit_of_work import DuckDbUnitOfWork


class ResearchUniverseSchemaManager:
    def __init__(self, uow: DuckDbUnitOfWork) -> None:
        self.uow = uow

    @property
    def con(self):
        if self.uow.connection is None:
            raise RuntimeError("active DB connection is required")
        return self.uow.connection

    def initialize(self) -> None:
        self._create_research_universe()

    def _create_research_universe(self) -> None:
        self.con.execute(
            """
            CREATE TABLE IF NOT EXISTS research_universe (
                symbol VARCHAR,
                venue_group VARCHAR,
                asset_class VARCHAR,
                is_adr BOOLEAN,
                is_suffix_derived BOOLEAN,
                suffix_type VARCHAR,
                base_symbol VARCHAR,
                is_preferred_candidate BOOLEAN,
                include_in_research_universe BOOLEAN,
                exclusion_reason VARCHAR,
                created_at TIMESTAMP
            )
            """
        )
