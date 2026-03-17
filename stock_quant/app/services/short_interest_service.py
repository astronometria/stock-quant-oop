from __future__ import annotations

from datetime import datetime
from typing import Iterable

from stock_quant.domain.entities.short_interest import (
    RawShortInterestRecord,
    ShortInterestRecord,
    ShortInterestSourceFile,
)
from stock_quant.domain.policies.finra_market_selection_policy import (
    FinraMarketSelectionPolicy,
)


class ShortInterestService:
    """
    Service canonique du domaine short interest.

    Objectifs :
    - rester compatible avec l'ancien flux `build(...)`
    - supporter le nouveau pipeline `load_raw() -> build_history() -> refresh_latest()`
    - garder la logique métier mince, SQL-first côté repository

    Notes :
    - `load_raw()` retourne un COUNT de lignes lues, comme attendu par le pipeline
    - `build_history()` construit l'historique canonique depuis la staging raw
    - `refresh_latest()` reconstruit la table latest depuis history
    """

    def __init__(
        self,
        repository=None,
        market_policy: FinraMarketSelectionPolicy | None = None,
    ) -> None:
        self.repository = repository
        self.market_policy = market_policy or FinraMarketSelectionPolicy()

        # Cache interne :
        # le pipeline charge d'abord les raw rows puis construit history.
        # On garde donc les raw en mémoire pour éviter un second fetch inutile
        # dans le même run.
        self._loaded_raw_rows: list[dict] = []

    # ------------------------------------------------------------------
    # New pipeline API
    # ------------------------------------------------------------------
    def load_raw(self) -> int:
        """
        Charge les lignes raw depuis la staging table et les garde en mémoire.

        Retour :
        - nombre de lignes raw lues
        """
        if self.repository is None:
            raise RuntimeError("repository is required for load_raw()")

        self._loaded_raw_rows = list(self.repository.load_raw())
        return len(self._loaded_raw_rows)

    def build_history(self) -> int:
        """
        Construit / upsert l'historique canonique short interest à partir
        des lignes raw déjà chargées.

        Retour :
        - nombre de lignes écrites/upsertées dans history
        """
        if self.repository is None:
            raise RuntimeError("repository is required for build_history()")

        # Si le pipeline appelle build_history() sans load_raw() avant,
        # on se protège en rechargeant les données.
        if not self._loaded_raw_rows:
            self._loaded_raw_rows = list(self.repository.load_raw())

        entries: list[ShortInterestRecord] = []

        for row in self._loaded_raw_rows:
            # Normalisation prudente du symbole.
            symbol = str(row.get("symbol") or "").strip().upper()
            if not symbol:
                continue

            # La date canonique côté FINRA short interest est settlement_date.
            settlement_date = row.get("settlement_date") or row.get("as_of_date")
            if settlement_date is None:
                continue

            short_interest_value = row.get("short_interest")
            if short_interest_value is None:
                continue

            previous_short_interest_value = row.get("previous_short_interest")
            avg_daily_volume_value = row.get("avg_daily_volume")
            shares_float_value = row.get("shares_float")
            revision_flag_value = row.get("revision_flag")
            source_market_value = row.get("source_market") or "unknown"
            source_file_value = row.get("source_file") or "unknown"
            ingested_at_value = row.get("available_at") or row.get("ingested_at") or datetime.utcnow()

            short_interest = int(short_interest_value or 0)
            previous_short_interest = int(previous_short_interest_value or 0)
            avg_daily_volume = float(avg_daily_volume_value or 0.0)

            # Calcul métier léger et déterministe.
            days_to_cover = None
            if avg_daily_volume > 0:
                days_to_cover = short_interest / avg_daily_volume

            short_interest_pct_float = None
            if shares_float_value not in (None, 0):
                try:
                    short_interest_pct_float = short_interest / float(shares_float_value)
                except Exception:
                    short_interest_pct_float = None

            entries.append(
                ShortInterestRecord(
                    symbol=symbol,
                    settlement_date=settlement_date,
                    short_interest=short_interest,
                    previous_short_interest=previous_short_interest,
                    avg_daily_volume=avg_daily_volume,
                    days_to_cover=days_to_cover,
                    shares_float=shares_float_value,
                    short_interest_pct_float=short_interest_pct_float,
                    revision_flag=revision_flag_value,
                    source_market=source_market_value,
                    source_file=source_file_value,
                    ingested_at=ingested_at_value,
                )
            )

        return int(self.repository.upsert_short_interest_history(entries))

    def refresh_latest(self) -> int:
        """
        Reconstruit la vue/latest canonique depuis history.

        Retour :
        - nombre de lignes présentes dans latest après refresh
        """
        if self.repository is None:
            raise RuntimeError("repository is required for refresh_latest()")

        return int(self.repository.rebuild_short_interest_latest())

    # ------------------------------------------------------------------
    # Legacy backward-compatible API
    # ------------------------------------------------------------------
    def build(
        self,
        raw_records: Iterable[RawShortInterestRecord],
        *,
        allowed_symbols: set[str] | None,
        source_market: str,
    ) -> tuple[list[ShortInterestRecord], list[ShortInterestSourceFile], dict[str, int]]:
        """
        Ancienne API conservée pour compatibilité avec les tests et anciens flows.

        Elle applique :
        - le filtre market
        - le filtre universe/symbols autorisés
        - les calculs dérivés simples
        """
        rows = list(raw_records)
        allowed = {str(x).strip().upper() for x in (allowed_symbols or set())}

        history: list[ShortInterestRecord] = []
        source_counts: dict[tuple[str, str, object], int] = {}

        metrics = {
            "accepted_records": 0,
            "accepted_source_files": 0,
            "skipped_market_mismatch": 0,
            "skipped_not_in_universe": 0,
        }

        for row in rows:
            symbol = str(row.symbol).strip().upper()
            if not symbol:
                continue

            if source_market != "both" and not self._matches_market(row.source_market, source_market):
                metrics["skipped_market_mismatch"] += 1
                continue

            if allowed and symbol not in allowed:
                metrics["skipped_not_in_universe"] += 1
                continue

            short_interest = int(row.short_interest or 0)
            previous_short_interest = int(row.previous_short_interest or 0)
            avg_daily_volume = float(row.avg_daily_volume or 0.0)

            days_to_cover = None
            if avg_daily_volume > 0:
                days_to_cover = short_interest / avg_daily_volume

            shares_float = int(row.shares_float) if row.shares_float is not None else None

            short_interest_pct_float = None
            if shares_float and shares_float > 0:
                short_interest_pct_float = short_interest / shares_float

            history.append(
                ShortInterestRecord(
                    symbol=symbol,
                    settlement_date=row.settlement_date,
                    short_interest=short_interest,
                    previous_short_interest=previous_short_interest,
                    avg_daily_volume=avg_daily_volume,
                    days_to_cover=days_to_cover,
                    shares_float=shares_float,
                    short_interest_pct_float=short_interest_pct_float,
                    revision_flag=row.revision_flag,
                    source_market=row.source_market,
                    source_file=row.source_file,
                    ingested_at=datetime.utcnow(),
                )
            )

            key = (row.source_file, row.source_market, row.source_date)
            source_counts[key] = source_counts.get(key, 0) + 1
            metrics["accepted_records"] += 1

        sources = [
            ShortInterestSourceFile(
                source_file=source_file,
                source_market=source_market_value,
                source_date=source_date,
                row_count=row_count,
                loaded_at=datetime.utcnow(),
            )
            for (source_file, source_market_value, source_date), row_count in sorted(source_counts.items())
        ]

        metrics["accepted_source_files"] = len(sources)

        return history, sources, metrics

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _matches_market(self, record_market: str | None, expected_market: str) -> bool:
        record = (record_market or "").strip().lower()
        expected = (expected_market or "").strip().lower()

        if expected == "both":
            return True

        return record == expected
