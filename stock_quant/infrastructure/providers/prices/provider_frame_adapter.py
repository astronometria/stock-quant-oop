from __future__ import annotations

"""
ProviderFrameAdapter

Responsabilité :
- appeler le provider avec les symboles provider
- remapper les résultats vers les symboles canoniques
"""

import pandas as pd


class ProviderFrameAdapter:
    def __init__(self, provider):
        self._provider = provider

    def fetch_prices(
        self,
        provider_symbols: list[str],
        provider_to_canonical: dict[str, str],
        start_date,
        end_date,
        requires_range_fetch: bool,
    ) -> pd.DataFrame | None:
        if not provider_symbols:
            return pd.DataFrame()

        df = self._provider.fetch(
            symbols=provider_symbols,
            start_date=start_date,
            end_date=end_date,
            requires_range_fetch=requires_range_fetch,
        )

        if df is None or df.empty:
            return pd.DataFrame()

        df = df.copy()
        df["symbol"] = df["symbol"].map(provider_to_canonical)

        # Élimine tout symbole provider qui ne remappe pas correctement.
        df = df.dropna(subset=["symbol"]).reset_index(drop=True)

        # Déduplication défensive.
        df = df.drop_duplicates(subset=["symbol", "price_date"]).reset_index(drop=True)

        return df
