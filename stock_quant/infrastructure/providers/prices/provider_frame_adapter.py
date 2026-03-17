from __future__ import annotations

"""
ProviderFrameAdapter

Responsabilité :
- appeler le provider avec des symboles provider
- remapper vers symbole canonique
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
            return None

        df = self._provider.fetch(
            symbols=provider_symbols,
            start_date=start_date,
            end_date=end_date,
            requires_range_fetch=requires_range_fetch,
        )

        if df is None or df.empty:
            return df

        # --------------------------------------------------------------
        # Remap vers symbole canonique
        # --------------------------------------------------------------
        df["symbol"] = df["symbol"].map(provider_to_canonical)

        return df
