from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class FeatureCatalogEntry:
    feature_name: str
    feature_family: str
    feature_group: str
    description: str
    input_tables: tuple[str, ...]
    output_table: str
    parameters_json: str = "{}"
    feature_version: str = "v1"
    is_active: bool = True


DEFAULT_FEATURE_CATALOG: tuple[FeatureCatalogEntry, ...] = (
    FeatureCatalogEntry(
        feature_name="close_to_sma_20",
        feature_family="technical",
        feature_group="trend",
        description="Distance relative entre le close et la SMA 20.",
        input_tables=("price_bars_adjusted",),
        output_table="technical_features_daily",
    ),
    FeatureCatalogEntry(
        feature_name="rsi_14",
        feature_family="technical",
        feature_group="momentum",
        description="RSI 14 jours.",
        input_tables=("price_bars_adjusted",),
        output_table="technical_features_daily",
    ),
    FeatureCatalogEntry(
        feature_name="days_to_cover",
        feature_family="short",
        feature_group="crowding",
        description="Days to cover provenant des données short interest.",
        input_tables=("short_interest_history",),
        output_table="short_features_daily",
    ),
    FeatureCatalogEntry(
        feature_name="news_event_count_1d",
        feature_family="news",
        feature_group="event",
        description="Nombre d'événements news liés au symbole sur 1 jour.",
        input_tables=("news_symbol_links",),
        output_table="news_features_daily",
    ),
    FeatureCatalogEntry(
        feature_name="ttm_revenue_growth",
        feature_family="fundamental",
        feature_group="growth",
        description="Croissance du revenu TTM.",
        input_tables=("fundamental_ttm",),
        output_table="fundamental_features_daily",
    ),
)


def feature_catalog_rows() -> list[dict[str, Any]]:
    return [asdict(item) for item in DEFAULT_FEATURE_CATALOG]
