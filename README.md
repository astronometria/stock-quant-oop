# stock-quant-oop

Pipeline orienté objet pour construire un référentiel marché US, un référentiel symboles, l’historique de prix, et les tables de base nécessaires aux enrichissements aval.

## État actuel

Le coeur OOP en place couvre déjà :

- initialisation du schéma DuckDB
- construction de `market_universe`
- construction de `symbol_reference`
- construction de `price_history` et `price_latest`
- provider prix daily via `yfinance`
- orchestrateur Python pour le core pipeline
- couche providers pour FINRA, news raw et symbol staging
- wrappers de compatibilité pour conserver les anciennes entrées CLI

## Structure du repo

- `cli/core/` : points d’entrée principaux
- `cli/ops/` : opérations récurrentes ou lourdes
- `cli/raw/` : scripts raw / staging déplacés hors du chemin principal
- `cli/tools/` : utilitaires d’export
- `stock_quant/` : package applicatif
- `docs/` : documentation projet
- `tests/` : tests unitaires et d’intégration

## Commandes coeur

### Initialiser la base

    python3 cli/core/init_market_db.py --db-path ~/stock-quant-oop/market.duckdb

### Construire l’univers marché

    python3 cli/core/build_market_universe.py --db-path ~/stock-quant-oop/market.duckdb

### Construire le référentiel symboles

    python3 cli/core/build_symbol_reference.py --db-path ~/stock-quant-oop/market.duckdb

### Construire les prix

Mode daily via `yfinance` :

    python3 cli/core/build_prices.py \
      --db-path ~/stock-quant-oop/market.duckdb \
      --mode daily \
      --symbol AAPL

Mode backfill via sources historiques :

    python3 cli/core/build_prices.py \
      --db-path ~/stock-quant-oop/market.duckdb \
      --mode backfill \
      --historical-source /chemin/source.csv

### Lancer le core pipeline

    python3 cli/core/run_core_pipeline.py \
      --db-path ~/stock-quant-oop/market.duckdb

## Documentation

- `docs/ARCHITECTURE.md`
- `docs/PIPELINE_USAGE.md`
- `docs/PRICE_PIPELINE.md`

## Remarques

- Le provider daily officiel pour les prix est `yfinance`.
- Les scripts raw/staging ont été déplacés sous `cli/raw/`, avec wrappers de compatibilité en racine `cli/`.
