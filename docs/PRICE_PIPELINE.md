# Price Pipeline

## Objectif

Construire les tables :

price_history  
price_latest  

à partir de plusieurs providers.

---

# Providers

## Daily

Provider principal :

YfinancePriceProvider

Localisation :

stock_quant.infrastructure.providers.prices.yfinance_price_provider

Usage :

- mise à jour journalière
- rattrapage court
- exécution par symbole

---

## Historique

Provider :

HistoricalPriceProvider

Localisation :

stock_quant.infrastructure.providers.prices.historical_price_provider

Usage :

- backfill massif
- ingestion CSV
- ingestion ZIP
- ingestion dossier Stooq

---

# Chaîne d’exécution

Provider  
→ PriceIngestionService  
→ BuildPricesPipeline  
→ DuckDbPriceRepository

---

# Entrée CLI principale

Daily :

    python3 cli/core/build_prices.py \
      --db-path ~/stock-quant-oop/market.duckdb \
      --mode daily \
      --symbol AAPL

Backfill :

    python3 cli/core/build_prices.py \
      --db-path ~/stock-quant-oop/market.duckdb \
      --mode backfill \
      --historical-source source.csv

---

# Remarques

price_latest est reconstruit à partir de price_history.

Les anciens scripts raw prix sont maintenant rangés sous :

cli/raw

Les wrappers restent présents sous :

cli
