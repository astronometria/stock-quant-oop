# Pipeline Usage

Ce document décrit comment utiliser les pipelines principaux.

---

# Initialiser la base

Commande :

    python3 cli/core/init_market_db.py \
      --db-path ~/stock-quant-oop/market.duckdb

Recréation complète :

    python3 cli/core/init_market_db.py \
      --db-path ~/stock-quant-oop/market.duckdb \
      --drop-existing

---

# Construire l’univers marché

    python3 cli/core/build_market_universe.py \
      --db-path ~/stock-quant-oop/market.duckdb

Exclure les ADR :

    python3 cli/core/build_market_universe.py \
      --db-path ~/stock-quant-oop/market.duckdb \
      --disallow-adr

---

# Construire le référentiel symboles

    python3 cli/core/build_symbol_reference.py \
      --db-path ~/stock-quant-oop/market.duckdb

---

# Prix

## Mise à jour journalière

    python3 cli/core/build_prices.py \
      --db-path ~/stock-quant-oop/market.duckdb \
      --mode daily \
      --symbol AAPL \
      --symbol MSFT

Le provider utilisé est yfinance.

---

## Backfill historique

    python3 cli/core/build_prices.py \
      --db-path ~/stock-quant-oop/market.duckdb \
      --mode backfill \
      --historical-source /chemin/source.csv

Plusieurs sources :

    python3 cli/core/build_prices.py \
      --db-path ~/stock-quant-oop/market.duckdb \
      --mode backfill \
      --historical-source source1.csv \
      --historical-source source2.zip

---

# FINRA

Pipeline principal :

    python3 cli/core/build_finra_short_interest.py \
      --db-path ~/stock-quant-oop/market.duckdb

Chargement raw :

    python3 cli/load_finra_short_interest_source_raw.py \
      --source fichier_finra.csv

---

# News

Chargement raw :

    python3 cli/load_news_source_raw.py \
      --source news.csv

Pipeline news :

    python3 cli/core/build_news_raw.py \
      --db-path ~/stock-quant-oop/market.duckdb

Extraction candidats ticker :

    python3 cli/core/build_news_symbol_candidates.py \
      --db-path ~/stock-quant-oop/market.duckdb

---

# Core Pipeline

Exécution complète :

    python3 cli/core/run_core_pipeline.py \
      --db-path ~/stock-quant-oop/market.duckdb

Smoke rapide :

    python3 cli/core/run_core_pipeline.py \
      --db-path ~/stock-quant-oop/market.duckdb \
      --skip-symbol-load \
      --skip-price-load \
      --skip-finra-load \
      --skip-news-load \
      --skip-finra \
      --skip-news-raw \
      --skip-news-candidates
