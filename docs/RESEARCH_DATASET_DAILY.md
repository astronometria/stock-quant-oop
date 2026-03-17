# Research Dataset Daily

Orchestrateur complet de dataset research.

Pipeline exécuté :

prices
short_interest
fundamentals
training_dataset
dataset_version

Commande :

python3 cli/ops/run_research_dataset_daily.py     --db-path market.duckdb     --dataset-version daily

Output :

training_dataset_daily
dataset_versions

Règles anti-biais :

- prices = price_history seulement
- fundamentals = available_at <= price_date
- short_interest = available_at <= price_date
- universe = include_in_universe = TRUE
