# Backtest Engine v1

## Objectif

Remplacer le simple backtest momentum par un moteur cross-sectional quotidien.

## Hypothèses v1

- long only
- rebalance daily
- equal weight
- holding = 1 jour effectif via label forward return
- ranking par colonne signal
- top N sélectionnés chaque jour

## Entrées

- research_dataset_daily
- dataset_name
- dataset_version
- signal_column
- label_column
- top_n

## Sorties

- backtest_runs
- backtest_positions
- backtest_daily

## Contraintes anti-biais

- utiliser uniquement research_dataset_daily
- ne jamais relire price_latest
- le signal doit venir d'une colonne feature, pas d'un label
- une seule ligne par (symbol, as_of_date) dans le dataset source

