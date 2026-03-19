# Dataset Versioning

Objectif :

Versionner les datasets PIT pour garantir la reproductibilité exacte des backtests
et des entraînements.

Pattern :

normalized / PIT dataset
↓
dataset version
↓
frozen training artifact

## Tables logiques

- training_dataset_daily
- dataset_versions

## Règles

1. Une version de dataset doit être immutable après publication.
2. Une version doit contenir au minimum :
   - dataset_name
   - dataset_version
   - built_at
   - row_count
   - start_date
   - end_date
   - source_tables_json
3. Les backtests doivent référencer une version explicite.
4. Les modèles doivent référencer une version explicite.

## Flux cible

price_history
fundamental_features_daily
short_features_daily
market_universe
↓
training_dataset_daily
↓
dataset_versions

## Anti-biais

- Le dataset versionné doit être construit à partir du dataset PIT
- jamais à partir de tables latest
- jamais à partir de raw
