# Runbook Research Pipeline

## Objectif

Ce runbook décrit l'exécution opératoire du moteur research `stock-quant-oop`.

## Ordre logique

1. Charger / mettre à jour les sources raw
2. Construire les couches normalisées
3. Construire features et labels
4. Construire le dataset versionné
5. Exécuter les expériences et backtests
6. Vérifier les journaux de runs

## Commandes principales

### Daily research run

python3 cli/ops/run_research_daily.py \
  --db-path ~/stock-quant-oop/market.duckdb \
  --dataset-name research_dataset_v1 \
  --dataset-version v1 \
  --experiment-name momentum_experiment_v1 \
  --backtest-name momentum_backtest_v1 \
  --verbose

### Replay avec nouveaux noms logiques

python3 cli/ops/run_research_replay.py \
  --db-path ~/stock-quant-oop/market.duckdb \
  --dataset-name research_dataset_replay \
  --dataset-version replay_v1 \
  --experiment-name momentum_experiment_replay \
  --backtest-name momentum_backtest_replay \
  --verbose

### Statut opératoire

python3 cli/tools/research_status.py --db-path ~/stock-quant-oop/market.duckdb

## Notes importantes

- Les tables `dataset_versions`, `experiment_runs`, `backtest_runs`, `llm_runs` sont traitées avec un nettoyage logique préalable dans le mode daily.
- `pipeline_runs` doit servir de journal opérationnel principal.
- Le backtest actuel est un prototype de signal et non un portefeuille réaliste.
- Les étapes les plus coûteuses doivent être refactorées en SQL-first.

## Priorités performance

1. `build_feature_engine`
2. `build_label_engine`
3. `build_dataset_builder`
4. `build_backtest`

## Priorités qualité

1. Tests unitaires et intégration réels
2. Point-in-time strict
3. Corporate actions réelles
4. SEC/XBRL réel
5. LLM réel avec tracking de coût/tokens
