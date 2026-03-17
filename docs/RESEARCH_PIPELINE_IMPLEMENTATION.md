# Research Pipeline Implementation Plan

## Objectif immédiat

Transformer le moteur de recherche quant actuel en pipeline de test cross-sectional propre.

## Ordre cible

1. build_feature_engine
2. build_label_engine
3. build_dataset_builder
4. build_backtest

## Objectif du backtest v1

- ranking quotidien cross-sectional
- top N long-only
- equal weight
- holding 1 jour
- rebalance daily

## Contraintes

- utiliser price_history uniquement
- ne jamais utiliser price_latest
- labels séparés des features
- dataset point-in-time
- une seule ligne par (symbol, as_of_date)

## Livrables v1

- diagnostic du pipeline recherche actuel
- inventaire des colonnes du dataset builder
- inventaire des colonnes du feature engine
- inventaire des labels disponibles
- plan de refactor du backtester

