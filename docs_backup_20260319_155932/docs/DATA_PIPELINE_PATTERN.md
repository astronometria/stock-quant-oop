# Stock-Quant Data Pipeline Pattern

Ce document définit le design pattern officiel pour tous les pipelines du projet.

Objectif :

- pipelines reproductibles
- auditables
- SQL-first
- bias-aware
- compatibles OOP

Chaque domaine de données suit exactement les mêmes couches :

raw
↓
normalized
↓
derived / features / serving

## 1. Raw layer

But :

- conserver les données source telles quelles
- append-only
- reprocessing possible
- audit complet

Les tables raw ne doivent jamais être modifiées par des pipelines downstream.

Convention :

    <domain>_*_raw

Exemples :

- symbol_reference_source_raw
- sec_filing_raw_index
- sec_xbrl_fact_raw
- price_source_daily_raw
- price_source_daily_raw_yahoo
- finra_short_interest_source_raw
- finra_daily_short_volume_source_raw

CLI associées :

- cli/raw/*

## 2. Normalized layer

But :

- harmoniser le schéma
- nettoyer les données
- typer correctement
- dédupliquer
- créer les clés métier
- intégrer les sources multiples
- garantir la sécurité PIT

Convention :

    <domain>_*
ou
    <domain>_*_normalized

Exemples :

- symbol_reference
- sec_filing
- sec_fact_normalized
- price_history
- finra_short_interest_history

Règles PIT :

- SEC : utiliser available_at pour la disponibilité marché
- FINRA : utiliser available_at / publication date pour la disponibilité marché
- PRICES : price_history est la normalized canonique, price_latest est serving only

Les pipelines downstream doivent lire uniquement les tables normalized.

CLI associées :

- cli/core/build_*

Pipelines :

- stock_quant/pipelines/build_*_pipeline.py

## 3. Derived / Feature layer

But :

- tables analytiques
- agrégations
- snapshots
- features ML
- latest state

Convention :

- *_latest
- *_features_daily
- *_snapshot_*
- *_ttm

Exemples :

- market_universe
- price_latest
- fundamental_snapshot_quarterly
- fundamental_snapshot_annual
- fundamental_ttm
- fundamental_features_daily
- finra_short_interest_latest
- short_features_daily

## 4. Orchestrators

Les orchestrateurs gèrent :

- ordre des pipelines
- refresh incrémental
- gestion des sources
- logging

CLI :

- cli/ops/run_*_daily_refresh.py
- cli/ops/rebuild_database_from_scratch.py

## Domain pipelines

### Symbols

raw

- symbol_reference_source_raw

normalized

- symbol_reference

derived

- market_universe
- universe_membership_history

### SEC

raw

- sec_filing_raw_index
- sec_xbrl_fact_raw

normalized

- sec_filing
- sec_fact_normalized

derived

- fundamental_snapshot_quarterly
- fundamental_snapshot_annual
- fundamental_ttm
- fundamental_features_daily

### Prices

raw

- price_source_daily_raw
- price_source_daily_raw_all
- price_source_daily_raw_yahoo

normalized

- price_history

derived

- price_latest

### FINRA

raw

- finra_short_interest_source_raw
- finra_daily_short_volume_source_raw

normalized

- finra_short_interest_history

derived

- finra_short_interest_latest
- short_features_daily

## Hard rules

1. Raw tables are append-only.
2. Normalized tables must be deterministic and idempotent.
3. Derived tables must depend only on normalized tables.
4. No research pipeline should read raw tables.
5. No backtest / dataset / labeler should read price_latest.
6. SEC and FINRA availability must use available_at, not economic period date alone.
7. Python orchestration should remain thin.
8. SQL-first logic whenever possible.
