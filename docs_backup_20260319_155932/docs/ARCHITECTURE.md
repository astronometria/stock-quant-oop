# Architecture

## Vue d’ensemble

Le projet suit une architecture en couches :

- domain : logique métier pure
- app/services : orchestration métier
- infrastructure/repositories : persistance DuckDB
- infrastructure/providers : accès aux sources externes ou staging
- pipelines : exécution standardisée des traitements
- cli : points d’entrée utilisateur

Cette architecture permet :

- séparation claire des responsabilités
- testabilité élevée
- remplacement facile des sources de données

---

# Structure principale

## stock_quant/domain

Contient les éléments métier fondamentaux :

entities  
value_objects  
policies  
ports  

Les policies contiennent les règles métier :

- UniverseInclusionPolicy
- ExchangeNormalizationPolicy
- SecurityClassificationPolicy
- UniverseConflictResolutionPolicy

---

## stock_quant/app/services

Services métier qui orchestrent les règles domain.

Principaux services :

- market_universe_service
- symbol_reference_service
- price_ingestion_service
- short_interest_service
- news_raw_ingestion_service
- news_symbol_candidate_service

---

## stock_quant/infrastructure/repositories

Couche de persistance DuckDB.

Principaux repositories :

- duckdb_universe_repository
- duckdb_symbol_reference_repository
- duckdb_price_repository
- duckdb_short_interest_repository
- duckdb_news_repository

---

## stock_quant/infrastructure/providers

Couche d’accès aux sources de données.

Providers principaux :

prices
- yfinance_price_provider
- historical_price_provider
- raw_price_loader

finra
- finra_provider
- finra_raw_loader

news
- news_source_loader

symbols
- symbol_source_loader

---

## stock_quant/pipelines

Chaque pipeline suit la structure :

extract  
transform  
validate  
load  

Pipelines principaux :

- market_universe_pipeline
- symbol_reference_pipeline
- prices_pipeline
- finra_short_interest_pipeline
- news_raw_pipeline
- news_symbol_candidates_pipeline

---

## stock_quant/app/orchestrators

Orchestration des pipelines.

Orchestrateur principal :

core_pipeline_orchestrator

Flux actuel :

1 init_market_db  
2 build_market_universe  
3 build_symbol_reference  
4 build_prices

---

# CLI

Les scripts CLI sont séparés par rôle :

cli/core
points d’entrée principaux

cli/ops
opérations récurrentes

cli/raw
scripts staging / ingestion brute

cli/tools
utilitaires d’export

cli/ops
anciens scripts conservés pour référence

---

# Compatibilité

Les anciens scripts ont été déplacés sous cli/raw.

Des wrappers sont conservés dans cli pour ne pas casser les anciennes commandes.

---

# Améliorations futures possibles

- enrichir les métriques pipeline
- intégrer news et FINRA dans le flux orchestré principal
- centraliser davantage le logging pipeline
