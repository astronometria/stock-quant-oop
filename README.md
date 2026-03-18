# stock-quant-oop

Quant research pipeline avec architecture OOP et design SQL-first.

---

## 🚀 Overview

Ce projet fournit une pipeline complète pour:

- ingestion de données marché (prices, FINRA, SEC)
- construction de datasets historiques canoniques
- génération de features quantitatives
- exécution incrémentale quotidienne

Objectif: un système robuste pour la recherche quantitative sans biais.

---

## ⚙️ Pipelines principaux

### Prices
- source: yfinance
- tables:
  - price_history (canonique)
  - price_latest (serving)

### SEC filings
- table:
  - sec_filing

### FINRA short interest
- tables:
  - finra_short_interest_history
  - finra_short_interest_latest

### FINRA daily short volume
- tables:
  - finra_daily_short_volume_source_raw
  - daily_short_volume_history

### Short features
- table:
  - short_features_daily

---

## 🧠 Principes fondamentaux

### 1. SQL-first
Toutes les transformations importantes sont faites en SQL:
- normalisation
- historisation
- features
- déduplication

Python est utilisé seulement pour:
- orchestration
- IO
- appels provider

---

### 2. Point-in-time safe
Aucune donnée future ne doit être utilisée.

Toutes les jointures doivent respecter:
- disponibilité réelle des données
- timestamp d’ingestion

---

### 3. No survivor bias
Ne jamais supprimer:
- symboles delistés
- données historiques anciennes

---

### 4. Séparation canonique / serving
Toujours utiliser pour la recherche:

- price_history
- finra_short_interest_history
- daily_short_volume_history
- short_features_daily

Éviter:
- price_latest
- finra_short_interest_latest

---

## ▶️ Pipeline quotidien

Commande principale:

python3 cli/run_daily_pipeline.py

---

## 🔁 Comportement incrémental

Le pipeline fait:

1. Prices (incremental yfinance)
2. FINRA daily short volume
   → skip si déjà aligné
3. FINRA short interest
   → noop si à jour
4. Short features
   → skip si données inchangées
5. SEC filings
   → toujours exécuté (rapide)

---

## ⏰ Cron recommandé (17h)

0 17 * * * cd /home/marty/stock-quant-oop && /usr/bin/python3 cli/run_daily_pipeline.py >> logs/cron.log 2>&1

---

## 📂 Structure du repo

stock-quant-oop/
  cli/
    core/
    raw/
  stock_quant/
    app/
    domain/
    infrastructure/
    pipelines/
  docs/
  logs/
  tests/

---

## 💾 Tables principales

### Prices
- price_history
- price_latest

### SEC
- sec_filing

### FINRA
- finra_short_interest_history
- daily_short_volume_history

### Features
- short_features_daily

---

## ⚡ Performance

- DuckDB (local analytics engine)
- pipelines incrémentales
- skip automatique des jobs lourds
- gestion de dizaines de millions de lignes

---

## 🔒 Locking DuckDB

Important:

Ne jamais:
- garder une connexion ouverte
- et lancer un autre process en parallèle

Sinon erreur:
"Conflicting lock"

---

## 🧪 Pipeline lourd

Short features:

python3 cli/core/build_short_features.py \
  --db-path market.duckdb \
  --duckdb-threads 2 \
  --duckdb-memory-limit 36GB

---

## 📜 Logs

Tous les logs sont dans:

logs/

Chaque step génère:
- un log dédié
- un log global si exécuté via cron

---

## 🔁 Rebuild complet

Ordre recommandé:

1. init schema
2. symbol normalization
3. prices
4. SEC
5. FINRA short interest
6. FINRA daily short volume
7. short features

---

## 🧠 Debug rapide

Dates maximales:

SELECT MAX(trade_date) FROM daily_short_volume_history;
SELECT MAX(settlement_date) FROM finra_short_interest_history;

---

## 🚀 État actuel

Le système supporte:

- données de prix journalières
- données FINRA complètes
- features short dérivées
- pipeline incrémental performant

---

## 🔮 Prochaines étapes

- signaux quantitatifs
- backtesting
- portfolio simulation
- stratégies alpha

---
## Schéma visuel

### Vue d’ensemble des pipelines

```mermaid
flowchart TD
    A[data/raw providers] --> B[Raw loaders]
    B --> C[Canonical history pipelines]
    C --> D[Derived feature pipelines]
    D --> E[Research / backtests / datasets]

    A1[yfinance] --> B1[build_prices.py]
    A2[SEC raw index / filings] --> B2[build_sec_filings.py]
    A3[FINRA short interest raw] --> B3[build_finra_short_interest.py]
    A4[FINRA daily short volume raw files] --> B4[load_finra_daily_short_volume_raw.py]

    B1 --> C1[price_history]
    B1 --> C2[price_latest]

    B2 --> C3[sec_filing]

    B3 --> C4[finra_short_interest_source_raw]
    C4 --> C5[finra_short_interest_history]
    C5 --> C6[finra_short_interest_latest]

    B4 --> C7[finra_daily_short_volume_source_raw]
    C7 --> C8[daily_short_volume_history]

    C5 --> D1[build_short_features.py]
    C8 --> D1
    D1 --> D2[short_features_daily]

    C1 --> E1[research joins]
    C3 --> E1
    C5 --> E1
    C8 --> E1
    D2 --> E1

### Schéma des tables principales
flowchart LR
    subgraph Prices
        P1[price_history<br/>canonical historical prices]
        P2[price_latest<br/>serving only]
        P1 --> P2
    end

    subgraph SEC
        S1[sec_filing<br/>normalized SEC filings]
    end

    subgraph FINRA_Short_Interest
        F1[finra_short_interest_source_raw<br/>raw staged source]
        F2[finra_short_interest_history<br/>canonical history]
        F3[finra_short_interest_latest<br/>serving only]
        F1 --> F2 --> F3
    end

    subgraph FINRA_Daily_Short_Volume
        DSV1[finra_daily_short_volume_source_raw<br/>raw staged source]
        DSV2[daily_short_volume_history<br/>canonical history]
        DSV1 --> DSV2
    end

    subgraph Derived
        SF1[short_features_daily<br/>derived research features]
    end

    F2 --> SF1
    DSV2 --> SF1

### Schéma point-in-time safe

flowchart TD
    A[daily_short_volume_history] -->|trade_date + available_at| C[short_features_daily]
    B[finra_short_interest_history] -->|settlement_date + ingested_at| C

    C --> D[as_of_date research joins]

    E[price_history] --> D
    F[sec_filing] --> D

    X[price_latest] -. serving only .-> Z[not for backtests]
    Y[finra_short_interest_latest] -. serving only .-> Z

### Couche d’orchestration quotidienne
flowchart TD
    O1[cli/run_daily_pipeline.py] --> O2[build_prices]
    O1 --> O3[probe FINRA daily short volume]
    O1 --> O4[build_finra_short_interest]
    O1 --> O5[probe short_features]
    O1 --> O6[build_sec_filings]

    O3 -->|if needed| O3A[build_finra_daily_short_volume]
    O3 -->|if aligned| O3B[skip]

    O5 -->|if source changed| O5A[build_short_features]
    O5 -->|if unchanged| O5B[skip]

### Légende
canonical = table historique de référence pour la recherche
serving only = table pratique pour affichage / latest, pas pour backtests
raw staged source = table de staging fidèle à la source brute
derived research features = table calculée à partir des historiques canoniques
### Règles d’utilisation
utiliser price_history pour la recherche, pas price_latest
utiliser finra_short_interest_history pour la recherche, pas finra_short_interest_latest
utiliser daily_short_volume_history comme source canonique short volume
utiliser short_features_daily pour les features dérivées
respecter available_at et ingested_at pour éviter le look-ahead bias
ne pas filtrer l’historique seulement au survivant actuel pour éviter le survivor bias
## 👤 Auteur

astronometria
