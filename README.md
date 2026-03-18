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

## 👤 Auteur

astronometria
