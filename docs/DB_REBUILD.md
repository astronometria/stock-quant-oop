# DB Rebuild

## Objectif

Ce document décrit la reconstruction complète de la base `market.duckdb`.

Script principal :

scripts/rebuild_db_from_scratch.sh

---

## Philosophie

Le projet utilise trois niveaux de vérité :

script shell = vérité exécutable  
doc markdown = vérité humaine  
orchestrateur Python = vérité produit long terme

Principes :

- SQL-first
- sources réelles uniquement
- snapshots locaux des fichiers raw
- univers US standard uniquement

Exchanges autorisés :

NASDAQ  
NYSE

Exclusions :

OTC  
Pink Sheet  
ETF  
ETN  
ADR  
Preferred  
Warrant  
Right  
Unit

---

## Script principal

Pour reconstruire la base complète :

./scripts/rebuild_db_from_scratch.sh

---

## Étapes exécutées

1. Initialiser le schéma

python3 cli/core/init_market_db.py --db-path market.duckdb

2. Télécharger les sources symboles

SEC :

python3 cli/raw/fetch_sec_company_tickers_raw.py

NASDAQ :

python3 cli/raw/fetch_nasdaq_symbol_directory_raw.py

3. Charger les sources raw

python3 cli/raw/load_symbol_reference_source_raw.py

Tables utilisées :

symbol_reference_source_raw

---

4. Construire l'univers marché

python3 cli/core/build_market_universe.py

Table :

market_universe

---

5. Construire le référentiel symboles

python3 cli/core/build_symbol_reference.py

Table :

symbol_reference

---

6. Construire les filings SEC

python3 cli/core/build_sec_filings.py

Table :

sec_filing

---

## Vérifications rapides

Compteurs :

SELECT COUNT(*) FROM symbol_reference_source_raw;

SELECT COUNT(*) FROM market_universe;

SELECT COUNT(*) FROM symbol_reference;

SELECT COUNT(*) FROM sec_filing;

---

## Résultat attendu

Après reconstruction :

symbol_reference_source_raw ≈ 22k  
market_universe ≈ 15k  
market_universe_included ≈ 6k  
symbol_reference ≈ 6k  
sec_filing ≈ 44k

---

## Commande recommandée

./scripts/rebuild_db_from_scratch.sh

Cette commande est la source de vérité exécutable.
