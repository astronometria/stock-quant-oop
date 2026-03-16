# DB Rebuild

## Objectif

Reconstruire `market.duckdb` depuis zéro avec des sources réelles stockées localement.

## Sources de vérité

- `scripts/rebuild_db_from_scratch.sh` = vérité exécutable
- `docs/DB_REBUILD.md` = vérité humaine
- `cli/ops/rebuild_database_from_scratch.py` = vérité produit long terme

## Commande principale

Depuis la racine du repo :

./scripts/rebuild_db_from_scratch.sh

## Ce que fait la reconstruction

1. initialise le schéma
2. télécharge les sources symboles SEC
3. télécharge les sources symboles NASDAQ
4. charge `symbol_reference_source_raw`
5. construit `market_universe`
6. construit `symbol_reference`
7. construit `sec_filing` si le raw SEC est déjà présent

## Univers cible

Actions US standard seulement :

- NASDAQ
- NYSE

Exclusions :

- OTC
- Pink Sheet
- ETF
- ETN
- ADR
- Preferred
- Warrant
- Right
- Unit

## Fichiers raw locaux

SEC :

data/symbol_sources/sec/

NASDAQ :

data/symbol_sources/nasdaq/

## Tables coeur attendues

- `symbol_reference_source_raw`
- `market_universe`
- `symbol_reference`
- `sec_filing`

## Vérification rapide

Le rebuild est sain si tu obtiens au minimum :

- `symbol_reference_source_raw` peuplée
- `market_universe` peuplée
- `market_universe` avec des lignes `include_in_universe = TRUE`
- `symbol_reference` peuplée
- `sec_filing` reconstruite si `sec_filing_raw_index` existe

## Commande produit long terme

Pour lancer directement l’orchestrateur :

python3 cli/ops/rebuild_database_from_scratch.py --db-path market.duckdb --verbose
