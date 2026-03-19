# DB Rebuild

## Objectif

Reconstruire `market.duckdb` depuis zéro avec des sources réelles stockées localement.

## Sources de vérité

- `scripts/rebuild_db_from_scratch.sh` = vérité exécutable
- `docs/DB_REBUILD.md` = vérité humaine
- `cli/ops/rebuild_database_from_scratch.py` = vérité produit long terme

## Commande principale

Depuis la racine du repo :

STOOQ_SOURCE=/path/to/stooq.zip ./scripts/rebuild_db_from_scratch.sh

## Ce que fait la reconstruction

1. initialise le schéma
2. télécharge les sources symboles SEC
3. télécharge les sources symboles NASDAQ
4. charge `symbol_reference_source_raw`
5. construit `market_universe`
6. construit `symbol_reference`
7. construit `sec_filing`
8. construit `sec_fact_normalized`
9. construit `fundamentals`
10. construit les prix historiques via Stooq
11. applique un refresh daily des prix via Yahoo Finance
12. exécute le refresh quotidien FINRA

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

- data/symbol_sources/sec/

NASDAQ :

- data/symbol_sources/nasdaq/

Prix historiques Stooq :

- sources explicites passées à `--stooq-source`

FINRA :

- data/raw/finra/short_interest/

## Tables coeur attendues

- `symbol_reference_source_raw`
- `market_universe`
- `symbol_reference`
- `sec_filing`
- `sec_fact_normalized`
- `fundamental_snapshot_quarterly`
- `fundamental_snapshot_annual`
- `fundamental_ttm`
- `fundamental_features_daily`
- `price_history`
- `price_latest`
- `finra_short_interest_source_raw`
- `finra_short_interest_history`
- `finra_short_interest_latest`
- `short_features_daily`

## Vérification rapide

Le rebuild est sain si tu obtiens au minimum :

- `symbol_reference_source_raw` peuplée
- `market_universe` peuplée
- `market_universe` avec des lignes `include_in_universe = TRUE`
- `symbol_reference` peuplée
- `sec_filing` reconstruite
- `sec_fact_normalized` peuplée si `sec_xbrl_fact_raw` est disponible
- tables fondamentales peuplées si `sec_fact_normalized` est disponible
- `price_history` et `price_latest` peuplées après le backfill Stooq puis le daily Yahoo
- tables FINRA peuplées si des fichiers FINRA exploitables sont disponibles

## Rappels critiques anti-biais

- Fundamentals : utiliser `available_at`, jamais `period_end_date` seul pour la disponibilité marché
- Prices : les pipelines de recherche ne doivent jamais lire `price_latest`
- FINRA : utiliser la date de disponibilité/publication, pas seulement la date économique
- Universe : le rebuild du projet reste ADR-free

## Commande produit long terme

Pour lancer directement l’orchestrateur :

python3 cli/ops/rebuild_database_from_scratch.py --db-path market.duckdb --stooq-source /path/to/stooq.zip --verbose

## Variantes utiles

Rebuild sans fondamentaux :

python3 cli/ops/rebuild_database_from_scratch.py --db-path market.duckdb --stooq-source /path/to/stooq.zip --skip-fundamentals --verbose

Rebuild en réutilisant déjà les raw symboles téléchargés :

python3 cli/ops/rebuild_database_from_scratch.py --db-path market.duckdb --stooq-source /path/to/stooq.zip --skip-sec-fetch --skip-nasdaq-fetch --verbose

Rebuild sans refresh Yahoo quotidien :

python3 cli/ops/rebuild_database_from_scratch.py --db-path market.duckdb --stooq-source /path/to/stooq.zip --skip-price-daily --verbose

Rebuild sans FINRA :

python3 cli/ops/rebuild_database_from_scratch.py --db-path market.duckdb --stooq-source /path/to/stooq.zip --skip-finra --verbose

Fenêtre FINRA explicite :

python3 cli/ops/rebuild_database_from_scratch.py --db-path market.duckdb --stooq-source /path/to/stooq.zip --finra-start-date 2026-01-01 --finra-end-date 2026-03-16 --verbose

Attention : sauter le backfill Stooq n'est pas un vrai from-scratch rebuild complet.
