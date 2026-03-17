# Price Pipeline V2

## Vue d'ensemble

Flux cible du pipeline prix :

1. `market_universe`
2. résolution du scope canonique de recherche
3. compatibilité provider Yahoo
4. batching provider
5. fetch brut
6. remap vers symbole canonique
7. écriture dans `price_history`
8. dérivation éventuelle vers `price_latest`

## Étapes

### 1. Scope de recherche

Source principale :

- `market_universe`
- `include_in_universe = TRUE`
- date `as_of_date` la plus récente applicable

### 2. Compatibilité provider

Le pipeline sépare :

- symbole canonique interne
- symbole Yahoo compatible

### 3. Fetch

Le fetch Yahoo doit être :

- batché
- journalisé
- résilient aux erreurs transitoires
- strict sur les exclusions permanentes

### 4. Normalisation

Après le fetch, le pipeline doit revenir au symbole canonique interne avant toute écriture.

### 5. Storage

#### `price_history`

- table canonique
- clé logique : `(symbol, price_date)`
- écriture idempotente
- jamais de symbole Yahoo persistant

#### `price_latest`

- vue ou table dérivée de serving
- jamais utilisée pour la logique de refresh recherche

## Règles critiques

- ne jamais écrire un symbole provider dans `price_history`
- ne jamais utiliser `price_latest` pour piloter la couverture recherche
- conserver la séparation recherche / serving
- garder la logique point-in-time cohérente avec le README

## Métriques utiles

- `research_scope_symbol_count`
- `provider_candidate_symbol_count`
- `provider_excluded_symbol_count`
- `mapping_applied_count`
- `rows_fetched`
- `rows_written`
- `rate_limit_count`
