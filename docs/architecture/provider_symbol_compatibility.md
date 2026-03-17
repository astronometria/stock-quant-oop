# Provider Symbol Compatibility (Yahoo Finance)

## Objectif

Séparer clairement :

- le symbole canonique de recherche
- le symbole provider Yahoo Finance utilisé seulement pour le fetch

Cela évite :

- la corruption des symboles canoniques
- les erreurs massives de fetch
- les faux diagnostics de couverture
- les problèmes de rate limit

## Principe clé

Un symbole canonique interne n'est pas forcément un symbole Yahoo valide.

Exemples fréquents :

| Canonical | Yahoo |
|---|---|
| BRK.B | BRK-B |
| BF.A | BF-A |
| CWEN.A | CWEN-A |
| CRD.A | CRD-A |

## Architecture cible

Flux logique :

1. `market_universe` définit le scope de recherche attendu
2. `price_provider_symbol_service` résout la compatibilité provider
3. `yfinance_symbol_mapping_policy` applique les règles Yahoo
4. `yfinance_price_provider` fetch les données avec les symboles provider
5. les résultats sont remappés vers le symbole canonique avant écriture

## Responsabilités

### `price_provider_symbol_service`

Produit, pour chaque symbole canonique :

- `canonical_symbol`
- `provider_name`
- `provider_symbol`
- `is_fetchable`
- `exclusion_reason`

### `yfinance_symbol_mapping_policy`

Applique des règles explicites et testables :

- mapping ciblé des classes d'actions
- exclusion des symboles non supportés
- normalisation minimale, sans altérer le canonique interne

## Exclusions probables

Les catégories suivantes doivent généralement être exclues du fetch Yahoo standard :

- rights
- warrants
- units
- preferreds complexes
- symboles avec syntaxe Yahoo instable
- symboles sans timezone ou sans quote valide

## Règles critiques

- `price_history` doit toujours stocker le symbole canonique
- le symbole Yahoo est temporaire et utilisé uniquement pendant le fetch
- `price_latest` ne doit jamais servir à décider la logique de refresh recherche
