# Daily Price Run

## Fréquence cible

Run quotidien après la fermeture du marché.

Heure opérationnelle visée :

- 17h locale

## Sources

Le scheduler quotidien peut lancer :

- prix Yahoo Finance
- SEC
- FINRA daily short volume
- FINRA short interest

Chaque source peut finir proprement avec un statut `noop` s'il n'y a rien de nouveau.

## Daily prices : comportement attendu

1. détecter la fenêtre effective incremental
2. résoudre le scope canonique
3. appliquer la compatibilité provider Yahoo
4. fetch par batches
5. remapper vers le symbole canonique
6. écrire de façon idempotente
7. journaliser dans `logs/`

## Logs

Exemples de logs attendus dans le repo :

- `logs/build_prices.log`
- `logs/yfinance_batches.log`
- `logs/provider_symbol_mapping.log`

## Monitoring

Surveiller en priorité :

- rate limits
- exclusions provider
- couverture fetchée
- écarts entre scope recherche et scope provider
- anomalies de volumes ou de lignes écrites

## Politique de terminaison

Le run doit :

- réussir si le traitement est propre
- retourner `noop` s'il n'y a rien à faire
- retourner `partial` si une partie du lot échoue
- retourner `failed` si le fetch ou l'écriture échoue de façon bloquante
