# YFinance Batching Strategy

## Objectif

Réduire :

- les erreurs HTTP 429
- les timeouts
- les gros lots instables
- le bruit opérationnel dans les runs quotidiens

## Stratégie

Le provider Yahoo ne doit pas recevoir tout le scope d'un coup.

Il faut batcher explicitement les symboles avec des paramètres configurables :

- `batch_size`
- `sleep_seconds_between_batches`
- `max_retries`
- `retry_backoff_seconds`

## Valeurs de départ recommandées

Point de départ prudent :

- batch size : 100 à 250
- pause entre batches : 0.5 à 2 secondes
- retries : faibles et ciblés

## Retry policy

Retry seulement pour les erreurs transitoires :

- rate limit
- timeout
- erreur réseau passagère
- erreur HTTP temporaire

Ne pas retry agressivement pour :

- symbole introuvable
- symbole incompatible
- quote vide permanente
- erreur de mapping

## Classification des erreurs

### Erreurs permanentes

- symbol not found
- possibly delisted
- no timezone found
- mapping invalide
- instrument non éligible

### Erreurs transitoires

- Too Many Requests
- timeout
- connexion interrompue
- erreur serveur passagère

## Logs attendus

Pour chaque batch :

- `batch_index`
- `batch_size`
- `success_count`
- `failure_count`
- `retry_count`
- `duration_seconds`

Pour la run globale :

- `provider_candidate_symbol_count`
- `provider_excluded_symbol_count`
- `provider_fetched_symbol_count`
- `provider_failed_symbol_count`
- `rate_limit_count`

## But final

Obtenir un fetch stable, progressif, auditable, et compatible avec un job quotidien post-close.
