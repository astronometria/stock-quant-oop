# Price Pipeline

## Objectif

Le domaine PRICES suit le pattern officiel :

raw
↓
normalized
↓
derived

## Couches

### Raw

Tables raw actuelles :

- price_source_daily_raw
- price_source_daily_raw_all
- price_source_daily_raw_yahoo

Règles :

- append-only
- auditables
- ne jamais servir directement aux backtests

### Normalized

Table canonique :

- price_history

Règles :

- déterministe
- idempotente
- utilisée par les pipelines de recherche
- point d’entrée unique pour features / labels / backtests

### Derived

Table derived :

- price_latest

Règles :

- serving only
- snapshot opérationnel
- debug / inspection / monitoring
- ne jamais être utilisée pour features / labels / datasets / backtests

## Sources

### Rebuild from scratch

- Stooq historical backfill obligatoire

### Daily refresh

- Yahoo Finance via yfinance

## Règles anti-biais

1. Les pipelines de recherche lisent uniquement `price_history`.
2. `price_latest` est interdit pour :
   - features
   - labels
   - datasets
   - backtests
3. Le rebuild complet doit faire :
   - backfill Stooq
   - puis daily Yahoo
4. Les raw prices multi-sources doivent converger vers une seule normalized canonique :
   - `price_history`

## Flux cible

Stooq / Yahoo
↓
price_source_daily_raw_*
↓
price_source_daily_raw_all
↓
price_history
↓
price_latest
