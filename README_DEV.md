# README_DEV — stock-quant-oop

## But

Ce document résume les conventions de développement et d'exploitation retenues après stabilisation des pipelines incrémentaux.

## Conventions générales

- Architecture : OOP + SQL-first
- Transformations volumineuses : DuckDB / SQL en priorité
- Python : orchestration, providers externes, contrôle de flux, retry
- Logs : `logs/`
- Scripts longs : progression visible avec `tqdm`
- Anciennes versions : suffixe `.bak`

## Flux recommandés

### Rebuild from scratch

À utiliser sur une nouvelle machine quand Stooq est déjà présent localement.

Commande cible :

```bash
cd ~/stock-quant-oop && python3 cli/ops/rebuild_db_from_scratch.py \
  --db-path /home/marty/stock-quant-oop/market.duckdb \
  | tee ~/log.txt
```

### Refresh incrémental manuel

Commande canonique :

```bash
cd ~/stock-quant-oop && python3 cli/ops/run_manual_incremental_data_refresh.py \
  --db-path /home/marty/stock-quant-oop/market.duckdb \
  | tee ~/log.txt
```

## Price pipeline

### Décisions retenues

- Rebuild complet : Stooq local
- Incrémental : Yahoo Finance
- Ajustement week-end dans `PriceRefreshWindowService`
- Réduction du scope aux symboles réellement en retard
- Provider Yahoo rendu plus conservateur face au rate limit

### Risques connus

- certains symboles spéciaux restent mal couverts par Yahoo
- warrants / units / preferred / syntaxes exotiques peuvent être exclus
- la notion de `last_complete_date` peut encore être sévère selon le scope historique

## FINRA pipeline

### État retenu

- raw sur disque validé
- chargement raw DuckDB validé
- `daily_short_volume_history` incrémental validé
- `short_features_daily` incrémental validé

### Notes pratiques

- certaines archives historiques contiennent seulement une ligne `0`
- ces fichiers doivent être traités comme payloads vides
- conserver la progression visible pendant les loads bulk

## SEC pipeline

### État retenu

- `build_sec_filings.py` chunké et incrémental
- `build_sec_fact_normalized.py` chunké par mois et incrémental
- le watermark facts doit suivre `sec_xbrl_fact_raw.ingested_at`
- ne pas utiliser `sec_filing.created_at` comme signal incrémental

### Propriété importante

Le pipeline SEC est maintenant :
- chunké
- visible
- réellement incrémental

## Cron

L'ancien cron utilisateur lançant `cli/run_daily_pipeline.py` a été commenté pour éviter :
- les runs concurrents
- les conflits de lock DuckDB
- les diagnostics confus

## Débogage recommandé

### Vérifier un lock DuckDB

```bash
lsof /home/marty/stock-quant-oop/market.duckdb
```

### Vérifier l'état final des tables

```bash
cd ~/stock-quant-oop && {
  echo "===== DATE ====="
  date
  python3 <<'PY'
import duckdb
con = duckdb.connect('/home/marty/stock-quant-oop/market.duckdb', read_only=True)
try:
    for table, sql in [
        ('price_history', 'SELECT COUNT(*), MIN(price_date), MAX(price_date) FROM price_history'),
        ('daily_short_volume_history', 'SELECT COUNT(*), MIN(trade_date), MAX(trade_date) FROM daily_short_volume_history'),
        ('short_features_daily', 'SELECT COUNT(*), MIN(as_of_date), MAX(as_of_date) FROM short_features_daily'),
        ('sec_filing', 'SELECT COUNT(*), MIN(filing_date), MAX(filing_date) FROM sec_filing'),
        ('sec_fact_normalized', 'SELECT COUNT(*), MIN(period_end_date), MAX(period_end_date) FROM sec_fact_normalized'),
        ('research_features_daily', 'SELECT COUNT(*), MIN(as_of_date), MAX(as_of_date) FROM research_features_daily'),
    ]:
        print(table, '=', con.execute(sql).fetchone())
finally:
    con.close()
PY
} | tee ~/log.txt
```

## Fichiers importants

- `cli/ops/run_manual_incremental_data_refresh.py`
- `cli/ops/rebuild_db_from_scratch.py`
- `cli/core/build_prices.py`
- `cli/core/build_sec_filings.py`
- `cli/core/build_sec_fact_normalized.py`
- `cli/core/build_finra_daily_short_volume.py`
- `cli/core/build_short_features.py`

## Règle d'exploitation

Pour les gros jobs :
- séparer l'écriture du script et son exécution
- garder les prompts de troubleshooting en un seul bloc copiable
- éviter les runs concurrents sur la même DB
