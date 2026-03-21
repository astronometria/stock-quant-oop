# stock-quant-oop

Pipeline quant orienté objet avec approche SQL-first pour reconstruire et mettre à jour une base DuckDB contenant :
- les prix historiques
- les données short FINRA
- les données SEC
- les features de recherche quotidiennes

## Hypothèses d'exploitation

- OS cible : Ubuntu 22.04
- Base locale : `market.duckdb`
- Les données Stooq sont déjà présentes sur la machine pour un rebuild complet
- Les mises à jour quotidiennes utilisent les pipelines incrémentaux
- Les logs d'exploitation sont écrits dans `logs/`

## Rebuild complet sur une nouvelle machine

Le rebuild complet suppose que l'historique Stooq est déjà disponible localement. Le flow visé est :

1. initialiser la base
2. charger les sources bulk / locales
3. construire les tables canoniques
4. reconstruire les tables dérivées et de recherche

Commande type :

```bash
cd ~/stock-quant-oop && python3 cli/ops/rebuild_db_from_scratch.py \
  --db-path /home/marty/stock-quant-oop/market.duckdb \
  | tee ~/log.txt
```

## Mise à jour incrémentale manuelle

Le script manuel canonique pour un refresh complet incrémental est :

```bash
cd ~/stock-quant-oop && python3 cli/ops/run_manual_incremental_data_refresh.py \
  --db-path /home/marty/stock-quant-oop/market.duckdb \
  | tee ~/log.txt
```

Ce script enchaîne les mises à jour incrémentales de :
- price
- FINRA
- SEC
- `research_features_daily`

## État validé du pipeline

À l'issue du refresh manuel validé :
- `price_history` est à jour jusqu'au `2026-03-20`
- `daily_short_volume_history` est à jour jusqu'au `2026-03-20`
- `short_features_daily` est à jour jusqu'au `2026-03-20`
- `sec_filing` est à jour jusqu'au `2026-03-18`
- `research_features_daily` est à jour jusqu'au `2026-03-20`

## Prix : rebuild vs incrémental

### Rebuild
- source principale : Stooq local
- usage : reconstruction complète d'une nouvelle machine

### Incrémental
- source principale : Yahoo Finance
- usage : rafraîchissement quotidien
- le refresh est maintenant plus sélectif :
  - ajusté au week-end
  - réduit aux symboles réellement en retard

## FINRA

Le pipeline FINRA suit maintenant un flow incrémental propre :
- raw sur disque
- chargement raw en base
- construction de `daily_short_volume_history`
- construction de `short_features_daily`

## SEC

Le pipeline SEC suit maintenant un flow incrémental chunké avec progression visible :
- `build_sec_filings.py`
- `build_sec_fact_normalized.py`

Le watermark de `sec_fact_normalized` suit la vraie source de vérité :
- `MAX(sec_xbrl_fact_raw.ingested_at)`

La progression est affichée par chunks mensuels pour éviter les runs opaques.

## Cron

L'ancien cron utilisateur qui lançait `cli/run_daily_pipeline.py` a été désactivé pour éviter les conflits de lock DuckDB avec les runs manuels.

## Vérification rapide après refresh

```bash
cd ~/stock-quant-oop && {
  echo "===== DATE ====="
  date

  python3 <<'PY'
import duckdb

con = duckdb.connect("/home/marty/stock-quant-oop/market.duckdb", read_only=True)
try:
    checks = [
        ("price_history", "SELECT COUNT(*), MIN(price_date), MAX(price_date) FROM price_history"),
        ("daily_short_volume_history", "SELECT COUNT(*), MIN(trade_date), MAX(trade_date) FROM daily_short_volume_history"),
        ("short_features_daily", "SELECT COUNT(*), MIN(as_of_date), MAX(as_of_date) FROM short_features_daily"),
        ("sec_filing", "SELECT COUNT(*), MIN(filing_date), MAX(filing_date) FROM sec_filing"),
        ("sec_fact_normalized", "SELECT COUNT(*), MIN(period_end_date), MAX(period_end_date) FROM sec_fact_normalized"),
        ("research_features_daily", "SELECT COUNT(*), MIN(as_of_date), MAX(as_of_date) FROM research_features_daily"),
    ]
    for label, sql in checks:
        print(label, "=", con.execute(sql).fetchone())
finally:
    con.close()
PY
} | tee ~/log.txt
```

## Notes de dev

- privilégier SQL-first pour les transformations
- garder Python mince pour l'orchestration
- conserver les anciens scripts en `.bak`
- utiliser des logs dans `logs/`
- conserver une progression visible (`tqdm`) sur les longs jobs
