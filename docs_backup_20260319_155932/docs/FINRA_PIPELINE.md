# FINRA Pipeline

Le domaine FINRA suit le design pattern officiel :

raw
↓
normalized
↓
derived

---

# Raw tables

Sources brutes FINRA.

Tables :

finra_short_interest_source_raw
finra_daily_short_volume_source_raw

Règles :

append-only
auditables
jamais utilisées directement pour les features

---

# Normalized

Table canonique :

finra_short_interest_history

Colonnes critiques :

symbol
as_of_date
short_interest
short_volume
available_at

Règle anti-biais :

as_of_date = date économique
available_at = date de publication FINRA

Les pipelines downstream doivent utiliser :

available_at

---

# Derived

Tables derived :

finra_short_interest_latest
short_features_daily

---

# Look-ahead bias rule

Ne jamais utiliser seulement :

as_of_date

Toujours utiliser :

available_at
