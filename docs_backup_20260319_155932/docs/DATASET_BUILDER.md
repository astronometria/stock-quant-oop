# Training Dataset Builder

Objectif :

Construire un dataset quant point-in-time.

Pipeline :

normalized
↓
point-in-time filter
↓
training_dataset_daily

---

Sources :

price_history
fundamental_features_daily
short_features_daily
market_universe

---

Règles anti-biais

1. utiliser seulement price_history
2. ne jamais utiliser price_latest
3. fundamentals filtrés sur available_at
4. short interest filtré sur available_at
5. symbol doit être dans market_universe

---

Output

training_dataset_daily
