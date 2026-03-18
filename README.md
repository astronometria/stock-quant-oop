# 📈 stock-quant-oop

![Python](https://img.shields.io/badge/python-3.10+-blue)
![DuckDB](https://img.shields.io/badge/database-duckdb-orange)
![Status](https://img.shields.io/badge/status-production--ready-green)
![License](https://img.shields.io/badge/license-private-lightgrey)

---

<details>
<summary><strong>🚀 Overview</strong></summary>

Quant research pipeline with:
- SQL-first architecture
- Point-in-time safe data model
- Incremental daily pipelines
- Large-scale FINRA + market data

</details>

---

<details>
<summary><strong>🏗️ Architecture Diagram</strong></summary>

```mermaid
flowchart TD
    A[Data Providers] --> B[Raw Layer]
    B --> C[Canonical Layer]
    C --> D[Feature Layer]
    D --> E[Research Layer]

    A1[yfinance] --> B1[prices raw]
    A2[SEC] --> B2[sec raw]
    A3[FINRA] --> B3[short raw]

    B1 --> C1[price_history]
    B2 --> C2[sec_filing]
    B3 --> C3[short_interest_history]
    B3 --> C4[daily_short_volume_history]

    C3 --> D1[short_features]
    C4 --> D1

    D1 --> E1[signals]
    E1 --> E2[backtests]
```

</details>

---

<details>
<summary><strong>📊 SQL Schema (simplified)</strong></summary>

```sql
CREATE TABLE price_history (
    symbol VARCHAR,
    date DATE,
    close DOUBLE
);

CREATE TABLE sec_filing (
    company_id VARCHAR,
    filing_date DATE,
    form_type VARCHAR
);

CREATE TABLE finra_short_interest_history (
    symbol VARCHAR,
    settlement_date DATE,
    short_interest BIGINT
);

CREATE TABLE daily_short_volume_history (
    symbol VARCHAR,
    trade_date DATE,
    short_volume BIGINT
);

CREATE TABLE short_features_daily (
    symbol VARCHAR,
    as_of_date DATE,
    short_ratio DOUBLE
);
```

</details>

---

<details>
<summary><strong>⚙️ Run Pipeline</strong></summary>

python3 cli/run_daily_pipeline.py

</details>

---

<details>
<summary><strong>⏰ Cron</strong></summary>

0 17 * * * cd /home/marty/stock-quant-oop && python3 cli/run_daily_pipeline.py >> logs/cron.log 2>&1

</details>

---

## 🧠 Principles

- SQL-first transformations
- No look-ahead bias
- No survivor bias
- Canonical history always wins

---

## 👤 Author
astronometria

