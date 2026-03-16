#!/usr/bin/env bash
set -euo pipefail

# ============================================================================
# Rebuild database from scratch
# ----------------------------------------------------------------------------
# Objectif:
# - reconstruire la DB locale à partir des vraies sources raw
# - garder un enchaînement reproductible et versionné
# - produire un log lisible
#
# Hypothèses:
# - exécution depuis le repo stock-quant-oop
# - Python et duckdb CLI disponibles
# - les scripts CLI du repo sont la source de vérité fonctionnelle
#
# Notes:
# - on garde le SQL-first du projet
# - on sépare bien fetch raw / load raw / build core
# - on utilise les fichiers raw téléchargés localement, jamais un proxy prix
# ============================================================================

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DB_PATH="${DB_PATH:-$PROJECT_ROOT/market.duckdb}"
LOG_PATH="${LOG_PATH:-$HOME/log.txt}"
RUN_DATE_UTC="$(date -u +%F)"
PYTHON_BIN="${PYTHON_BIN:-python3}"

# ----------------------------------------------------------------------------
# Redirige toute la sortie vers le terminal + log fichier.
# ----------------------------------------------------------------------------
mkdir -p "$(dirname "$LOG_PATH")"
exec > >(tee "$LOG_PATH") 2>&1

echo "===== DATE ====="
date

echo "===== PROJECT_ROOT ====="
echo "$PROJECT_ROOT"

echo "===== DB_PATH ====="
echo "$DB_PATH"

echo "===== LOG_PATH ====="
echo "$LOG_PATH"

cd "$PROJECT_ROOT"

# ----------------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------------
run_step() {
  local step_name="$1"
  shift

  echo
  echo "===== STEP: ${step_name} ====="
  echo "COMMAND: $*"
  "$@"
}

assert_file_exists() {
  local file_path="$1"
  if [ ! -f "$file_path" ]; then
    echo "ERROR: required file not found: $file_path" >&2
    exit 1
  fi
}

get_latest_file() {
  local pattern="$1"
  local latest
  latest="$(find . -path "$pattern" -type f | sort | tail -n 1 || true)"
  if [ -z "$latest" ]; then
    echo ""
    return 0
  fi
  echo "$latest"
}

# ----------------------------------------------------------------------------
# 0) Préflight
# ----------------------------------------------------------------------------
run_step "PYTHON VERSION" "$PYTHON_BIN" --version
run_step "GIT BRANCH" git branch --show-current
run_step "GIT STATUS" git status --short || true

# ----------------------------------------------------------------------------
# 1) Initialiser / valider le schéma de base
# ----------------------------------------------------------------------------
# Si le script d'init existe, on le lance. Cela permet de repartir proprement.
# On garde --db-path explicite.
# ----------------------------------------------------------------------------
if [ -f "cli/core/init_market_db.py" ]; then
  run_step "INIT MARKET DB" \
    "$PYTHON_BIN" cli/core/init_market_db.py --db-path "$DB_PATH"
else
  echo "WARNING: cli/core/init_market_db.py not found, skipping init step"
fi

# ----------------------------------------------------------------------------
# 2) Télécharger les vraies sources symboles sur disque local
# ----------------------------------------------------------------------------
run_step "FETCH SEC COMPANY TICKERS RAW" \
  "$PYTHON_BIN" cli/raw/fetch_sec_company_tickers_raw.py --verbose

run_step "FETCH NASDAQ SYMBOL DIRECTORY RAW" \
  "$PYTHON_BIN" cli/raw/fetch_nasdaq_symbol_directory_raw.py --verbose

# ----------------------------------------------------------------------------
# 3) Résoudre les fichiers raw les plus récents
# ----------------------------------------------------------------------------
SEC_FILE="$(get_latest_file "./data/symbol_sources/sec/sec_company_tickers_*.csv")"
NASDAQ_LISTED_FILE="$(get_latest_file "./data/symbol_sources/nasdaq/nasdaqlisted_*.csv")"
OTHER_LISTED_FILE="$(get_latest_file "./data/symbol_sources/nasdaq/otherlisted_*.csv")"

echo
echo "===== RESOLVED RAW FILES ====="
echo "SEC_FILE=$SEC_FILE"
echo "NASDAQ_LISTED_FILE=$NASDAQ_LISTED_FILE"
echo "OTHER_LISTED_FILE=$OTHER_LISTED_FILE"

if [ -z "$SEC_FILE" ] || [ -z "$NASDAQ_LISTED_FILE" ] || [ -z "$OTHER_LISTED_FILE" ]; then
  echo "ERROR: one or more symbol source files are missing" >&2
  exit 1
fi

assert_file_exists "$SEC_FILE"
assert_file_exists "$NASDAQ_LISTED_FILE"
assert_file_exists "$OTHER_LISTED_FILE"

# ----------------------------------------------------------------------------
# 4) Charger les sources symboles dans la staging raw
# ----------------------------------------------------------------------------
run_step "LOAD SYMBOL_REFERENCE_SOURCE_RAW" \
  "$PYTHON_BIN" cli/raw/load_symbol_reference_source_raw.py \
    --db-path "$DB_PATH" \
    --truncate \
    --source "$NASDAQ_LISTED_FILE" \
    --source "$OTHER_LISTED_FILE" \
    --source "$SEC_FILE" \
    --verbose

# ----------------------------------------------------------------------------
# 5) Construire le coeur symboles / univers
# ----------------------------------------------------------------------------
run_step "BUILD MARKET UNIVERSE" \
  "$PYTHON_BIN" cli/core/build_market_universe.py --db-path "$DB_PATH" --verbose

run_step "BUILD SYMBOL REFERENCE" \
  "$PYTHON_BIN" cli/core/build_symbol_reference.py --db-path "$DB_PATH" --verbose

# ----------------------------------------------------------------------------
# 6) Construire SEC filings si le raw index existe déjà
# ----------------------------------------------------------------------------
if duckdb "$DB_PATH" "SELECT COUNT(*) FROM sec_filing_raw_index;" >/dev/null 2>&1; then
  run_step "BUILD SEC FILINGS" \
    "$PYTHON_BIN" cli/core/build_sec_filings.py --db-path "$DB_PATH" --verbose || true
else
  echo
  echo "===== SKIP BUILD SEC FILINGS ====="
  echo "Reason: sec_filing_raw_index table missing or unreadable"
fi

# ----------------------------------------------------------------------------
# 7) Résumé de sanity check
# ----------------------------------------------------------------------------
echo
echo "===== FINAL COUNTS ====="
duckdb "$DB_PATH" <<'SQL'
SELECT 'symbol_reference_source_raw' AS table_name, COUNT(*) AS row_count FROM symbol_reference_source_raw
UNION ALL
SELECT 'market_universe' AS table_name, COUNT(*) AS row_count FROM market_universe
UNION ALL
SELECT 'market_universe_included' AS table_name, COUNT(*) AS row_count FROM market_universe WHERE include_in_universe = TRUE
UNION ALL
SELECT 'symbol_reference' AS table_name, COUNT(*) AS row_count FROM symbol_reference
UNION ALL
SELECT 'sec_filing_raw_index' AS table_name, COUNT(*) AS row_count FROM sec_filing_raw_index
UNION ALL
SELECT 'sec_filing' AS table_name, COUNT(*) AS row_count FROM sec_filing;
SQL

echo
echo "===== SAMPLE SYMBOL_REFERENCE ====="
duckdb "$DB_PATH" <<'SQL'
SELECT
  symbol,
  cik,
  company_name,
  exchange,
  source_name,
  symbol_match_enabled,
  name_match_enabled
FROM symbol_reference
LIMIT 20;
SQL

echo
echo "===== SAMPLE MARKET_UNIVERSE INCLUDED ====="
duckdb "$DB_PATH" <<'SQL'
SELECT
  symbol,
  company_name,
  exchange_normalized,
  security_type,
  include_in_universe,
  exclusion_reason
FROM market_universe
WHERE include_in_universe = TRUE
LIMIT 20;
SQL

echo
echo "===== REBUILD COMPLETE ====="
echo "DB_PATH=$DB_PATH"
echo "LOG_PATH=$LOG_PATH"
