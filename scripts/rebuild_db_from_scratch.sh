#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DB_PATH="${DB_PATH:-$PROJECT_ROOT/market.duckdb}"
LOG_PATH="${LOG_PATH:-$HOME/log.txt}"
PYTHON_BIN="${PYTHON_BIN:-python3}"

mkdir -p "$(dirname "$LOG_PATH")"

cd "$PROJECT_ROOT"

exec > >(tee "$LOG_PATH") 2>&1

echo "===== DATE ====="
date
echo "===== PROJECT_ROOT ====="
echo "$PROJECT_ROOT"
echo "===== DB_PATH ====="
echo "$DB_PATH"
echo "===== LOG_PATH ====="
echo "$LOG_PATH"

echo "===== RUN ORCHESTRATOR ====="
"$PYTHON_BIN" cli/ops/rebuild_database_from_scratch.py \
  --db-path "$DB_PATH" \
  --verbose

echo "===== DONE ====="
