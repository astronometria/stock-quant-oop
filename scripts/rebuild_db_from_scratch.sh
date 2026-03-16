#!/usr/bin/env bash

set -e

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DB_PATH="$PROJECT_ROOT/market.duckdb"
LOG_PATH="$PROJECT_ROOT/logs/rebuild_db.log"

mkdir -p "$PROJECT_ROOT/logs"

{
echo "===== DATE ====="
date

echo "===== PROJECT_ROOT ====="
echo "$PROJECT_ROOT"

echo "===== DB_PATH ====="
echo "$DB_PATH"

echo "===== RUN ORCHESTRATOR ====="

python3 "$PROJECT_ROOT/cli/ops/rebuild_database_from_scratch.py" \
    --db-path "$DB_PATH" \
    --verbose

echo "===== REBUILD DATABASE COMPLETE ====="

} | tee "$LOG_PATH"

echo
echo "LOG WRITTEN TO:"
echo "$LOG_PATH"
