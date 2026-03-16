#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DB_PATH="${DB_PATH:-$PROJECT_ROOT/market.duckdb}"
LOG_PATH="${LOG_PATH:-$PROJECT_ROOT/logs/rebuild_db.log}"
PYTHON_BIN="${PYTHON_BIN:-python3}"

# IMPORTANT:
# - Stooq backfill is part of the rebuild by design.
# - Pass STOOQ historical source(s) via STOOQ_SOURCE env var, repeatable through shell arrays is not possible
#   here, so we support a single env var containing one path.
# - For multiple sources, call the Python orchestrator directly.

mkdir -p "$PROJECT_ROOT/logs"

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

if [ -z "${STOOQ_SOURCE:-}" ]; then
  echo "ERROR: STOOQ_SOURCE is required for rebuild_db_from_scratch.sh" >&2
  echo "Example:" >&2
  echo "  STOOQ_SOURCE=/path/to/stooq.zip ./scripts/rebuild_db_from_scratch.sh" >&2
  exit 1
fi

echo "===== RUN ORCHESTRATOR ====="
"$PYTHON_BIN" cli/ops/rebuild_database_from_scratch.py \
  --db-path "$DB_PATH" \
  --stooq-source "$STOOQ_SOURCE" \
  --verbose

echo "===== DONE ====="
