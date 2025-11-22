#!/usr/bin/env bash
set -euo pipefail

echo "Resetting MLflow database to fix migration issues..."
PGPASSWORD="${MLFLOW_DB_PASSWORD:-mlflow}"
DB_NAME="${MLFLOW_DB_NAME:-mlflow}"
DB_USER="${MLFLOW_DB_USER:-mlflow}"

export PGPASSWORD
export DB_NAME
export DB_USER

# Try to drop/create DB (ignore failures)
psql -h postgres -U "$DB_USER" -d postgres -c "DROP DATABASE IF EXISTS \"$DB_NAME\";" 2>/dev/null || true
psql -h postgres -U "$DB_USER" -d postgres -c "CREATE DATABASE \"$DB_NAME\";" 2>/dev/null || true

echo "Starting MLflow server..."
exec mlflow server \
  --host 0.0.0.0 --port 5500 \
  --backend-store-uri "${MLFLOW_BACKEND_STORE_URI}" \
  --default-artifact-root "${MLFLOW_ARTIFACTS_DESTINATION}" \
  --allowed-hosts '*' \
  --cors-allowed-origins '*'
