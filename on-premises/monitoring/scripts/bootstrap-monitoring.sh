#!/usr/bin/env bash
# ============================================================================
# BOOTSTRAP MONITORING USERS / FUNCTIONS
# ============================================================================
# Mục tiêu:
# - Tạo monitoring users trong MySQL Source, PostgreSQL Target, Dagster Metadata.
# - Tạo monitoring functions phục vụ postgres_exporter.
# - Chạy idempotent ở mức hợp lý cho lab.
#
# Production note:
# - Không nên inline secrets trong script thật.
# - Nên dùng secret manager, vault, hoặc CI/CD secret.
# - Script này ưu tiên dễ học và chạy được trong lab.
# ============================================================================

set -Eeuo pipefail

# Luôn chạy từ root của repository.
cd "$(dirname "$0")/../.."

if [[ ! -f .env ]]; then
  echo "❌ .env file not found. Please create it from .env.example."
  exit 1
fi

# Load biến môi trường từ .env
set -a
source .env
set +a

# Fail-fast nếu thiếu biến quan trọng.
: "${MYSQL_ROOT_PASSWORD:?MYSQL_ROOT_PASSWORD is required}"
: "${POSTGRES_USER:?POSTGRES_USER is required}"
: "${POSTGRES_DB:?POSTGRES_DB is required}"
: "${DAGSTER_METADATA_USER:?DAGSTER_METADATA_USER is required}"
: "${DAGSTER_METADATA_DB:?DAGSTER_METADATA_DB is required}"
: "${MONITORING_MYSQL_PASSWORD:?MONITORING_MYSQL_PASSWORD is required}"
: "${MONITORING_POSTGRES_TARGET_PASSWORD:?MONITORING_POSTGRES_TARGET_PASSWORD is required}"
: "${MONITORING_POSTGRES_METADATA_PASSWORD:?MONITORING_POSTGRES_METADATA_PASSWORD is required}"

echo "============================================================"
echo "[1/3] Creating MySQL monitoring user"
echo "============================================================"

# Dùng sed để inject password.
# Lab note: không nên dùng password chứa ký tự đặc biệt phức tạp nếu dùng sed.
sed "s|__MONITORING_MYSQL_PASSWORD__|${MONITORING_MYSQL_PASSWORD}|g" \
  monitoring/sql/mysql_monitoring_user.sql \
  | docker compose exec -T mysql-source mysql -uroot -p"${MYSQL_ROOT_PASSWORD}"

echo "============================================================"
echo "[2/3] Creating PostgreSQL target monitoring role/functions"
echo "============================================================"

sed "s|__MONITORING_POSTGRES_TARGET_PASSWORD__|${MONITORING_POSTGRES_TARGET_PASSWORD}|g" \
  monitoring/sql/postgres_target_monitoring.sql \
  | docker compose exec -T postgres-target psql \
      -v ON_ERROR_STOP=1 \
      -U "${POSTGRES_USER}" \
      -d "${POSTGRES_DB}"

echo "============================================================"
echo "[3/3] Creating Dagster metadata monitoring role/functions"
echo "============================================================"

sed "s|__MONITORING_POSTGRES_METADATA_PASSWORD__|${MONITORING_POSTGRES_METADATA_PASSWORD}|g" \
  monitoring/sql/postgres_metadata_monitoring.sql \
  | docker compose exec -T dagster-metadata psql \
      -v ON_ERROR_STOP=1 \
      -U "${DAGSTER_METADATA_USER}" \
      -d "${DAGSTER_METADATA_DB}"

echo "✅ Monitoring bootstrap completed."