#!/bin/sh
# =====================================================================
# MinIO Bucket Initialization Script
# =====================================================================
# Runs inside the minio/mc container. Waits for MinIO to be reachable,
# configures the mc client alias, and creates/configures the buckets
# required by the IoT streaming pipeline.
# =====================================================================

set -e

MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://minio:9000}"
MINIO_ROOT_USER="${MINIO_ROOT_USER:-minioadmin}"
MINIO_ROOT_PASSWORD="${MINIO_ROOT_PASSWORD:-minioadmin123}"
MINIO_BUCKET="${MINIO_BUCKET:-iot-telemetry-raw}"
MC_ALIAS="localminio"

echo "=================================================================="
echo "[minio-init] Starting MinIO bucket initialization"
echo "[minio-init] Endpoint: ${MINIO_ENDPOINT}"
echo "[minio-init] Target bucket: ${MINIO_BUCKET}"
echo "=================================================================="

# ---------------------------------------------------------------------
# Wait for MinIO to become reachable
# ---------------------------------------------------------------------
MAX_RETRIES=30
RETRY_COUNT=0

until mc alias set "${MC_ALIAS}" "${MINIO_ENDPOINT}" "${MINIO_ROOT_USER}" "${MINIO_ROOT_PASSWORD}" >/dev/null 2>&1; do
    RETRY_COUNT=$((RETRY_COUNT + 1))
    if [ "${RETRY_COUNT}" -ge "${MAX_RETRIES}" ]; then
        echo "[minio-init] ERROR: Could not connect to MinIO after ${MAX_RETRIES} attempts."
        exit 1
    fi
    echo "[minio-init] MinIO not ready yet, retrying (${RETRY_COUNT}/${MAX_RETRIES})..."
    sleep 2
done

echo "[minio-init] Successfully connected to MinIO."

# ---------------------------------------------------------------------
# Create primary bucket for raw telemetry archives
# ---------------------------------------------------------------------
if mc ls "${MC_ALIAS}/${MINIO_BUCKET}" >/dev/null 2>&1; then
    echo "[minio-init] Bucket '${MINIO_BUCKET}' already exists. Skipping creation."
else
    echo "[minio-init] Creating bucket '${MINIO_BUCKET}'..."
    mc mb "${MC_ALIAS}/${MINIO_BUCKET}"
    echo "[minio-init] Bucket '${MINIO_BUCKET}' created."
fi

# ---------------------------------------------------------------------
# Create secondary bucket for processed/aggregated parquet output
# ---------------------------------------------------------------------
SECONDARY_BUCKET="iot-telemetry-processed"
if mc ls "${MC_ALIAS}/${SECONDARY_BUCKET}" >/dev/null 2>&1; then
    echo "[minio-init] Bucket '${SECONDARY_BUCKET}' already exists. Skipping creation."
else
    echo "[minio-init] Creating bucket '${SECONDARY_BUCKET}'..."
    mc mb "${MC_ALIAS}/${SECONDARY_BUCKET}"
    echo "[minio-init] Bucket '${SECONDARY_BUCKET}' created."
fi

# ---------------------------------------------------------------------
# Set lifecycle-friendly versioning off (keep simple for demo), and
# apply a basic download policy so objects can be inspected easily
# via the console/API during development.
# ---------------------------------------------------------------------
mc version disable "${MC_ALIAS}/${MINIO_BUCKET}" >/dev/null 2>&1 || true
mc version disable "${MC_ALIAS}/${SECONDARY_BUCKET}" >/dev/null 2>&1 || true

mc anonymous set download "${MC_ALIAS}/${MINIO_BUCKET}" >/dev/null 2>&1 || true

# ---------------------------------------------------------------------
# Seed folder structure with placeholder objects (optional but useful
# so the bucket structure is visible immediately in the console)
# ---------------------------------------------------------------------
echo "iot-telemetry-raw bucket initialized on $(date -u)" > /tmp/README.txt
mc cp /tmp/README.txt "${MC_ALIAS}/${MINIO_BUCKET}/_init/README.txt" >/dev/null 2>&1 || true
mc cp /tmp/README.txt "${MC_ALIAS}/${SECONDARY_BUCKET}/_init/README.txt" >/dev/null 2>&1 || true

echo "[minio-init] Bucket initialization complete."
echo "=================================================================="

exit 0
