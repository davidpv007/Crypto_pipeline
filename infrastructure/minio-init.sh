#!/bin/sh
set -e

echo "Waiting for MinIO to be ready..."
sleep 5

# Configure mc client
mc alias set local http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD"

# Create buckets
mc mb --ignore-existing local/bronze
mc mb --ignore-existing local/silver
mc mb --ignore-existing local/gold
mc mb --ignore-existing local/marts

echo "✅ Buckets created: bronze, silver, gold, marts"