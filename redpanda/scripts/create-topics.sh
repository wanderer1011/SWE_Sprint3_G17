#!/bin/bash
# ─────────────────────────────────────────────────────────────
# Create Redpanda Topics for IDOP Ingestion Layer
#
# Topics:
#   telemetry-hot   — Fast ML/Alerts path (NVMe, low latency)
#   telemetry-cold  — Bulk log storage path (S3 tiered)
#   telemetry-dlq   — Dead letter queue (7-day retention)
#   telemetry-ml-features — Flink → ML pipeline
#   alerts-critical — Flink → AlertManager
# ─────────────────────────────────────────────────────────────
set -euo pipefail

BROKER="${REDPANDA_BROKER:-127.0.0.1:9092}"
REPLICATION="${REPLICATION_FACTOR:-1}"

echo "==> Creating Redpanda topics on ${BROKER}..."

# Hot Path — low latency, high throughput, NVMe storage
rpk topic create telemetry-hot \
    --brokers "${BROKER}" \
    --partitions "${HOT_PARTITIONS:-6}" \
    --replicas "${REPLICATION}" \
    --topic-config retention.ms=86400000 \
    --topic-config segment.bytes=134217728 \
    --topic-config compression.type=snappy \
    --topic-config min.insync.replicas=1 \
    || echo "  (topic telemetry-hot may already exist)"

# Cold Path — bulk storage, Zstd compression, S3 tiered storage
rpk topic create telemetry-cold \
    --brokers "${BROKER}" \
    --partitions "${COLD_PARTITIONS:-12}" \
    --replicas "${REPLICATION}" \
    --topic-config retention.ms=604800000 \
    --topic-config segment.bytes=536870912 \
    --topic-config compression.type=zstd \
    --topic-config min.insync.replicas=1 \
    --topic-config redpanda.remote.write=true \
    --topic-config redpanda.remote.read=true \
    || echo "  (topic telemetry-cold may already exist)"

# Dead Letter Queue — 7-day retention, manual inspection
rpk topic create telemetry-dlq \
    --brokers "${BROKER}" \
    --partitions 3 \
    --replicas "${REPLICATION}" \
    --topic-config retention.ms=604800000 \
    --topic-config compression.type=zstd \
    || echo "  (topic telemetry-dlq may already exist)"

# ML Features — Flink output → Python ML Pipeline
rpk topic create telemetry-ml-features \
    --brokers "${BROKER}" \
    --partitions 6 \
    --replicas "${REPLICATION}" \
    --topic-config retention.ms=3600000 \
    --topic-config compression.type=snappy \
    || echo "  (topic telemetry-ml-features may already exist)"

# Critical Alerts — Flink output → AlertManager/PagerDuty
rpk topic create alerts-critical \
    --brokers "${BROKER}" \
    --partitions 3 \
    --replicas "${REPLICATION}" \
    --topic-config retention.ms=259200000 \
    --topic-config compression.type=snappy \
    || echo "  (topic alerts-critical may already exist)"

echo "==> Topic creation complete."
rpk topic list --brokers "${BROKER}"
