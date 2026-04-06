-- ═══════════════════════════════════════════════════════════
-- 004: Materialized View (Kafka → MergeTree)
-- REQ-2.4: Telemetry aggregated via MV and batched before
--          insertion into MergeTree storage
-- ═══════════════════════════════════════════════════════════

CREATE MATERIALIZED VIEW IF NOT EXISTS telemetry_staging.kafka_to_logs
TO telemetry.logs
AS
SELECT
    -- Convert timestamp: handle both epoch nanoseconds (string) and ISO 8601
    multiIf(
        position(timestamp, 'T') > 0, parseDateTime64BestEffortOrZero(timestamp, 9, 'UTC'),
        length(timestamp) > 0, fromUnixTimestamp64Nano(toInt64OrZero(timestamp)),
        toDateTime64(0, 9, 'UTC')
    ) AS timestamp,

    -- Tier 1: Identity & Correlation
    trace_id,
    span_id,
    parent_span_id,
    service_name,
    service_version,

    -- Tier 2: Classification
    severity_text,
    severity,
    severity_number,
    category,
    source_type,
    signal_type,
    status_code,

    -- Tier 3: Content
    body,
    error_class,

    -- Tier 4: Operational Context
    host_name,
    environment,
    deployment_id,
    duration_ms,

    -- Tier 5: Pipeline Metadata
    pipeline_ts,
    pii_masked,

    -- Vector Aggregator enrichments
    ml_features,
    is_anomalous,
    anomaly_reason,
    is_metric,
    is_security_flag,
    resource_flat,
    parseDateTime64BestEffortOrZero(aggregator_received_at, 9, 'UTC') AS aggregator_received_at,
    now() AS inserted_at
FROM telemetry_staging.kafka_telemetry_cold;
