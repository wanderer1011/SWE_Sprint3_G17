-- ═══════════════════════════════════════════════════════════
-- 002: Kafka Engine Table (Consumer from Redpanda cold topic)
-- REQ-2.4: ClickHouse Kafka Engine operates as consumer group
-- ═══════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS telemetry_staging.kafka_telemetry_cold
(
    -- Tier 1: Identity & Correlation
    timestamp           String,
    trace_id            String,
    span_id             String,
    parent_span_id      String,
    service_name        String,
    service_version     String,

    -- Tier 2: Classification
    severity_text       String,
    severity            String,
    severity_number     UInt8,
    category            String,
    source_type         String,
    signal_type         String,
    status_code         UInt16,

    -- Tier 3: Content
    body                String,
    error_class         String,

    -- Tier 4: Operational Context
    host_name           String,
    environment         String,
    deployment_id       String,
    duration_ms         Float64,

    -- Tier 5: Pipeline Metadata
    pipeline_ts         String,
    pii_masked          UInt8,

    -- Vector Aggregator enrichments
    ml_features         String,
    is_anomalous        UInt8,
    anomaly_reason      String,
    is_metric           UInt8,
    is_security_flag    UInt8,
    resource_flat       String,
    aggregator_received_at  String
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'redpanda:9092',
    kafka_topic_list = 'telemetry-cold',
    kafka_group_name = 'clickhouse-cold-consumer',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1,
    kafka_max_block_size = 100000,       -- REQ-2.4: batch 100k rows
    kafka_skip_broken_messages = 1000;
