-- ═══════════════════════════════════════════════════════════
-- 003: Main Telemetry MergeTree Storage Table
-- REQ-4.1: Replicated storage engine for data durability
-- REQ-4.2: Sorted and partitioned by service, timestamp, severity
-- ═══════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS telemetry.logs
(
    -- Tier 1: Identity & Correlation
    timestamp         DateTime64(9, 'UTC')  CODEC(DoubleDelta, ZSTD(1)),
    trace_id          String                CODEC(ZSTD(1)),
    span_id           String                CODEC(ZSTD(1)),
    parent_span_id    String                CODEC(ZSTD(1)),
    service_name      LowCardinality(String),
    service_version   LowCardinality(String),

    -- Tier 2: Classification
    severity_text     LowCardinality(String),
    severity          LowCardinality(String),
    severity_number   UInt8,
    category          LowCardinality(String),
    source_type       LowCardinality(String),
    signal_type       LowCardinality(String),
    status_code       UInt16,

    -- Tier 3: Content
    body              String                CODEC(ZSTD(3)),
    error_class       LowCardinality(String),

    -- Tier 4: Operational Context
    host_name         LowCardinality(String),
    environment       LowCardinality(String),
    deployment_id     String                CODEC(ZSTD(1)),
    duration_ms       Float64,

    -- Tier 5: Pipeline Metadata
    pipeline_ts       String                CODEC(ZSTD(1)),
    pii_masked        UInt8,

    -- Vector Aggregator enrichments
    ml_features       String                CODEC(ZSTD(1)),
    is_anomalous      UInt8,
    anomaly_reason    LowCardinality(String),
    is_metric         UInt8,
    is_security_flag  UInt8,
    resource_flat     String                CODEC(ZSTD(1)),
    aggregator_received_at  DateTime64(9, 'UTC'),

    -- Insertion metadata
    inserted_at       DateTime DEFAULT now()
)
ENGINE = MergeTree()
PARTITION BY (toYYYYMMDD(timestamp), service_name)
ORDER BY (service_name, severity, timestamp, trace_id)
TTL toDateTime(timestamp) + INTERVAL 7 DAY   -- REQ-RET-1: 7-day hot retention
SETTINGS
    index_granularity = 8192,
    merge_with_ttl_timeout = 86400,
    storage_policy = 'default';
