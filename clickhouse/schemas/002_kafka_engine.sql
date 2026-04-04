-- ═══════════════════════════════════════════════════════════
-- 002: Kafka Engine Table (Consumer from Redpanda cold topic)
-- REQ-2.4: ClickHouse Kafka Engine operates as consumer group
-- ═══════════════════════════════════════════════════════════

-- CREATE TABLE IF NOT EXISTS telemetry_staging.kafka_telemetry_cold
-- (
--     timestamp         DateTime64(9, 'UTC'),
--     trace_id          String,
--     span_id           String,
--     service_name      String,
--     severity          String,
--     body              String,
--     ml_features       String,     -- JSON-encoded ML features
--     is_anomalous      UInt8,
--     anomaly_reason    String,
--     aggregator_received_at  DateTime64(9, 'UTC')
-- )
-- ENGINE = Kafka
-- SETTINGS
--     kafka_broker_list = 'redpanda:9092',
--     kafka_topic_list = 'telemetry-cold',
--     kafka_group_name = 'clickhouse-cold-consumer',
--     kafka_format = 'JSONEachRow',
--     kafka_num_consumers = 1,
--     kafka_max_block_size = 100000,       -- REQ-2.4: batch 100k rows
--     kafka_skip_broken_messages = 1000;

-- testing cause of JSON parsing issues with ml_features field — will add back after fixing producer side
CREATE TABLE IF NOT EXISTS telemetry_staging.kafka_telemetry_cold
(
    timestamp String,
    trace_id String,
    span_id String,
    service_name String,
    severity String,
    body String,
    is_anomalous UInt8
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'redpanda:9092',
    kafka_topic_list = 'telemetry-cold',
    kafka_group_name = 'clickhouse-cold-consumer',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1,
    kafka_skip_broken_messages = 100;