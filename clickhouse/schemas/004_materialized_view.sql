-- ═══════════════════════════════════════════════════════════
-- 004: Materialized View (Kafka → MergeTree)
-- REQ-2.4: Telemetry aggregated via MV and batched before
--          insertion into MergeTree storage
-- ═══════════════════════════════════════════════════════════

-- CREATE MATERIALIZED VIEW IF NOT EXISTS telemetry_staging.kafka_to_logs
-- TO telemetry.logs
-- AS
-- SELECT
--     timestamp,
--     trace_id,
--     span_id,
--     -- Extract service.name from the JSON if stored nested
--     COALESCE(
--         JSONExtractString(service_name, ''),
--         service_name
--     ) AS service_name,
--     severity,
--     body,
--     ml_features,
--     is_anomalous,
--     anomaly_reason,
--     aggregator_received_at,
--     now() AS inserted_at
-- FROM telemetry_staging.kafka_telemetry_cold;

-- latest used and created manually is :
CREATE MATERIALIZED VIEW telemetry_staging.kafka_to_logs
TO telemetry.logs
AS
SELECT
    parseDateTime64BestEffort(timestamp) AS timestamp,
    trace_id,
    span_id,
    service_name,
    severity,
    body,
    '' AS ml_features,
    is_anomalous,
    '' AS anomaly_reason,
    now() AS aggregator_received_at,
    now() AS inserted_at
FROM telemetry_staging.kafka_telemetry_cold;
