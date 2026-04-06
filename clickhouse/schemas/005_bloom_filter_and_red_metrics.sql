-- ═══════════════════════════════════════════════════════════
-- 005: Bloom Filter Index on trace_id
-- REQ-4.3: Probabilistic data structures (Bloom filters)
--          on trace_id for fast point lookups
-- ═══════════════════════════════════════════════════════════

ALTER TABLE telemetry.logs
    ADD INDEX IF NOT EXISTS idx_trace_id_bloom trace_id
    TYPE bloom_filter(0.01)
    GRANULARITY 4;

-- Secondary index on span_id for span-level lookups
ALTER TABLE telemetry.logs
    ADD INDEX IF NOT EXISTS idx_span_id_bloom span_id
    TYPE bloom_filter(0.01)
    GRANULARITY 4;

-- ═══════════════════════════════════════════════════════════
-- 006: Materialized View for RED Metrics (Grafana Dashboards)
-- REQ-5.5: Dashboards query pre-computed materialized views
-- ═══════════════════════════════════════════════════════════

-- Aggregated 1-minute RED metrics per service
CREATE TABLE IF NOT EXISTS telemetry.red_metrics_1m
(
    window_start      DateTime       CODEC(DoubleDelta),
    service_name      LowCardinality(String),
    total_requests    UInt64,
    error_count       UInt64,
    avg_latency_ms    Float64,
    p95_latency_ms    Float64,
    p99_latency_ms    Float64
)
ENGINE = SummingMergeTree()
PARTITION BY toYYYYMMDD(window_start)
ORDER BY (service_name, window_start)
TTL window_start + INTERVAL 30 DAY;

CREATE MATERIALIZED VIEW IF NOT EXISTS telemetry.red_metrics_1m_mv
TO telemetry.red_metrics_1m
AS
SELECT
    toStartOfMinute(timestamp) AS window_start,
    service_name,
    count()                     AS total_requests,
    countIf(severity IN ('ERROR', 'CRITICAL', 'FATAL')) AS error_count,
    avg(if(duration_ms >= 0, duration_ms, 0))           AS avg_latency_ms,
    quantile(0.95)(if(duration_ms >= 0, duration_ms, 0)) AS p95_latency_ms,
    quantile(0.99)(if(duration_ms >= 0, duration_ms, 0)) AS p99_latency_ms
FROM telemetry.logs
GROUP BY window_start, service_name;
