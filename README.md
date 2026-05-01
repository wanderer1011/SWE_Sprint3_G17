# IDOP Ingestion Layer — Sprint 3

> Intelligence-Driven Observability Platform — Ingestion & Stream Processing Module

## Architecture Overview

```
Edge Agents ──→ Envoy LB ──→ Vector Aggregator ──→ Redpanda ──┬──→ ClickHouse (Cold Path)
                                  │                            │
                              VRL Transforms:                  └──→ Flink (Hot Path)
                              • Context Validation                   │
                              • Feature Translation                  ├──→ telemetry-ml-features
                              • Tail-Based Sampling                  └──→ alerts-critical
                              • Intelligent Routing
```

### Data Flow

| Path | Route | Purpose |
|------|-------|---------|
| **Hot** | `telemetry-hot` → Flink → ML Features / Alerts | Anomalous traces, metrics, security events |
| **Cold** | `telemetry-cold` → ClickHouse Kafka Engine → MergeTree | Structured logs, normal traces |
| **DLQ** | `telemetry-dlq` | Invalid/malformed events (7-day retention) |

## Folder Structure

```
ingestion-layer/
├── docker-compose.yml          # Full stack orchestration
├── .env.example                # Environment configuration template
├── Makefile                    # Build/run/test shortcuts
│
├── envoy/                      # L4/L7 Load Balancer (Ingestion Entry Point)
│   ├── Dockerfile
│   └── envoy.yaml              # Envoy proxy config with gRPC/HTTP2 routing
│
├── vector-aggregator/          # Stateless Ingestion Tier
│   ├── Dockerfile
│   ├── vector.yaml             # Main Vector config (sources → transforms → sinks)
│   └── vrl/                    # Vector Remap Language transforms
│       ├── context_validation.vrl    # REQ-2.6: Enforce trace_id, timestamps
│       ├── feature_translation.vrl   # ML feature extraction (GC, errors, latency)
│       └── tail_sampling.vrl         # Anomaly flagging & probabilistic sampling
│
├── redpanda/                   # Durable Message Buffer (Kafka-compatible)
│   ├── console-config.yaml     # Redpanda Console UI config
│   └── scripts/
│       └── create-topics.sh    # Topic creation with configured partitions/retention
│
├── flink/                      # Hot Path Stream Processing (Java 17)
│   ├── Dockerfile
│   ├── pom.xml                 # Maven build with Flink 1.19 + Kafka connector
│   └── src/main/java/com/idop/flink/
│       ├── FlinkJobMain.java           # Entry point
│       ├── jobs/
│       │   └── HotPathJob.java         # REQ-2.5: Tumbling windows, error rates
│       ├── functions/
│       │   ├── ErrorRateAggregator.java      # 1-min windowed aggregation + alerts
│       │   ├── AnomalyContextEnricher.java   # REQ-2.7: ClickHouse context lookup
│       │   └── TelemetryTimestampExtractor.java  # REQ-2.10: Event-time extraction
│       ├── models/
│       │   ├── TelemetryEvent.java     # Input event POJO
│       │   ├── AlertEvent.java         # Alert side output
│       │   └── MLFeaturePayload.java   # Enriched ML feature bundle
│       └── serialization/
│           ├── TelemetryDeserializer.java
│           ├── MLFeatureSerializer.java
│           └── AlertSerializer.java
│
├── clickhouse/                 # Cold Path Analytical Storage
│   ├── Dockerfile
│   ├── config.xml              # Server config (Kafka integration, query limits)
│   ├── users.xml               # RBAC (default + grafana_reader)
│   └── schemas/                # Auto-applied on container init
│       ├── 001_create_databases.sql
│       ├── 002_kafka_engine.sql          # REQ-2.4: Kafka consumer engine
│       ├── 003_telemetry_table.sql       # MergeTree with TTL + partitioning
│       ├── 004_materialized_view.sql     # Kafka → MergeTree pipeline
│       └── 005_bloom_filter_and_red_metrics.sql  # REQ-4.3 + RED metrics MV
│
├── scripts/
│   ├── health-check.sh         # Service health verification
│   └── generate-test-data.py   # Synthetic telemetry generator
│
├── tests/                      # Integration & E2E tests (pytest)
│   ├── conftest.py             # Shared fixtures & helpers
│   ├── requirements.txt
│   ├── test_routing.py         # Hot/Cold/DLQ routing verification
│   ├── test_clickhouse_ingestion.py  # Cold path storage tests
│   └── test_e2e_pipeline.py    # Full pipeline integration tests
│
└── monitoring/
    ├── prometheus/
    │   └── prometheus.yml      # Scrape configs for all services
    ├── grafana/
    │   ├── provisioning/
    │   │   ├── datasources.yml # Prometheus + ClickHouse datasources
    │   │   └── dashboards.yml
    │   └── dashboards/
    │       └── ingestion-overview.json  # Pre-built dashboard
    └── alertmanager/
        └── alertmanager.yml    # Alert routing rules
```

## Requirements Traceability

| Requirement | Component | File(s) |
|-------------|-----------|---------|
| REQ-2.1 (Data Type Routing) | Vector Aggregator | `vector.yaml`, `tail_sampling.vrl` |
| REQ-2.2 (Partitioning) | Redpanda | `create-topics.sh`, `vector.yaml` (key_field) |
| REQ-2.3 (Durability) | Redpanda | `create-topics.sh` (replicas, acks) |
| REQ-2.4 (Cold Consumer) | ClickHouse | `002_kafka_engine.sql`, `004_materialized_view.sql` |
| REQ-2.5 (Hot Consumer & Windows) | Flink | `HotPathJob.java`, `ErrorRateAggregator.java` |
| REQ-2.6 (Feature Eval & Sampling) | Vector Aggregator | `context_validation.vrl`, `feature_translation.vrl` |
| REQ-2.7 (Anomaly Context Lookup) | Flink | `AnomalyContextEnricher.java` |
| REQ-2.8 (RCP Output) | Flink | `HotPathJob.java` → `telemetry-ml-features` |
| REQ-2.9 (Offset Commits) | ClickHouse + Flink | Kafka Engine auto-commit, Flink checkpoints |
| REQ-2.10 (Event-Time Watermarking) | Flink | `TelemetryTimestampExtractor.java`, 60s grace |
| REQ-2.11 (Dead Letter Queue) | Vector Aggregator | `vector.yaml` → `telemetry-dlq` |
| REQ-4.2 (Sorted Partitioning) | ClickHouse | `003_telemetry_table.sql` |
| REQ-4.3 (Bloom Filters) | ClickHouse | `005_bloom_filter_and_red_metrics.sql` |

## Quick Start

```bash
# 1. Copy environment config
cp .env.example .env

# 2. Build and start all services
make build
make up

# 3. Create Redpanda topics
make topics

# 4. Verify everything is healthy
make health

# 5. Build and deploy the Flink job
make flink-deploy

# 6. Generate test data
make generate-data

# 7. Run integration tests
make test
```

## Key Ports

| Service | Port | URL |
|---------|------|-----|
| Envoy (ingress) | 10000 | `http://localhost:10000` |
| Vector Aggregator API | 8686 | `http://localhost:8686` |
| Redpanda Kafka | 19092 | `localhost:19092` |
| Redpanda Console | 8080 | `http://localhost:8080` |
| ClickHouse HTTP | 8123 | `http://localhost:8123` |
| Flink Web UI | 8081 | `http://localhost:8081` |
| Prometheus | 9090 | `http://localhost:9090` |
| Grafana | 3000 | `http://localhost:3000` |

## Architecture Improvements Over Original Design

1. **VRL-based ML Feature Translation** — Feature extraction happens at the aggregator level via VRL transforms, reducing Flink CPU load and enabling faster routing decisions.

2. **Probabilistic Tail Sampling** — Non-anomalous metrics are sampled at 10% before reaching the hot path, significantly reducing Flink and Redpanda hot-topic load.

3. **Pre-computed RED Metrics MV** — A ClickHouse Materialized View pre-aggregates 1-minute RED metrics, ensuring Grafana dashboards never hit raw log tables (REQ-5.5).

4. **Bloom Filter Indexes** — trace_id and span_id have bloom filter indexes for sub-100ms point lookups (REQ-4.3).

5. **Structured Alert Side Output** — Flink emits typed `AlertEvent` objects to a dedicated `alerts-critical` topic, decoupling alert routing from ML feature generation.

6. **Health Check Script** — Single-command verification of all 8 services for CI/CD integration.
