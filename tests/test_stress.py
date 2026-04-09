"""
Unified stress, pipeline, and latency tests for the IDOP Ingestion Layer.

Covers:
  1. HTTP → Vector Aggregator throughput (sequential & concurrent)
  2. Kafka → Redpanda direct produce throughput
  3. Burst ingestion with acceptance-rate assertions
  4. Cold-path burst: HTTP → Vector → Redpanda no-loss verification
  5. Hot-path burst: HTTP → Vector → Redpanda no-loss verification
  6. Hot-path latency: ingest → telemetry-hot topic visibility (with p95 budget)
  7. Cold-path latency: ingest → ClickHouse row visibility (with p95 budget)
  8. E2E cold-path latency: HTTP → Vector → Redpanda → ClickHouse
"""

import json
import math
import os
import statistics
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed

import pytest
import requests

# ── Import shared helpers ──
from conftest import (
    CLICKHOUSE_PASSWORD,
    CLICKHOUSE_URL,
    CLICKHOUSE_USER,
    REDPANDA_BROKER,
    VECTOR_AGG_URL,
    make_telemetry_event,
    send_events_to_vector,
    wait_for_service,
)

# ── Constants ──
VECTOR_HTTP_INGEST_URL = os.getenv("VECTOR_HTTP_INGEST_URL", "http://localhost:8687")
SMALL_BATCH      = 100
MEDIUM_BATCH     = 1_000
LARGE_BATCH      = 5_000
CONCURRENT_WORKERS = 10
ALLOWED_STATUS_CODES = {200, 201, 202, 204}


# ────────────────────────────────────────────────────────────
# Shared helpers
# ────────────────────────────────────────────────────────────

def generate_batch(count, service_prefix="stress"):
    """Generate a batch of schema-compliant telemetry events."""
    return [
        make_telemetry_event(
            service_name=f"{service_prefix}-svc-{i % 8}",
            severity="ERROR" if i % 20 == 0 else "INFO",
            body=f"Stress test event #{i} — latency {i * 3}ms",
        )
        for i in range(count)
    ]


def send_single_http(event, url=VECTOR_HTTP_INGEST_URL):
    """Send one event; return (status_code, duration_ms)."""
    start = time.perf_counter()
    try:
        resp = requests.post(url, json=event, timeout=10)
        return resp.status_code, (time.perf_counter() - start) * 1000
    except Exception:
        return 0, (time.perf_counter() - start) * 1000


def send_batch_http(events, url=VECTOR_HTTP_INGEST_URL):
    """Send events sequentially; return (successes, total_ms, latency_list)."""
    latencies, successes = [], 0
    start = time.perf_counter()
    for evt in events:
        code, lat = send_single_http(evt, url)
        latencies.append(lat)
        if 200 <= code < 300:
            successes += 1
    return successes, (time.perf_counter() - start) * 1000, latencies


def query_clickhouse(sql):
    """Execute a ClickHouse query via HTTP GET and return stripped text."""
    resp = requests.get(
        CLICKHOUSE_URL,
        params={"query": sql, "user": CLICKHOUSE_USER, "password": CLICKHOUSE_PASSWORD},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.text.strip()


def clickhouse_count_for_body(marker):
    """Return row count in telemetry.logs matching an exact body string."""
    safe = marker.replace("'", "''")
    resp = requests.post(
        CLICKHOUSE_URL,
        data=f"SELECT count() FROM telemetry.logs WHERE body = '{safe}'",
        params={"user": CLICKHOUSE_USER, "password": CLICKHOUSE_PASSWORD},
        timeout=10,
    )
    resp.raise_for_status()
    result = resp.text.strip()
    return int(result) if result else 0


def percentile(values, pct):
    """Return the pct-th percentile of values (0-100)."""
    if not values:
        return 0.0
    ordered = sorted(values)
    index = max(0, min(math.ceil((pct / 100.0) * len(ordered)) - 1, len(ordered) - 1))
    return ordered[index]


def safe_json(message_bytes):
    """Decode Kafka message bytes to dict, returning {} on error."""
    try:
        return json.loads(message_bytes.decode("utf-8"))
    except Exception:
        return {}


def make_kafka_consumer(topic):
    """Create a KafkaConsumer for the given topic; skip test if unavailable."""
    try:
        from kafka import KafkaConsumer
        return KafkaConsumer(
            topic,
            bootstrap_servers=REDPANDA_BROKER,
            group_id=f"stress-{uuid.uuid4().hex[:10]}",
            auto_offset_reset="latest",
            consumer_timeout_ms=1000,
            value_deserializer=safe_json,
        )
    except Exception:
        pytest.skip("kafka-python not available or Redpanda not reachable")


def consume_unique_bodies(consumer, body_prefix, expected_count, timeout_sec):
    """Poll consumer until expected_count unique bodies with body_prefix are seen."""
    seen = set()
    deadline = time.monotonic() + timeout_sec
    while time.monotonic() < deadline and len(seen) < expected_count:
        for messages in consumer.poll(timeout_ms=1000).values():
            for msg in messages:
                body = str((msg.value or {}).get("body", ""))
                if body.startswith(body_prefix):
                    seen.add(body)
    return seen


def wait_for_topic_marker(consumer, marker, timeout_sec):
    """Return True once any consumed message body contains marker."""
    deadline = time.monotonic() + timeout_sec
    while time.monotonic() < deadline:
        for messages in consumer.poll(timeout_ms=500).values():
            for msg in messages:
                if marker in str((msg.value or {}).get("body", "")):
                    return True
    return False


# ────────────────────────────────────────────────────────────
# 1. HTTP Ingestion Throughput
# ────────────────────────────────────────────────────────────

class TestHTTPIngestionThroughput:
    """Measure events/sec through the Vector HTTP endpoint."""

    @pytest.fixture(autouse=True)
    def _wait_for_vector(self):
        wait_for_service(VECTOR_HTTP_INGEST_URL, timeout=30)

    def test_sequential_100_events(self):
        """Baseline: 100 events sent one-at-a-time."""
        events = generate_batch(SMALL_BATCH)
        successes, total_ms, latencies = send_batch_http(events)

        eps = (successes / total_ms) * 1000 if total_ms > 0 else 0
        p50 = statistics.median(latencies)
        p95 = sorted(latencies)[int(len(latencies) * 0.95)]
        p99 = sorted(latencies)[int(len(latencies) * 0.99)]

        print(f"\n--- Sequential {SMALL_BATCH} events ---")
        print(f"  Successes : {successes}/{len(events)}")
        print(f"  Total     : {total_ms:.0f} ms")
        print(f"  Throughput: {eps:.1f} events/sec")
        print(f"  Latency p50={p50:.1f}ms  p95={p95:.1f}ms  p99={p99:.1f}ms")

        assert successes >= SMALL_BATCH * 0.95, f"Too many failures: {successes}/{SMALL_BATCH}"

    def test_sequential_1000_events(self):
        """Medium load: 1000 sequential events."""
        events = generate_batch(MEDIUM_BATCH)
        successes, total_ms, latencies = send_batch_http(events)

        eps = (successes / total_ms) * 1000 if total_ms > 0 else 0
        p50 = statistics.median(latencies)
        p95 = sorted(latencies)[int(len(latencies) * 0.95)]

        print(f"\n--- Sequential {MEDIUM_BATCH} events ---")
        print(f"  Successes : {successes}/{len(events)}")
        print(f"  Total     : {total_ms:.0f} ms")
        print(f"  Throughput: {eps:.1f} events/sec")
        print(f"  Latency p50={p50:.1f}ms  p95={p95:.1f}ms")

        assert successes >= MEDIUM_BATCH * 0.95

    def test_concurrent_1000_events(self):
        """Concurrent load: 1000 events across 10 threads."""
        events = generate_batch(MEDIUM_BATCH)
        chunk_size = MEDIUM_BATCH // CONCURRENT_WORKERS
        chunks = [events[i:i + chunk_size] for i in range(0, len(events), chunk_size)]

        all_latencies, total_successes = [], 0
        start = time.perf_counter()

        with ThreadPoolExecutor(max_workers=CONCURRENT_WORKERS) as executor:
            for successes, _, latencies in executor.map(send_batch_http, chunks):
                total_successes += successes
                all_latencies.extend(latencies)

        wall_ms = (time.perf_counter() - start) * 1000
        eps = (total_successes / wall_ms) * 1000 if wall_ms > 0 else 0
        p50 = statistics.median(all_latencies)
        p95 = sorted(all_latencies)[int(len(all_latencies) * 0.95)]

        print(f"\n--- Concurrent {MEDIUM_BATCH} events ({CONCURRENT_WORKERS} threads) ---")
        print(f"  Successes : {total_successes}/{len(events)}")
        print(f"  Wall time : {wall_ms:.0f} ms")
        print(f"  Throughput: {eps:.1f} events/sec")
        print(f"  Latency p50={p50:.1f}ms  p95={p95:.1f}ms")

        assert total_successes >= MEDIUM_BATCH * 0.90

    def test_concurrent_5000_events(self):
        """High load: 5000 events across 10 threads."""
        events = generate_batch(LARGE_BATCH)
        chunk_size = LARGE_BATCH // CONCURRENT_WORKERS
        chunks = [events[i:i + chunk_size] for i in range(0, len(events), chunk_size)]

        all_latencies, total_successes = [], 0
        start = time.perf_counter()

        with ThreadPoolExecutor(max_workers=CONCURRENT_WORKERS) as executor:
            for successes, _, latencies in executor.map(send_batch_http, chunks):
                total_successes += successes
                all_latencies.extend(latencies)

        wall_ms = (time.perf_counter() - start) * 1000
        eps = (total_successes / wall_ms) * 1000 if wall_ms > 0 else 0
        p50 = statistics.median(all_latencies)
        p95 = sorted(all_latencies)[int(len(all_latencies) * 0.95)]

        print(f"\n--- Concurrent {LARGE_BATCH} events ({CONCURRENT_WORKERS} threads) ---")
        print(f"  Successes : {total_successes}/{len(events)}")
        print(f"  Wall time : {wall_ms:.0f} ms")
        print(f"  Throughput: {eps:.1f} events/sec")
        print(f"  Latency p50={p50:.1f}ms  p95={p95:.1f}ms")

        assert total_successes >= LARGE_BATCH * 0.90


# ────────────────────────────────────────────────────────────
# 2. Kafka Direct Throughput
# ────────────────────────────────────────────────────────────

class TestKafkaDirectThroughput:
    """Measure produce throughput directly to Redpanda."""

    @pytest.fixture(autouse=True)
    def _ensure_kafka(self):
        try:
            from kafka import KafkaProducer
            self.producer = KafkaProducer(
                bootstrap_servers=REDPANDA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                acks="all",
                linger_ms=5,
                batch_size=64 * 1024,
            )
        except Exception:
            pytest.skip("kafka-python not available or Redpanda not reachable")
        yield
        self.producer.close()

    def test_kafka_produce_5000_events(self):
        """Direct Kafka produce throughput to telemetry-cold."""
        events = generate_batch(LARGE_BATCH, service_prefix="kafka-stress")

        start = time.perf_counter()
        for evt in events:
            self.producer.send("telemetry-cold", key=evt["trace_id"], value=evt)
        self.producer.flush()
        total_ms = (time.perf_counter() - start) * 1000

        eps = (LARGE_BATCH / total_ms) * 1000 if total_ms > 0 else 0
        print(f"\n--- Kafka Direct {LARGE_BATCH} events ---")
        print(f"  Total     : {total_ms:.0f} ms")
        print(f"  Throughput: {eps:.1f} events/sec")

        assert total_ms > 0


# ────────────────────────────────────────────────────────────
# 3. Burst Ingestion
# ────────────────────────────────────────────────────────────

class TestBurstIngestion:
    """Rapid-fire bursts with acceptance-rate assertions."""

    @pytest.fixture(autouse=True)
    def _wait(self):
        wait_for_service(VECTOR_HTTP_INGEST_URL, timeout=30)

    def test_burst_500_then_query(self):
        """Send 500 events as fast as possible across 20 threads."""
        marker = uuid.uuid4().hex[:8]
        events = []
        for i in range(500):
            sev = "ERROR" if i % 5 == 0 else "INFO"
            evt = make_telemetry_event(
                service_name=f"burst-{marker}",
                severity=sev,
                body=f"Burst event {i} marker={marker}",
            )
            if sev == "ERROR":
                evt["error_class"] = "BurstTestError"
            events.append(evt)

        start = time.perf_counter()
        with ThreadPoolExecutor(max_workers=20) as executor:
            futures = [executor.submit(send_single_http, evt) for evt in events]
            results = [f.result() for f in as_completed(futures)]

        wall_ms = (time.perf_counter() - start) * 1000
        successes = sum(1 for code, _ in results if 200 <= code < 300)
        eps = (successes / wall_ms) * 1000 if wall_ms > 0 else 0

        print(f"\n--- Burst 500 events (20 threads) ---")
        print(f"  Successes : {successes}/500")
        print(f"  Wall time : {wall_ms:.0f} ms")
        print(f"  Throughput: {eps:.1f} events/sec")

        assert successes >= 400, f"Too many failures in burst: {successes}/500"


# ────────────────────────────────────────────────────────────
# 4. Pipeline Stress — No-Loss Verification via Kafka
# ────────────────────────────────────────────────────────────

class TestPipelineStress:
    """Burst assertions with Kafka consumer verification (no event loss)."""

    @pytest.fixture(autouse=True)
    def _wait(self):
        wait_for_service(VECTOR_HTTP_INGEST_URL, timeout=30)

    @pytest.mark.performance
    def test_cold_path_burst_without_loss(self):
        """Send cold-path burst and assert all events arrive on telemetry-cold."""
        event_count       = int(os.getenv("STRESS_COLD_EVENT_COUNT", "250"))
        worker_count      = int(os.getenv("STRESS_WORKERS", "24"))
        consume_timeout   = float(os.getenv("STRESS_COLD_TIMEOUT_SEC", "120"))
        drain_budget      = float(os.getenv("STRESS_COLD_DRAIN_BUDGET_SEC", "90"))

        marker_prefix = f"stress-cold-{uuid.uuid4().hex[:10]}"
        events = [
            make_telemetry_event(
                service_name="stress-cold-service",
                severity="INFO",
                body=f"{marker_prefix}-{i}",
            )
            for i in range(event_count)
        ]

        consumer = make_kafka_consumer("telemetry-cold")
        try:
            consumer.poll(timeout_ms=1000)  # warm consumer group

            send_start = time.monotonic()
            with ThreadPoolExecutor(max_workers=worker_count) as executor:
                statuses = list(executor.map(
                    lambda e: send_single_http(e)[0], events
                ))
            assert all(s in ALLOWED_STATUS_CODES for s in statuses), (
                f"Send failures: {sum(s in ALLOWED_STATUS_CODES for s in statuses)}/{len(statuses)} ok"
            )

            drain_start = time.monotonic()
            seen = consume_unique_bodies(consumer, marker_prefix, event_count, consume_timeout)
            drain_elapsed = time.monotonic() - drain_start
            total_elapsed = time.monotonic() - send_start

            assert len(seen) == event_count, (
                f"Cold-path event loss. Expected {event_count}, observed {len(seen)}"
            )
            assert drain_elapsed <= drain_budget, (
                f"Cold-path drain {drain_elapsed:.3f}s exceeded budget {drain_budget:.3f}s "
                f"(total {total_elapsed:.3f}s)"
            )
        finally:
            consumer.close()

    @pytest.mark.performance
    def test_hot_path_burst_without_loss(self):
        """Send hot-path burst and assert all events arrive on telemetry-hot."""
        event_count     = int(os.getenv("STRESS_HOT_EVENT_COUNT", "150"))
        worker_count    = int(os.getenv("STRESS_WORKERS", "24"))
        consume_timeout = float(os.getenv("STRESS_HOT_TIMEOUT_SEC", "120"))
        drain_budget    = float(os.getenv("STRESS_HOT_DRAIN_BUDGET_SEC", "90"))

        marker_prefix = f"stress-hot-{uuid.uuid4().hex[:10]}"
        events = [
            make_telemetry_event(
                service_name="stress-hot-service",
                severity="CRITICAL",
                body=f"{marker_prefix}-{i} OutOfMemoryError: Java heap space",
            )
            for i in range(event_count)
        ]

        consumer = make_kafka_consumer("telemetry-hot")
        try:
            consumer.poll(timeout_ms=1000)  # warm consumer group

            send_start = time.monotonic()
            with ThreadPoolExecutor(max_workers=worker_count) as executor:
                statuses = list(executor.map(
                    lambda e: send_single_http(e)[0], events
                ))
            assert all(s in ALLOWED_STATUS_CODES for s in statuses), (
                f"Send failures: {sum(s in ALLOWED_STATUS_CODES for s in statuses)}/{len(statuses)} ok"
            )

            drain_start = time.monotonic()
            seen = consume_unique_bodies(consumer, marker_prefix, event_count, consume_timeout)
            drain_elapsed = time.monotonic() - drain_start
            total_elapsed = time.monotonic() - send_start

            assert len(seen) == event_count, (
                f"Hot-path event loss. Expected {event_count}, observed {len(seen)}"
            )
            assert drain_elapsed <= drain_budget, (
                f"Hot-path drain {drain_elapsed:.3f}s exceeded budget {drain_budget:.3f}s "
                f"(total {total_elapsed:.3f}s)"
            )
        finally:
            consumer.close()


# ────────────────────────────────────────────────────────────
# 5. Pipeline Latency — p95 Budget Assertions
# ────────────────────────────────────────────────────────────

class TestPipelineLatency:
    """Latency measurements for hot and cold paths with SLO budgets."""

    @pytest.fixture(autouse=True)
    def _wait(self):
        wait_for_service(VECTOR_HTTP_INGEST_URL, timeout=30)

    @pytest.mark.performance
    def test_hot_path_event_visibility_latency(self):
        """Ingest → telemetry-hot topic visibility latency with p95 budget."""
        sample_count        = int(os.getenv("LATENCY_HOT_SAMPLE_COUNT", "6"))
        per_event_timeout   = float(os.getenv("LATENCY_HOT_TIMEOUT_SEC", "20"))
        p95_budget          = float(os.getenv("LATENCY_HOT_P95_BUDGET_SEC", "10"))
        max_budget          = float(os.getenv("LATENCY_HOT_MAX_BUDGET_SEC", "15"))

        consumer = make_kafka_consumer("telemetry-hot")
        latencies = []
        try:
            consumer.poll(timeout_ms=1000)  # warm consumer group

            for i in range(sample_count):
                marker = f"lat-hot-{uuid.uuid4().hex[:10]}-{i}"
                event = make_telemetry_event(
                    service_name="latency-hot-service",
                    severity="CRITICAL",
                    body=f"{marker} OutOfMemoryError: Java heap space",
                )
                start = time.monotonic()
                code, _ = send_single_http(event)
                assert code in ALLOWED_STATUS_CODES, f"Unexpected HTTP status: {code}"

                found = wait_for_topic_marker(consumer, marker, per_event_timeout)
                assert found, f"Marker '{marker}' not seen on telemetry-hot within timeout"
                latencies.append(time.monotonic() - start)
        finally:
            consumer.close()

        p50 = percentile(latencies, 50)
        p95 = percentile(latencies, 95)
        p99 = percentile(latencies, 99)

        print(f"\n--- Hot-Path Latency ({sample_count} samples) ---")
        print(f"  p50={p50:.3f}s  p95={p95:.3f}s  p99={p99:.3f}s  max={max(latencies):.3f}s")

        assert p95 <= p95_budget, (
            f"Hot path p95 {p95:.3f}s exceeded budget {p95_budget:.3f}s "
            f"(p50={p50:.3f}s, p99={p99:.3f}s)"
        )
        assert max(latencies) <= max_budget, (
            f"Hot path max {max(latencies):.3f}s exceeded budget {max_budget:.3f}s"
        )

    @pytest.mark.performance
    def test_cold_path_clickhouse_visibility_latency(self):
        """Ingest → ClickHouse row visibility latency with p95 budget."""
        sample_count      = int(os.getenv("LATENCY_COLD_SAMPLE_COUNT", "4"))
        per_event_timeout = float(os.getenv("LATENCY_COLD_TIMEOUT_SEC", "40"))
        p95_budget        = float(os.getenv("LATENCY_COLD_P95_BUDGET_SEC", "30"))
        max_budget        = float(os.getenv("LATENCY_COLD_MAX_BUDGET_SEC", "40"))

        latencies = []
        for i in range(sample_count):
            marker = f"lat-cold-{uuid.uuid4().hex[:10]}-{i}"
            event = make_telemetry_event(
                service_name="latency-cold-service",
                severity="INFO",
                body=marker,
            )
            start = time.monotonic()
            code, _ = send_single_http(event)
            assert code in ALLOWED_STATUS_CODES, f"Unexpected HTTP status: {code}"

            deadline = time.monotonic() + per_event_timeout
            found = False
            while time.monotonic() < deadline:
                if clickhouse_count_for_body(marker) >= 1:
                    found = True
                    latencies.append(time.monotonic() - start)
                    break
                time.sleep(0.5)

            assert found, f"Marker '{marker}' not visible in ClickHouse within timeout"

        p50 = percentile(latencies, 50)
        p95 = percentile(latencies, 95)
        p99 = percentile(latencies, 99)

        print(f"\n--- Cold-Path ClickHouse Latency ({sample_count} samples) ---")
        print(f"  p50={p50:.3f}s  p95={p95:.3f}s  p99={p99:.3f}s  max={max(latencies):.3f}s")

        assert p95 <= p95_budget, (
            f"Cold path p95 {p95:.3f}s exceeded budget {p95_budget:.3f}s "
            f"(p50={p50:.3f}s, p99={p99:.3f}s)"
        )
        assert max(latencies) <= max_budget, (
            f"Cold path max {max(latencies):.3f}s exceeded budget {max_budget:.3f}s"
        )


# ────────────────────────────────────────────────────────────
# 6. E2E Cold Path — HTTP → Vector → Redpanda → ClickHouse
# ────────────────────────────────────────────────────────────

class TestColdPathEndToEnd:
    """End-to-end latency: HTTP → Vector → Redpanda → ClickHouse."""

    @pytest.fixture(autouse=True)
    def _wait(self):
        wait_for_service(VECTOR_HTTP_INGEST_URL, timeout=30)
        wait_for_service(CLICKHOUSE_URL, timeout=30)

    def test_e2e_cold_path_latency(self):
        """Send 200 events via HTTP; measure time until ≥50% appear in ClickHouse."""
        marker = uuid.uuid4().hex[:12]
        count = 200
        events = [
            make_telemetry_event(
                service_name=f"e2e-stress-{marker}",
                severity="INFO",
                body=f"E2E stress marker={marker} idx={i}",
            )
            for i in range(count)
        ]

        baseline_sql = (
            f"SELECT count() FROM telemetry.logs "
            f"WHERE service_name = 'e2e-stress-{marker}'"
        )
        try:
            baseline = int(query_clickhouse(baseline_sql))
        except Exception:
            baseline = 0

        send_start = time.perf_counter()
        successes, send_ms, _ = send_batch_http(events)
        print(f"\n--- E2E Cold Path ({count} events) ---")
        print(f"  Send time: {send_ms:.0f} ms ({successes} accepted)")

        target = baseline + int(count * 0.5)
        poll_start = time.perf_counter()
        arrived = baseline
        while (time.perf_counter() - poll_start) < 60:
            try:
                arrived = int(query_clickhouse(baseline_sql))
                if arrived >= target:
                    break
            except Exception:
                pass
            time.sleep(2)

        e2e_ms = (time.perf_counter() - send_start) * 1000
        arrival_pct = ((arrived - baseline) / count) * 100

        print(f"  Arrived   : {arrived - baseline}/{count} ({arrival_pct:.0f}%)")
        print(f"  E2E time  : {e2e_ms:.0f} ms")

        assert arrived > baseline, "No events arrived in ClickHouse within 60s"
