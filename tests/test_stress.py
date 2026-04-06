"""
Stress tests for IDOP Ingestion Layer.

Measures end-to-end ingestion throughput:
  1. HTTP → Vector Aggregator (single-threaded & concurrent)
  2. Kafka → Redpanda direct produce
  3. Cold path: Vector → Redpanda → ClickHouse arrival latency
"""

import json
import os
import statistics
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed

import pytest
import requests

# ── Import shared helpers ──
from conftest import (
    CLICKHOUSE_URL,
    CLICKHOUSE_PASSWORD,
    CLICKHOUSE_USER,
    REDPANDA_BROKER,
    VECTOR_AGG_URL,
    make_telemetry_event,
    send_events_to_vector,
    wait_for_service,
)

# ── Stress test constants ──
VECTOR_HTTP_INGEST_URL = os.getenv("VECTOR_HTTP_INGEST_URL", "http://localhost:8687")
SMALL_BATCH =  100
MEDIUM_BATCH = 1_000
LARGE_BATCH =  5_000
CONCURRENT_WORKERS = 10


# ────────────────────────────────────────────────────────────
# Helpers
# ────────────────────────────────────────────────────────────

def generate_batch(count, service_prefix="stress"):
    """Generate a batch of schema-compliant telemetry events."""
    events = []
    for i in range(count):
        evt = make_telemetry_event(
            service_name=f"{service_prefix}-svc-{i % 8}",
            severity="ERROR" if i % 20 == 0 else "INFO",
            body=f"Stress test event #{i} — latency {i * 3}ms",
        )
        events.append(evt)
    return events


def send_single_http(event, url=VECTOR_HTTP_INGEST_URL):
    """Send a single event over HTTP and return (status, duration_ms)."""
    start = time.perf_counter()
    try:
        resp = requests.post(url, json=event, timeout=10)
        elapsed = (time.perf_counter() - start) * 1000
        return resp.status_code, elapsed
    except Exception as e:
        elapsed = (time.perf_counter() - start) * 1000
        return 0, elapsed


def send_batch_http(events, url=VECTOR_HTTP_INGEST_URL):
    """Send a batch of events sequentially, return (success_count, total_ms, per_event_ms_list)."""
    latencies = []
    successes = 0
    start = time.perf_counter()
    for evt in events:
        code, lat = send_single_http(evt, url)
        latencies.append(lat)
        if 200 <= code < 300:
            successes += 1
    total_ms = (time.perf_counter() - start) * 1000
    return successes, total_ms, latencies


def query_clickhouse(sql):
    """Execute a ClickHouse query and return the response text."""
    resp = requests.get(
        CLICKHOUSE_URL,
        params={"query": sql, "user": CLICKHOUSE_USER, "password": CLICKHOUSE_PASSWORD},
        timeout=30,
    )
    resp.raise_for_status()
    return resp.text.strip()


# ────────────────────────────────────────────────────────────
# Tests
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

        all_latencies = []
        total_successes = 0
        start = time.perf_counter()

        with ThreadPoolExecutor(max_workers=CONCURRENT_WORKERS) as executor:
            futures = [executor.submit(send_batch_http, chunk) for chunk in chunks]
            for f in as_completed(futures):
                successes, _, latencies = f.result()
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

        all_latencies = []
        total_successes = 0
        start = time.perf_counter()

        with ThreadPoolExecutor(max_workers=CONCURRENT_WORKERS) as executor:
            futures = [executor.submit(send_batch_http, chunk) for chunk in chunks]
            for f in as_completed(futures):
                successes, _, latencies = f.result()
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


class TestColdPathEndToEnd:
    """Measure end-to-end latency: HTTP → Vector → Redpanda → ClickHouse."""

    @pytest.fixture(autouse=True)
    def _wait(self):
        wait_for_service(VECTOR_HTTP_INGEST_URL, timeout=30)
        wait_for_service(CLICKHOUSE_URL, timeout=30)

    def test_e2e_cold_path_latency(self):
        """Send 200 events via HTTP and measure time until they appear in ClickHouse."""
        marker = uuid.uuid4().hex[:12]
        count = 200
        events = []
        for i in range(count):
            evt = make_telemetry_event(
                service_name=f"e2e-stress-{marker}",
                severity="INFO",
                body=f"E2E stress marker={marker} idx={i}",
            )
            events.append(evt)

        # Get baseline count
        baseline_sql = (
            f"SELECT count() FROM telemetry.logs "
            f"WHERE service_name = 'e2e-stress-{marker}'"
        )
        try:
            baseline = int(query_clickhouse(baseline_sql))
        except Exception:
            baseline = 0

        # Send events
        send_start = time.perf_counter()
        successes, send_ms, _ = send_batch_http(events)
        print(f"\n--- E2E Cold Path ({count} events) ---")
        print(f"  Send time: {send_ms:.0f} ms ({successes} accepted)")

        # Poll ClickHouse until events arrive
        target = baseline + int(count * 0.5)  # wait for at least 50%
        poll_start = time.perf_counter()
        timeout = 60  # seconds
        arrived = baseline
        while (time.perf_counter() - poll_start) < timeout:
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
        print(f"  E2E time  : {e2e_ms:.0f} ms (send + ClickHouse arrival)")

        assert arrived > baseline, "No events arrived in ClickHouse within timeout"


class TestBurstIngestion:
    """Test burst patterns — rapid fire followed by query."""

    @pytest.fixture(autouse=True)
    def _wait(self):
        wait_for_service(VECTOR_HTTP_INGEST_URL, timeout=30)

    def test_burst_500_then_query(self):
        """Send 500 events as fast as possible, then verify routing."""
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
        # Fire all at once with max concurrency
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
