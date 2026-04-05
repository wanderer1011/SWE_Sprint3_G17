"""
Latency-focused integration tests for the IDOP ingestion pipeline.

These tests measure practical end-to-end latency segments:
1) Vector HTTP ingest -> Redpanda topic visibility (hot path)
2) Vector HTTP ingest -> ClickHouse row visibility (cold path)
"""

import json
import importlib.util
import math
import os
import time
import uuid
from pathlib import Path

import pytest
import requests


def _load_test_conftest():
    conftest_path = Path(__file__).with_name("conftest.py")
    spec = importlib.util.spec_from_file_location("tests_conftest", conftest_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("Unable to load tests/conftest.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


_CONFTEST = _load_test_conftest()
CLICKHOUSE_URL = _CONFTEST.CLICKHOUSE_URL
REDPANDA_BROKER = _CONFTEST.REDPANDA_BROKER
make_telemetry_event = _CONFTEST.make_telemetry_event
send_events_to_vector = _CONFTEST.send_events_to_vector


ALLOWED_STATUS_CODES = {200, 201, 202, 204}
CLICKHOUSE_AUTH_PARAMS = {"user": "default", "password": "changeme"}


def _safe_json(message_bytes):
    try:
        return json.loads(message_bytes.decode("utf-8"))
    except Exception:
        return {}


def _make_consumer(topic):
    KafkaConsumer = __import__("kafka").KafkaConsumer

    return KafkaConsumer(
        topic,
        bootstrap_servers=REDPANDA_BROKER,
        group_id=f"latency-{uuid.uuid4().hex[:10]}",
        auto_offset_reset="latest",
        consumer_timeout_ms=1000,
        value_deserializer=_safe_json,
    )


def _percentile(values, pct):
    if not values:
        return 0.0
    ordered = sorted(values)
    index = math.ceil((pct / 100.0) * len(ordered)) - 1
    index = max(0, min(index, len(ordered) - 1))
    return ordered[index]


def _wait_for_topic_marker(consumer, marker, timeout_sec):
    deadline = time.monotonic() + timeout_sec
    while time.monotonic() < deadline:
        records = consumer.poll(timeout_ms=500)
        for messages in records.values():
            for msg in messages:
                body = str((msg.value or {}).get("body", ""))
                if marker in body:
                    return True
    return False


def _clickhouse_count_for_body(marker):
    safe_marker = marker.replace("'", "''")
    query = f"SELECT count() FROM telemetry.logs WHERE body = '{safe_marker}'"
    resp = requests.post(
        CLICKHOUSE_URL,
        data=query,
        params=CLICKHOUSE_AUTH_PARAMS,
        timeout=10,
    )
    resp.raise_for_status()
    result = resp.text.strip()
    return int(result) if result else 0


class TestPipelineLatency:
    """Latency measurements for hot and cold paths."""

    @pytest.mark.performance
    def test_hot_path_event_visibility_latency(self):
        """Measure ingest->hot-topic visibility latency and assert practical budgets."""
        sample_count = int(os.getenv("LATENCY_HOT_SAMPLE_COUNT", "6"))
        per_event_timeout_sec = float(os.getenv("LATENCY_HOT_TIMEOUT_SEC", "20"))
        p95_budget_sec = float(os.getenv("LATENCY_HOT_P95_BUDGET_SEC", "10"))
        max_budget_sec = float(os.getenv("LATENCY_HOT_MAX_BUDGET_SEC", "15"))

        consumer = _make_consumer("telemetry-hot")
        latencies = []

        try:
            # Warm the consumer group before producing events.
            consumer.poll(timeout_ms=1000)

            for index in range(sample_count):
                marker = f"lat-hot-{uuid.uuid4().hex[:10]}-{index}"
                event = make_telemetry_event(
                    service_name="latency-hot-service",
                    severity="CRITICAL",
                    body=f"{marker} OutOfMemoryError: Java heap space",
                )

                start = time.monotonic()
                status = send_events_to_vector([event])[0]
                assert status in ALLOWED_STATUS_CODES, f"Unexpected HTTP status: {status}"

                found = _wait_for_topic_marker(consumer, marker, timeout_sec=per_event_timeout_sec)
                assert found, f"Marker '{marker}' not observed on telemetry-hot within timeout"
                latencies.append(time.monotonic() - start)
        finally:
            consumer.close()

        p50 = _percentile(latencies, 50)
        p95 = _percentile(latencies, 95)
        p99 = _percentile(latencies, 99)

        assert p95 <= p95_budget_sec, (
            f"Hot path p95 latency {p95:.3f}s exceeded budget {p95_budget_sec:.3f}s "
            f"(p50={p50:.3f}s, p99={p99:.3f}s, samples={len(latencies)})"
        )
        assert max(latencies) <= max_budget_sec, (
            f"Hot path max latency {max(latencies):.3f}s exceeded budget {max_budget_sec:.3f}s"
        )

    @pytest.mark.performance
    def test_cold_path_clickhouse_visibility_latency(self):
        """Measure ingest->ClickHouse visibility latency for cold-path events."""
        sample_count = int(os.getenv("LATENCY_COLD_SAMPLE_COUNT", "4"))
        per_event_timeout_sec = float(os.getenv("LATENCY_COLD_TIMEOUT_SEC", "40"))
        p95_budget_sec = float(os.getenv("LATENCY_COLD_P95_BUDGET_SEC", "30"))
        max_budget_sec = float(os.getenv("LATENCY_COLD_MAX_BUDGET_SEC", "40"))

        latencies = []

        for index in range(sample_count):
            marker = f"lat-cold-{uuid.uuid4().hex[:10]}-{index}"
            event = make_telemetry_event(
                service_name="latency-cold-service",
                severity="INFO",
                body=marker,
            )

            start = time.monotonic()
            status = send_events_to_vector([event])[0]
            assert status in ALLOWED_STATUS_CODES, f"Unexpected HTTP status: {status}"

            found = False
            deadline = time.monotonic() + per_event_timeout_sec
            while time.monotonic() < deadline:
                if _clickhouse_count_for_body(marker) >= 1:
                    found = True
                    latencies.append(time.monotonic() - start)
                    break
                time.sleep(0.5)

            assert found, f"Marker '{marker}' not visible in ClickHouse within timeout"

        p50 = _percentile(latencies, 50)
        p95 = _percentile(latencies, 95)
        p99 = _percentile(latencies, 99)

        assert p95 <= p95_budget_sec, (
            f"Cold path p95 latency {p95:.3f}s exceeded budget {p95_budget_sec:.3f}s "
            f"(p50={p50:.3f}s, p99={p99:.3f}s, samples={len(latencies)})"
        )
        assert max(latencies) <= max_budget_sec, (
            f"Cold path max latency {max(latencies):.3f}s exceeded budget {max_budget_sec:.3f}s"
        )
