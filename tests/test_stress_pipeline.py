"""
Stress-focused integration tests for the IDOP ingestion pipeline.

These tests validate burst behavior under practical local load and check
for event loss on hot/cold routing paths.
"""

import json
import importlib.util
import os
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
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
REDPANDA_BROKER = _CONFTEST.REDPANDA_BROKER
VECTOR_AGG_URL = _CONFTEST.VECTOR_AGG_URL
make_telemetry_event = _CONFTEST.make_telemetry_event


ALLOWED_STATUS_CODES = {200, 201, 202, 204}


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
        group_id=f"stress-{uuid.uuid4().hex[:10]}",
        auto_offset_reset="latest",
        consumer_timeout_ms=1000,
        value_deserializer=_safe_json,
    )


def _post_event(event):
    try:
        resp = requests.post(VECTOR_AGG_URL, json=event, timeout=10)
        return resp.status_code
    except requests.RequestException:
        return 0


def _send_events_concurrently(events, workers):
    with ThreadPoolExecutor(max_workers=workers) as executor:
        return list(executor.map(_post_event, events))


def _consume_unique_bodies(consumer, body_prefix, expected_count, timeout_sec):
    seen = set()
    deadline = time.monotonic() + timeout_sec

    while time.monotonic() < deadline and len(seen) < expected_count:
        records = consumer.poll(timeout_ms=1000)
        for messages in records.values():
            for msg in messages:
                body = str((msg.value or {}).get("body", ""))
                if body.startswith(body_prefix):
                    seen.add(body)
    return seen


class TestPipelineStress:
    """Burst and no-loss assertions for pipeline routing topics."""

    @pytest.mark.performance
    def test_cold_path_burst_without_loss(self):
        """Send a cold-path burst and assert all events arrive on telemetry-cold."""
        event_count = int(os.getenv("STRESS_COLD_EVENT_COUNT", "250"))
        worker_count = int(os.getenv("STRESS_WORKERS", "24"))
        consume_timeout_sec = float(os.getenv("STRESS_COLD_TIMEOUT_SEC", "120"))
        drain_budget_sec = float(os.getenv("STRESS_COLD_DRAIN_BUDGET_SEC", "90"))

        marker_prefix = f"stress-cold-{uuid.uuid4().hex[:10]}"
        events = [
            make_telemetry_event(
                service_name="stress-cold-service",
                severity="INFO",
                body=f"{marker_prefix}-{index}",
            )
            for index in range(event_count)
        ]

        consumer = _make_consumer("telemetry-cold")
        try:
            # Warm consumer group before producing.
            consumer.poll(timeout_ms=1000)

            send_start = time.monotonic()
            statuses = _send_events_concurrently(events, workers=worker_count)
            assert all(code in ALLOWED_STATUS_CODES for code in statuses), (
                f"Send failures detected. Success={sum(code in ALLOWED_STATUS_CODES for code in statuses)} "
                f"Total={len(statuses)}"
            )

            drain_start = time.monotonic()
            seen = _consume_unique_bodies(
                consumer,
                body_prefix=marker_prefix,
                expected_count=event_count,
                timeout_sec=consume_timeout_sec,
            )
            drain_elapsed = time.monotonic() - drain_start
            total_elapsed = time.monotonic() - send_start

            assert len(seen) == event_count, (
                f"Cold-path event loss detected. Expected {event_count}, observed {len(seen)}"
            )
            assert drain_elapsed <= drain_budget_sec, (
                f"Cold-path drain time {drain_elapsed:.3f}s exceeded budget {drain_budget_sec:.3f}s "
                f"(total elapsed {total_elapsed:.3f}s)"
            )
        finally:
            consumer.close()

    @pytest.mark.performance
    def test_hot_path_burst_without_loss(self):
        """Send a hot-path burst and assert all events arrive on telemetry-hot."""
        event_count = int(os.getenv("STRESS_HOT_EVENT_COUNT", "150"))
        worker_count = int(os.getenv("STRESS_WORKERS", "24"))
        consume_timeout_sec = float(os.getenv("STRESS_HOT_TIMEOUT_SEC", "120"))
        drain_budget_sec = float(os.getenv("STRESS_HOT_DRAIN_BUDGET_SEC", "90"))

        marker_prefix = f"stress-hot-{uuid.uuid4().hex[:10]}"
        events = [
            make_telemetry_event(
                service_name="stress-hot-service",
                severity="CRITICAL",
                body=f"{marker_prefix}-{index} OutOfMemoryError: Java heap space",
            )
            for index in range(event_count)
        ]

        consumer = _make_consumer("telemetry-hot")
        try:
            # Warm consumer group before producing.
            consumer.poll(timeout_ms=1000)

            send_start = time.monotonic()
            statuses = _send_events_concurrently(events, workers=worker_count)
            assert all(code in ALLOWED_STATUS_CODES for code in statuses), (
                f"Send failures detected. Success={sum(code in ALLOWED_STATUS_CODES for code in statuses)} "
                f"Total={len(statuses)}"
            )

            drain_start = time.monotonic()
            seen = _consume_unique_bodies(
                consumer,
                body_prefix=marker_prefix,
                expected_count=event_count,
                timeout_sec=consume_timeout_sec,
            )
            drain_elapsed = time.monotonic() - drain_start
            total_elapsed = time.monotonic() - send_start

            assert len(seen) == event_count, (
                f"Hot-path event loss detected. Expected {event_count}, observed {len(seen)}"
            )
            assert drain_elapsed <= drain_budget_sec, (
                f"Hot-path drain time {drain_elapsed:.3f}s exceeded budget {drain_budget_sec:.3f}s "
                f"(total elapsed {total_elapsed:.3f}s)"
            )
        finally:
            consumer.close()
