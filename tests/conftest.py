"""
Shared test fixtures for IDOP Ingestion Layer integration tests.
"""

import json
import os
import time
import uuid

import pytest
import requests


# ── Service URLs (from docker-compose) ──
# changed Vector Aggregator port from 8686 to 8687, that is correct port, vector aggregator listens on it
VECTOR_AGG_URL = os.getenv("VECTOR_AGG_URL", "http://localhost:8687")
REDPANDA_BROKER = os.getenv("REDPANDA_BROKER", "localhost:19092")
REDPANDA_ADMIN_URL = os.getenv("REDPANDA_ADMIN_URL", "http://localhost:9644")
CLICKHOUSE_URL = os.getenv("CLICKHOUSE_URL", "http://localhost:8123")
# ClickHouse container is running correctly, but the HTTP interface likely requires authentication.
# CLICKHOUSE_URL = os.getenv("CLICKHOUSE_URL", "http://default:@localhost:8123")
FLINK_URL = os.getenv("FLINK_URL", "http://localhost:8081")


def wait_for_service(url, timeout=60, interval=2):
    """Wait for a service to become healthy."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            resp = requests.get(url, timeout=5)
            if resp.status_code < 500:
                return True
        except requests.ConnectionError:
            pass
        time.sleep(interval)
    pytest.skip(f"Service at {url} not available after {timeout}s")


@pytest.fixture(scope="session", autouse=True)
def ensure_services():
    """Ensure all services are running before tests."""
    # redpanda status endpoint changed from cluster/health to status/ready in latest versions
    wait_for_service(f"{REDPANDA_ADMIN_URL}/v1/cluster/health_overview")
    wait_for_service(f"{CLICKHOUSE_URL}/ping")


def make_telemetry_event(
    service_name="test-service",
    severity="INFO",
    body="Test log event",
    is_anomalous=False,
):
    """Create a valid telemetry event for testing."""
    return {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "trace_id": uuid.uuid4().hex,
        "span_id": uuid.uuid4().hex[:16],
        "service": {"name": service_name},
        "severity": severity,
        "body": body,
    }


def send_events_to_vector(events, url=VECTOR_AGG_URL):
    """Send events to Vector Aggregator via HTTP."""
    results = []
    for event in events:
        resp = requests.post(url, json=event, timeout=10)
        results.append(resp.status_code)
    return results
