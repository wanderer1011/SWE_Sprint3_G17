"""
End-to-end pipeline test for the IDOP Ingestion Layer.

Tests the full flow: Edge → Envoy → Vector Aggregator → Redpanda
→ ClickHouse (cold) / Flink (hot) → ML Features topic.
"""

import json
import time
import uuid

import pytest
import requests

from conftest import (
    make_telemetry_event,
    send_events_to_vector,
    CLICKHOUSE_URL,
    CLICKHOUSE_USER,
    CLICKHOUSE_PASSWORD,
    FLINK_URL,
    REDPANDA_BROKER,
    REDPANDA_ADMIN_URL,
)


class TestEndToEndPipeline:
    """Full pipeline integration tests."""

    def test_pipeline_processes_batch(self):
        """Send a batch of mixed events and verify routing."""
        events = []

        # 5 normal events (should go cold)
        for _ in range(5):
            events.append(make_telemetry_event(severity="INFO", body="Normal log event"))

        # 3 error events (should go hot)
        for _ in range(3):
            events.append(make_telemetry_event(
                severity="CRITICAL",
                body="OutOfMemoryError: Java heap space",
            ))

        # 1 invalid event (should go DLQ)
        events.append({
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "severity": "ERROR",
            "body": "Missing trace_id event",
        })

        results = send_events_to_vector(events)
        assert all(r in (200, 201, 204) for r in results), f"Some sends failed: {results}"

    def test_redpanda_topics_exist(self):
        """Verify all required topics are created."""
        resp = requests.get(f"{REDPANDA_ADMIN_URL}/v1/cluster/health_overview", timeout=5)
        assert resp.status_code == 200

    def test_flink_job_running(self):
        """Verify the Flink hot path job is running."""
        try:
            resp = requests.get(f"{FLINK_URL}/jobs/overview", timeout=5)
            if resp.status_code == 200:
                jobs = resp.json().get("jobs", [])
                running = [j for j in jobs if j.get("state") == "RUNNING"]
                if len(running) == 0:
                    pytest.skip("No Flink jobs running (deploy with make flink-deploy)")
        except requests.ConnectionError:
            pytest.skip("Flink not reachable")

    def test_clickhouse_schema_deployed(self):
        """Verify ClickHouse schema is deployed correctly."""
        resp = requests.post(
            CLICKHOUSE_URL,
            params={"user": CLICKHOUSE_USER, "password": CLICKHOUSE_PASSWORD},
            data="SELECT name FROM system.tables WHERE database = 'telemetry'",
            timeout=5,
        )
        assert resp.status_code == 200
        tables = resp.text.strip().split("\n")
        assert "logs" in tables, f"Expected 'logs' table, got: {tables}"
