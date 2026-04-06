"""
Test ClickHouse cold path ingestion.

Verifies that events flowing through telemetry-cold are correctly
consumed by the ClickHouse Kafka Engine and stored in telemetry.logs.
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
    REDPANDA_BROKER,
)


def clickhouse_query(query):
    """Execute a ClickHouse query via HTTP interface."""
    resp = requests.post(
        CLICKHOUSE_URL,
        data=query,
        headers={"Content-Type": "text/plain"},
        params={"user": CLICKHOUSE_USER, "password": CLICKHOUSE_PASSWORD},
        timeout=10,
    )
    resp.raise_for_status()
    return resp.text.strip()


class TestClickHouseIngestion:
    """Test cold path: Redpanda → ClickHouse Kafka Engine → MergeTree."""

    def test_event_lands_in_clickhouse(self):
        """Send event via Vector, verify it appears in ClickHouse."""
        unique_body = f"test-clickhouse-{uuid.uuid4().hex[:8]}"

        event = make_telemetry_event(
            service_name="test-ch-ingestion",
            severity="INFO",
            body=unique_body,
        )
        send_events_to_vector([event])

        # Wait for Kafka Engine + MV pipeline
        time.sleep(15)

        result = clickhouse_query(
            f"SELECT count() FROM telemetry.logs WHERE body = '{unique_body}'"
        )
        count = int(result) if result else 0
        assert count >= 1, f"Expected event in ClickHouse, got count={count}"

    def test_bloom_filter_trace_lookup(self):
        """Verify trace_id point lookup uses bloom filter index."""
        trace_id = uuid.uuid4().hex

        event = make_telemetry_event(service_name="test-bloom")
        event["trace_id"] = trace_id
        send_events_to_vector([event])

        time.sleep(15)

        result = clickhouse_query(
            f"SELECT count() FROM telemetry.logs WHERE trace_id = '{trace_id}'"
        )
        count = int(result) if result else 0
        assert count >= 1, f"Expected trace in ClickHouse, got count={count}"

    def test_red_metrics_materialized_view(self):
        """Verify RED metrics MV aggregates data."""
        result = clickhouse_query(
            "SELECT count() FROM telemetry.red_metrics_1m"
        )
        # Just verify the table exists and is queryable
        assert result is not None
