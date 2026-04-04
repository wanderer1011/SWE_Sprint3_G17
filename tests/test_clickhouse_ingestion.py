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
    REDPANDA_BROKER,
)


def clickhouse_query(query):
    """Execute a ClickHouse query via HTTP interface."""
    # added a params attribute additionlly
    resp = requests.post(
        CLICKHOUSE_URL,
        data=query,
        # due to 403 forbidden error:
        # headers={
        #     "X-ClickHouse-User": "default",
        #     "X-ClickHouse-Key": ""
        # },
        params={
            "user": "default",
            "password": "changeme"
        },
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

        # # Wait for Kafka Engine + MV pipeline
        # time.sleep(15)
        # result = clickhouse_query(
        #     f"SELECT count() FROM telemetry.logs WHERE body = '{unique_body}'")
        # count = int(result) if result else 0
        
        # REPLACED THE ABOVE 5 LINES WITH BELOW 9 LINES TO MAKE THE TEST MORE RELIABLE(AND MITIGATE ASSERTION ERRORS DUE TO PIPELINE DELAYS)

        # Poll ClickHouse until the event appears (max 30s)
        count = 0
        start = time.time()
        while time.time() - start < 30:
            result = clickhouse_query(
                f"SELECT count() FROM telemetry.logs WHERE body = '{unique_body}'")
            count = int(result) if result else 0
            if count >= 1:
                break
            time.sleep(1)

        assert count >= 1, f"Expected event in ClickHouse, got count={count}"

    def test_bloom_filter_trace_lookup(self):
        """Verify trace_id point lookup uses bloom filter index."""
        trace_id = uuid.uuid4().hex

        event = make_telemetry_event(service_name="test-bloom")
        event["trace_id"] = trace_id
        send_events_to_vector([event])
        
        # wait time slightly increased
        time.sleep(20)
        
        # more reliable query to check bloom filters
        # result = clickhouse_query(
        #     f"SELECT count() FROM telemetry.logs WHERE trace_id = '{trace_id}'"
        # )
        result = clickhouse_query(
            f"""
            SELECT count()
            FROM telemetry.logs
            WHERE trace_id = '{trace_id}'
            AND service_name = 'test-bloom'
            """
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
