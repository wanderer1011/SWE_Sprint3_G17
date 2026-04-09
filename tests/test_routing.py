"""
Test hot/cold/DLQ routing logic.

Verifies that the Vector Aggregator correctly routes events:
- Anomalous/error events → telemetry-hot
- Normal logs → telemetry-cold
- Invalid events (missing trace_id) → telemetry-dlq
"""

import json
import time
import uuid

import pytest

from conftest import (
    make_telemetry_event,
    send_events_to_vector,
    REDPANDA_BROKER,
)


def get_kafka_consumer(topic, group_id=None):
    """Create a Kafka consumer for the given topic."""
    from kafka import KafkaConsumer

    return KafkaConsumer(
        topic,
        bootstrap_servers=REDPANDA_BROKER,
        group_id=group_id or f"test-{uuid.uuid4().hex[:8]}",
        auto_offset_reset="latest",
        consumer_timeout_ms=10000,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )


class TestHotColdRouting:
    """Test that events are correctly routed to hot/cold paths."""

    def test_normal_log_routes_to_cold(self):
        """Normal INFO log should go to telemetry-cold."""
        consumer = get_kafka_consumer("telemetry-cold")
        # Consume any existing messages first
        for _ in consumer:
            pass

        event = make_telemetry_event(severity="INFO", body="Normal operation log")
        send_events_to_vector([event])

        time.sleep(3)
        messages = list(consumer)
        consumer.close()

        assert len(messages) > 0, "Expected event on telemetry-cold"

    def test_error_event_routes_to_hot(self):
        """CRITICAL severity event should go to telemetry-hot."""
        consumer = get_kafka_consumer("telemetry-hot")
        for _ in consumer:
            pass

        event = make_telemetry_event(
            severity="CRITICAL",
            body="OutOfMemoryError: Java heap space",
        )
        send_events_to_vector([event])

        time.sleep(3)
        messages = list(consumer)
        consumer.close()

        assert len(messages) > 0, "Expected event on telemetry-hot"

    def test_security_event_routes_to_hot(self):
        """Security-related log should be flagged and routed to hot."""
        consumer = get_kafka_consumer("telemetry-hot")
        for _ in consumer:
            pass

        event = make_telemetry_event(
            severity="WARN",
            body="Unauthorized access attempt detected - authentication failed",
        )
        send_events_to_vector([event])

        time.sleep(3)
        messages = list(consumer)
        consumer.close()

        assert len(messages) > 0, "Expected security event on telemetry-hot"


class TestDLQRouting:
    """Test that invalid events are routed to the dead letter queue."""

    def test_missing_trace_id_routes_to_dlq(self):
        """Event without trace_id should go to telemetry-dlq."""
        consumer = get_kafka_consumer("telemetry-dlq")
        for _ in consumer:
            pass

        # Send an event without trace_id
        invalid_event = {
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "severity": "INFO",
            "body": "Event missing trace_id",
            "service": {"name": "test-service"},
        }
        send_events_to_vector([invalid_event])

        time.sleep(3)
        messages = list(consumer)
        consumer.close()

        assert len(messages) > 0, "Expected invalid event on telemetry-dlq"
