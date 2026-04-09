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
import requests

from conftest import (
    make_telemetry_event,
    send_events_to_vector,
    REDPANDA_BROKER,
    VECTOR_AGG_URL,
)


def get_kafka_consumer(topic, group_id=None):
    """Create a Kafka consumer positioned at the live end of the topic.

    Uses assign() + end_offsets() + seek() (eager) instead of the lazy
    seek_to_end() so the consumer's fetch position is locked to the current
    log end BEFORE the test event is produced.  This prevents:
      - Decompressing existing snappy-compressed historical messages.
      - The race where lazy seek_to_end() resolves AFTER the test event is
        already written to Kafka, causing ListOffsets to return an offset past
        our event and miss it entirely.
    """
    from kafka import KafkaConsumer, TopicPartition

    consumer = KafkaConsumer(
        bootstrap_servers=REDPANDA_BROKER,
        group_id=group_id or f"test-{uuid.uuid4().hex[:8]}",
        auto_offset_reset="latest",
        consumer_timeout_ms=10000,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )

    # Wait for topic metadata (partitions_for_topic returns None until the
    # broker responds with metadata for the first time).
    partitions = None
    for _ in range(10):
        partitions = consumer.partitions_for_topic(topic)
        if partitions:
            break
        time.sleep(0.3)

    tps = [TopicPartition(topic, p) for p in sorted(partitions or [0])]
    consumer.assign(tps)

    # Eagerly resolve end offsets NOW (synchronous RPC) and seek explicitly.
    # Any message produced after this point will be visible on the next poll().
    end_offsets = consumer.end_offsets(tps)
    for tp, offset in end_offsets.items():
        consumer.seek(tp, offset)

    return consumer


class TestHotColdRouting:
    """Test that events are correctly routed to hot/cold paths."""

    def test_normal_log_routes_to_cold(self):
        """Normal INFO log should go to telemetry-cold."""
        consumer = get_kafka_consumer("telemetry-cold")

        event = make_telemetry_event(severity="INFO", body="Normal operation log")
        send_events_to_vector([event])

        messages = []
        start = time.time()
        while time.time() - start < 10:
            records = consumer.poll(timeout_ms=500)
            for tp, msgs in records.items():
                messages.extend(msgs)
            if messages:
                break
        consumer.close()

        assert len(messages) > 0, "Expected event on telemetry-cold"

    def test_error_event_routes_to_hot(self):
        """CRITICAL severity event should go to telemetry-hot."""
        consumer = get_kafka_consumer("telemetry-hot")

        event = make_telemetry_event(
            severity="CRITICAL",
            body="OutOfMemoryError: Java heap space",
        )
        send_events_to_vector([event])

        messages = []
        start = time.time()
        while time.time() - start < 10:
            records = consumer.poll(timeout_ms=500)
            for tp, msgs in records.items():
                messages.extend(msgs)
            if messages:
                break
        consumer.close()

        assert len(messages) > 0, "Expected event on telemetry-hot"

    def test_security_event_routes_to_hot(self):
        """Security-related log should be flagged and routed to hot."""
        consumer = get_kafka_consumer("telemetry-hot")

        event = make_telemetry_event(
            severity="WARN",
            body="Unauthorized access attempt detected - authentication failed",
        )
        send_events_to_vector([event])

        messages = []
        start = time.time()
        while time.time() - start < 10:
            records = consumer.poll(timeout_ms=500)
            for tp, msgs in records.items():
                messages.extend(msgs)
            if messages:
                break
        consumer.close()

        assert len(messages) > 0, "Expected security event on telemetry-hot"


class TestDLQRouting:
    """Test that invalid events are routed to the dead letter queue."""

    def test_missing_trace_id_routes_to_dlq(self):
        """Event without trace_id should go to telemetry-dlq."""
        consumer = get_kafka_consumer("telemetry-dlq")

        # Send an event without trace_id.
        # Use a 30-second timeout: Vector's HTTP source has acknowledgements
        # enabled, so it holds the response until the event is flushed to
        # Kafka, which can take >10 s when the broker is under load.
        invalid_event = {
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "severity": "INFO",
            "body": "Event missing trace_id",
            "service": {"name": "test-service"},
        }
        resp = requests.post(VECTOR_AGG_URL, json=invalid_event, timeout=30)
        assert resp.status_code in (200, 201, 202, 204), (
            f"Unexpected status sending invalid event: {resp.status_code}"
        )

        messages = []
        start = time.time()
        while time.time() - start < 15:
            records = consumer.poll(timeout_ms=500)
            for tp, msgs in records.items():
                messages.extend(msgs)
            if messages:
                break
        consumer.close()

        assert len(messages) > 0, "Expected invalid event on telemetry-dlq"











# """
# Test hot/cold/DLQ routing logic.

# Verifies that the Vector Aggregator correctly routes events:
# - Anomalous/error events → telemetry-hot
# - Normal logs → telemetry-cold
# - Invalid events (missing trace_id) → telemetry-dlq
# """

# import json
# import time
# import uuid

# import pytest

# from conftest import (
#     make_telemetry_event,
#     send_events_to_vector,
#     REDPANDA_BROKER,
# )


# def get_kafka_consumer(topic, group_id=None):
#     """Create a Kafka consumer for the given topic."""
#     from kafka import KafkaConsumer

#     return KafkaConsumer(
#         topic,
#         bootstrap_servers=REDPANDA_BROKER,
#         group_id=group_id or f"test-{uuid.uuid4().hex[:8]}",
#         auto_offset_reset="latest",
#         consumer_timeout_ms=10000,
#         value_deserializer=lambda m: json.loads(m.decode("utf-8")),
#     )


# class TestHotColdRouting:
#     """Test that events are correctly routed to hot/cold paths."""

#     def test_normal_log_routes_to_cold(self):
#         """Normal INFO log should go to telemetry-cold."""
#         consumer = get_kafka_consumer("telemetry-cold")
        
#         # # Consume any existing messages first
#         # for _ in consumer:
#         #     pass
#         # replace the above 3 lines with below line(This initializes the consumer without draining the topic.)
#         consumer.poll(timeout_ms=1000)

#         event = make_telemetry_event(severity="INFO", body="Normal operation log")
#         send_events_to_vector([event])

#         time.sleep(3)
#         # wait untill kafka actually produces a msg
#         # messages = list(consumer)
#         messages = []
#         start = time.time()

#         while time.time() - start < 8:
#             records = consumer.poll(timeout_ms=500)
    
#             for tp, msgs in records.items():
#                 messages.extend(msgs)

#             if messages:
#                 break

#         consumer.close()

#         assert len(messages) > 0, "Expected event on telemetry-cold"

#     def test_error_event_routes_to_hot(self):
#         """CRITICAL severity event should go to telemetry-hot."""
#         consumer = get_kafka_consumer("telemetry-hot")
#         for _ in consumer:
#             pass

#         event = make_telemetry_event(
#             severity="CRITICAL",
#             body="OutOfMemoryError: Java heap space",
#         )
#         send_events_to_vector([event])

#         time.sleep(3)
        
#         # wait until kafka actually produces a msg
#         # messages = list(consumer)
#         messages = []
#         start = time.time()

#         while time.time() - start < 8:
#             records = consumer.poll(timeout_ms=500)

#             for tp, msgs in records.items():
#                 messages.extend(msgs)

#             if messages:
#                 break
#         consumer.close()

#         assert len(messages) > 0, "Expected event on telemetry-hot"

#     def test_security_event_routes_to_hot(self):
#         """Security-related log should be flagged and routed to hot."""
#         consumer = get_kafka_consumer("telemetry-hot")
#         for _ in consumer:
#             pass

#         event = make_telemetry_event(
#             severity="WARN",
#             body="Unauthorized access attempt detected - authentication failed",
#         )
#         send_events_to_vector([event])

#         time.sleep(3)
#         messages = list(consumer)
#         consumer.close()

#         assert len(messages) > 0, "Expected security event on telemetry-hot"


# class TestDLQRouting:
#     """Test that invalid events are routed to the dead letter queue."""

#     def test_missing_trace_id_routes_to_dlq(self):
#         """Event without trace_id should go to telemetry-dlq."""
#         consumer = get_kafka_consumer("telemetry-dlq")
#         for _ in consumer:
#             pass

#         # Send an event without trace_id
#         invalid_event = {
#             "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
#             "severity": "INFO",
#             "body": "Event missing trace_id",
#             "service": {"name": "test-service"},
#         }
#         send_events_to_vector([invalid_event])

#         time.sleep(3)
#         messages = list(consumer)
#         consumer.close()

#         assert len(messages) > 0, "Expected invalid event on telemetry-dlq"
