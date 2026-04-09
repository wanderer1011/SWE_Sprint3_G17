"""
Generate sample telemetry data for the IDOP Ingestion Layer.

Produces realistic telemetry events matching the Universal Telemetry Schema
and pushes them to the Vector Aggregator HTTP endpoint, directly to Redpanda,
or alternating between both.

Usage:
    python generate-test-data.py --mode http --count 1000
    python generate-test-data.py --mode kafka --count 5000
    python generate-test-data.py --mode mixed --count 2000
"""

import argparse
import json
import random
import sys
import uuid
from datetime import datetime, timezone

# ── Configuration ──
VECTOR_HTTP_URL = "http://localhost:8687"
REDPANDA_BROKER = "localhost:19092"

SERVICES = [
    "api-gateway", "user-service", "order-service",
    "payment-service", "inventory-service", "notification-service",
    "auth-service", "search-service",
]

SEVERITIES = ["DEBUG", "INFO", "INFO", "INFO", "WARN", "ERROR", "CRITICAL"]
SEVERITY_WEIGHTS = [5, 30, 30, 30, 15, 8, 2]

# OTel severity number mapping (1-24 scale)
SEVERITY_NUMBERS = {
    "TRACE": 1, "DEBUG": 5, "INFO": 9, "WARN": 13,
    "ERROR": 17, "CRITICAL": 21, "FATAL": 24,
}

CATEGORIES = ["otel-application", "otel-infrastructure", "legacy-syslog"]
SOURCE_TYPES = ["otel", "legacy"]
SIGNAL_TYPES = ["log", "trace"]
ENVIRONMENTS = ["production", "staging", "development"]
SERVICE_VERSIONS = ["v1.0.0", "v2.3.1", "v3.0.0-rc1", "unknown"]

LOG_TEMPLATES = [
    "Request processed successfully in {latency}ms",
    "Database query completed: {query_type} on {table}",
    "Cache hit for key: session:{session_id}",
    "Connection pool: {active}/{max} active connections",
    "GC pause detected: {gc_duration}ms stop-the-world",
    "HTTP {method} {path} responded with {status} in {latency}ms",
    "Authentication failed for user: redacted_user",
    "Connection timeout to downstream service: {target_service}",
    "OutOfMemoryError: Java heap space",
    "NullPointerException at com.idop.service.Handler.process",
    "Circuit breaker OPEN for {target_service}",
    "Rate limit exceeded for client: {client_id}",
    "TLS handshake completed with {target_service}",
    "Kafka consumer lag: {lag} messages on partition {partition}",
    "Unauthorized access attempt from IP: 10.0.x.x",
]


def generate_trace_id():
    return uuid.uuid4().hex


def generate_span_id():
    return uuid.uuid4().hex[:16]


def generate_event():
    """Generate a single telemetry event matching the Universal Telemetry Schema."""
    service = random.choice(SERVICES)
    severity = random.choices(SEVERITIES, weights=SEVERITY_WEIGHTS, k=1)[0]
    template = random.choice(LOG_TEMPLATES)

    latency = random.randint(1, 5000)
    gc_duration = random.randint(10, 500)
    status = random.choices([200, 201, 400, 404, 500, 502, 503], weights=[50, 10, 5, 5, 10, 5, 5], k=1)[0]

    body = template.format(
        latency=latency,
        query_type=random.choice(["SELECT", "INSERT", "UPDATE"]),
        table=random.choice(["users", "orders", "sessions"]),
        session_id=uuid.uuid4().hex[:8],
        active=random.randint(1, 50),
        max=50,
        gc_duration=gc_duration,
        method=random.choice(["GET", "POST", "PUT", "DELETE"]),
        path=random.choice(["/api/v1/users", "/api/v1/orders", "/api/v1/health"]),
        status=status,
        target_service=random.choice(SERVICES),
        client_id=uuid.uuid4().hex[:8],
        lag=random.randint(0, 10000),
        partition=random.randint(0, 11),
    )

    # Derive error_class from body content
    error_class = ""
    if "Exception" in body or "Error" in body:
        for token in body.split():
            if token.endswith("Exception") or token.endswith("Error"):
                error_class = token.rstrip(":,.")
                break
    if not error_class and severity in ("ERROR", "CRITICAL"):
        error_class = random.choice(["TimeoutError", "ConnectionError", "RuntimeError", ""])

    now_ns = int(datetime.now(timezone.utc).timestamp() * 1e9)

    event = {
        # Tier 1: Identity & Correlation
        "timestamp": str(now_ns),
        "trace_id": generate_trace_id(),
        "span_id": generate_span_id(),
        "parent_span_id": generate_span_id() if random.random() > 0.3 else "none",
        "service_name": service,
        "service_version": random.choice(SERVICE_VERSIONS),

        # Tier 2: Classification
        "severity_text": severity,
        "severity_number": SEVERITY_NUMBERS.get(severity, 9),
        "category": random.choice(CATEGORIES),
        "source_type": random.choice(SOURCE_TYPES),
        "signal_type": random.choice(SIGNAL_TYPES),
        "status_code": status if "responded with" in body else 0,

        # Tier 3: Content
        "body": body,
        "error_class": error_class,

        # Tier 4: Operational Context
        "host_name": f"node-{random.randint(1, 10)}",
        "environment": random.choice(ENVIRONMENTS),
        "deployment_id": f"deploy-{uuid.uuid4().hex[:12]}",
        "duration_ms": round(random.uniform(0.5, 5000.0), 1) if random.random() > 0.2 else -1.0,

        # Tier 5: Pipeline Metadata
        "pipeline_ts": str(now_ns + random.randint(100000, 1000000)),
        "pii_masked": True,
    }

    return event


def send_http(events):
    """Send events to Vector Aggregator via HTTP."""
    import urllib.request

    for i, event in enumerate(events):
        data = json.dumps(event).encode("utf-8")
        req = urllib.request.Request(
            VECTOR_HTTP_URL,
            data=data,
            headers={"Content-Type": "application/json"},
        )
        try:
            urllib.request.urlopen(req, timeout=5)
        except Exception as e:
            print(f"  Failed to send event {i}: {e}", file=sys.stderr)

        if (i + 1) % 100 == 0:
            print(f"  Sent {i + 1}/{len(events)} events")


def send_kafka(events):
    """Send events directly to Redpanda via kafka-python."""
    try:
        from kafka import KafkaProducer
    except ImportError:
        print("kafka-python not installed. Run: pip install kafka-python", file=sys.stderr)
        sys.exit(1)

    producer = KafkaProducer(
        bootstrap_servers=REDPANDA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
    )

    topic = "telemetry-cold"
    for i, event in enumerate(events):
        producer.send(topic, key=event.get("trace_id"), value=event)
        if (i + 1) % 500 == 0:
            print(f"  Produced {i + 1}/{len(events)} events to {topic}")

    producer.flush()
    producer.close()

def send_mixed(events):
    """Send events using both HTTP and Kafka (alternating per event)."""
    import urllib.request

    try:
        from kafka import KafkaProducer
    except ImportError:
        print("kafka-python not installed. Falling back to HTTP only.", file=sys.stderr)
        send_http(events)
        return

    producer = KafkaProducer(
        bootstrap_servers=REDPANDA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
    )

    topic = "telemetry-cold"
    http_count = 0
    kafka_count = 0

    for i, event in enumerate(events):
        if i % 2 == 0:
            data = json.dumps(event).encode("utf-8")
            req = urllib.request.Request(
                VECTOR_HTTP_URL,
                data=data,
                headers={"Content-Type": "application/json"},
            )
            try:
                urllib.request.urlopen(req, timeout=5)
                http_count += 1
            except Exception as e:
                print(f"  Failed HTTP send for event {i}: {e}", file=sys.stderr)
        else:
            try:
                producer.send(topic, key=event.get("trace_id"), value=event)
                kafka_count += 1
            except Exception as e:
                print(f"  Failed Kafka send for event {i}: {e}", file=sys.stderr)

        if (i + 1) % 200 == 0:
            print(
                f"  Processed {i + 1}/{len(events)} events "
                f"(HTTP: {http_count}, Kafka: {kafka_count})"
            )

    producer.flush()
    producer.close()


def main():
    parser = argparse.ArgumentParser(description="Generate IDOP test telemetry data")
    parser.add_argument("--mode", choices=["http", "kafka", "mixed"], default="http")
    parser.add_argument("--count", type=int, default=1000)
    args = parser.parse_args()

    print(f"Generating {args.count} telemetry events...")
    events = [generate_event() for _ in range(args.count)]
    print(f"Generated {len(events)} events. Sending via {args.mode}...")

    if args.mode == "http":
        send_http(events)
    elif args.mode == "kafka":
        send_kafka(events)
    else:
        send_mixed(events)

    print("Done.")


if __name__ == "__main__":
    main()