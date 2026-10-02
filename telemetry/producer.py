import json
import random
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer


KAFKA_BOOTSTRAP = "localhost:29092"
TOPIC = "telemetry"

SERVICES = [
    "api-gateway",
    "user-service",
    "order-service",
    "payment-service",
    "inventory-service",
    "database-service",
]

ENDPOINTS = {
    "api-gateway": "/process",
    "user-service": "/process",
    "order-service": "/process",
    "payment-service": "/process",
    "inventory-service": "/health",
    "database-service": "/health",
}


producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
)


def generate_event():
    service = random.choice(SERVICES)

    latency = max(5, random.gauss(100, 25))
    cpu = max(5, min(100, random.gauss(50, 12)))
    memory = max(10, min(100, random.gauss(60, 10)))
    db_latency = max(1, random.gauss(20, 5))
    db_connections = max(1, int(random.gauss(30, 5)))

    status_code = 200
    error = False
    db_error = False

    trace_id = str(uuid.uuid4())
    request_id = str(uuid.uuid4())

    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "service": service,
        "request_id": request_id,
        "trace_id": trace_id,
        "endpoint": ENDPOINTS[service],
        "method": "GET",
        "latency_ms": round(latency, 2),
        "status_code": status_code,
        "error": error,
        "cpu_percent": round(cpu, 2),
        "memory_percent": round(memory, 2),
        "db_latency_ms": round(db_latency, 2),
        "db_connections": db_connections,
        "db_error": db_error,
    }


def main():
    print("Starting telemetry producer...")
    print(f"Kafka: {KAFKA_BOOTSTRAP}")
    print(f"Topic: {TOPIC}")
    print("Target throughput: 20 events/sec")

    interval = 1 / 20

    try:
        while True:
            start = time.perf_counter()

            event = generate_event()

            producer.send(TOPIC, value=event)

            elapsed = time.perf_counter() - start
            time.sleep(max(0, interval - elapsed))

    except KeyboardInterrupt:
        print("\nStopping producer...")

    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()