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

FAILURE_MODES = {
    "cpu_spike": "order-service",
    "memory_leak": "user-service",
    "database_latency": "database-service",
    "database_connection_exhaustion": "database-service",
    "http_5xx_spike": "payment-service",
    "network_latency": "inventory-service",
    "service_crash": "payment-service",
    "traffic_spike": "api-gateway",
}


producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda x: json.dumps(x).encode("utf-8"),
)


def event(service, failure_type=None, incident_id=None):
    latency = random.gauss(100, 20)
    cpu = random.gauss(50, 10)
    memory = random.gauss(60, 8)
    db_latency = random.gauss(20, 4)
    connections = random.gauss(30, 4)
    status = 200
    error = False
    db_error = False

    if failure_type == "cpu_spike":
        cpu = random.uniform(90, 99)
        latency *= 2

    elif failure_type == "memory_leak":
        memory = random.uniform(90, 99)
        latency *= 1.5

    elif failure_type == "database_latency":
        db_latency = random.uniform(300, 800)
        latency *= 3

    elif failure_type == "database_connection_exhaustion":
        connections = random.uniform(90, 100)
        db_error = random.random() < 0.5
        error = db_error
        status = 500 if error else 200

    elif failure_type == "http_5xx_spike":
        status = random.choice([500, 502, 503])
        error = True
        latency *= 2

    elif failure_type == "network_latency":
        latency = random.uniform(500, 1200)

    elif failure_type == "service_crash":
        status = 503
        error = True
        latency = random.uniform(800, 1500)

    elif failure_type == "traffic_spike":
        latency *= random.uniform(1.5, 3)

    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "service": service,
        "request_id": str(uuid.uuid4()),
        "trace_id": str(uuid.uuid4()),
        "endpoint": "/process",
        "method": "GET",
        "latency_ms": round(max(1, latency), 2),
        "status_code": status,
        "error": error,
        "cpu_percent": round(max(1, min(100, cpu)), 2),
        "memory_percent": round(max(1, min(100, memory)), 2),
        "db_latency_ms": round(max(1, db_latency), 2),
        "db_connections": int(max(1, connections)),
        "db_error": db_error,
        "failure_type": failure_type,
        "incident_id": incident_id,
    }


def run_incident(failure_type, duration=10):
    incident_id = str(uuid.uuid4())
    service = FAILURE_MODES[failure_type]

    print()
    print("=" * 60)
    print(f"INCIDENT: {failure_type}")
    print(f"SERVICE: {service}")
    print(f"INCIDENT ID: {incident_id}")
    print("=" * 60)

    start = time.time()

    while time.time() - start < duration:
        for _ in range(20):
            producer.send(
                TOPIC,
                event(
                    service,
                    failure_type,
                    incident_id,
                ),
            )

        time.sleep(1)

    producer.flush()

    print(f"Incident complete: {incident_id}")


def main():
    print("Failure injection started.")

    while True:
        failure_type = random.choice(list(FAILURE_MODES.keys()))

        run_incident(
            failure_type,
            duration=10,
        )

        print("Cooling down for 10 seconds...")
        time.sleep(10)


if __name__ == "__main__":
    main()