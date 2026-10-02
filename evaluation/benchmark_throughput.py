import json
import time
from kafka import KafkaProducer

RATE = 20
DURATION = 60

producer = KafkaProducer(
    bootstrap_servers="localhost:29092",
    value_serializer=lambda x: json.dumps(x).encode("utf-8")
)

event = {
    "service": "order-service",
    "request_id": "benchmark",
    "trace_id": "benchmark",
    "endpoint": "/process",
    "method": "GET",
    "latency_ms": 100,
    "status_code": 200,
    "error": False,
    "cpu_percent": 40,
    "memory_percent": 50,
    "db_latency_ms": 20,
    "db_connections": 10,
    "db_error": False
}

print("THROUGHPUT BENCHMARK")
print("=" * 60)
print(f"Target rate : {RATE} events/sec")
print(f"Duration    : {DURATION} seconds")
print()

start = time.perf_counter()
count = 0

while time.perf_counter() - start < DURATION:

    producer.send("telemetry", event)
    count += 1

    elapsed = time.perf_counter() - start
    target_time = count / RATE

    sleep_time = target_time - elapsed

    if sleep_time > 0:
        time.sleep(sleep_time)

producer.flush()

elapsed = time.perf_counter() - start
actual_rate = count / elapsed
daily_equivalent = actual_rate * 86400
required_rate = 1_000_000 / 86400

print("=" * 60)
print(f"Events produced       : {count:,}")
print(f"Elapsed time          : {elapsed:.2f} seconds")
print(f"Actual throughput     : {actual_rate:.2f} events/sec")
print(f"Daily equivalent      : {daily_equivalent:,.0f} events/day")
print(f"1M/day requirement    : {required_rate:.2f} events/sec")
print("=" * 60)

producer.close()