import json
import statistics
import time
from kafka import KafkaConsumer


consumer = KafkaConsumer(
    "telemetry",
    bootstrap_servers="localhost:29092",
    auto_offset_reset="latest",
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
)

print()
print("=" * 70)
print("SPARK DETECTION LATENCY BENCHMARK")
print("=" * 70)
print("Waiting for failure-injection incidents...")
print("Start the failure injector in another PowerShell window.")
print("Run all 8 failure modes.")
print("Press Ctrl+C after the tests finish.")
print()


incidents = {}


def is_anomaly(event):
    return (
        event.get("cpu_percent", 0) >= 85
        or event.get("memory_percent", 0) >= 85
        or event.get("db_latency_ms", 0) >= 200
        or event.get("db_connections", 0) >= 80
        or event.get("status_code", 0) >= 500
        or event.get("latency_ms", 0) >= 300
    )


try:
    while True:

        records = consumer.poll(timeout_ms=1000)

        for _, messages in records.items():

            for message in messages:

                event = message.value

                incident_id = event.get("incident_id")

                if not incident_id:
                    continue

                failure_type = event.get(
                    "failure_type",
                    "unknown"
                )

                if incident_id not in incidents:

                    incidents[incident_id] = {
                        "failure_type": failure_type,
                        "start_time": time.time(),
                        "detected": False,
                        "detection_time": None,
                    }

                    print(
                        f"Incident started: {failure_type}"
                    )

                incident = incidents[incident_id]

                if (
                    not incident["detected"]
                    and is_anomaly(event)
                ):

                    incident["detected"] = True
                    incident["detection_time"] = time.time()

                    latency_ms = (
                        incident["detection_time"]
                        - incident["start_time"]
                    ) * 1000

                    incident["latency_ms"] = latency_ms

                    print(
                        f"  Detected: {failure_type:<35} "
                        f"{latency_ms:8.2f} ms"
                    )


except KeyboardInterrupt:
    print()
    print("Stopping benchmark...")

finally:
    consumer.close()


print()
print("=" * 70)
print("DETECTION LATENCY RESULTS")
print("=" * 70)


detected_latencies = []

for incident_id, data in incidents.items():

    if data["detected"]:

        latency = data["latency_ms"]

        detected_latencies.append(latency)

        print(
            f"{data['failure_type']:<35} "
            f"{latency:8.2f} ms"
        )

    else:

        print(
            f"{data['failure_type']:<35} "
            f"MISSED"
        )


print()
print("=" * 70)

total = len(incidents)
detected = len(detected_latencies)
missed = total - detected


print(f"Total incidents       : {total}")
print(f"Detected incidents    : {detected}")
print(f"Missed incidents      : {missed}")


if total > 0:

    recall = detected / total * 100

    print(
        f"Incident recall       : {recall:.2f}%"
    )


if detected_latencies:

    print(
        f"Mean detection latency   : "
        f"{statistics.mean(detected_latencies):.2f} ms"
    )

    print(
        f"Median detection latency : "
        f"{statistics.median(detected_latencies):.2f} ms"
    )

    print(
        f"Minimum detection latency: "
        f"{min(detected_latencies):.2f} ms"
    )

    print(
        f"Maximum detection latency: "
        f"{max(detected_latencies):.2f} ms"
    )

print("=" * 70)