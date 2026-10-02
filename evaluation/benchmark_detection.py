import json
import time
from collections import defaultdict
from kafka import KafkaConsumer


CONSUME_SECONDS_AFTER_LAST_INCIDENT = 15

consumer = KafkaConsumer(
    "telemetry",
    bootstrap_servers="localhost:29092",
    auto_offset_reset="latest",
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
)


def is_anomaly(event):
    return (
        event.get("cpu_percent", 0) >= 85
        or event.get("memory_percent", 0) >= 85
        or event.get("db_latency_ms", 0) >= 200
        or event.get("db_connections", 0) >= 80
        or event.get("status_code", 0) >= 500
        or event.get("latency_ms", 0) >= 300
    )


incidents = {}

print("DETECTION BENCHMARK")
print("=" * 60)
print("Waiting for injected incidents...")
print("Run failure_injection\\injector.py in another terminal.")
print("After the injector finishes, wait 15 seconds and press Ctrl+C.")
print()


try:

    last_incident_time = time.time()

    while True:

        records = consumer.poll(timeout_ms=1000)

        for _, messages in records.items():

            for message in messages:

                event = message.value

                incident_id = event.get("incident_id")
                failure_type = event.get("failure_type")

                if not incident_id:
                    continue

                if incident_id not in incidents:

                    incidents[incident_id] = {
                        "failure_type": failure_type,
                        "events": 0,
                        "anomalous_events": 0,
                    }

                    print(
                        f"Incident received: "
                        f"{failure_type}"
                    )

                incidents[incident_id]["events"] += 1

                if is_anomaly(event):

                    incidents[incident_id]["anomalous_events"] += 1

                last_incident_time = time.time()


except KeyboardInterrupt:

    print("\nStopping benchmark...")

finally:

    consumer.close()


print()
print("=" * 60)
print("DETECTION BENCHMARK RESULTS")
print("=" * 60)

total_incidents = len(incidents)
detected_incidents = 0

failure_results = defaultdict(
    lambda: {
        "total": 0,
        "detected": 0
    }
)

for incident_id, data in incidents.items():

    detected = data["anomalous_events"] > 0

    if detected:
        detected_incidents += 1

    failure_results[data["failure_type"]]["total"] += 1

    if detected:
        failure_results[data["failure_type"]]["detected"] += 1

    status = "DETECTED" if detected else "MISSED"

    print(
        f"{data['failure_type']:<35} "
        f"{status:<10} "
        f"events={data['events']}"
    )


print()
print("=" * 60)

if total_incidents > 0:

    recall = detected_incidents / total_incidents

    print(f"Total incidents    : {total_incidents}")
    print(f"Detected           : {detected_incidents}")
    print(f"Missed             : {total_incidents - detected_incidents}")
    print(f"Incident recall    : {recall * 100:.2f}%")

else:

    print("No incidents were captured.")


print()
print("BREAKDOWN BY FAILURE MODE")
print("=" * 60)

for failure_type, result in sorted(failure_results.items()):

    mode_recall = (
        result["detected"] / result["total"]
        if result["total"] > 0
        else 0
    )

    print(
        f"{failure_type:<35} "
        f"{result['detected']}/{result['total']} "
        f"({mode_recall * 100:.2f}%)"
    )

print("=" * 60)