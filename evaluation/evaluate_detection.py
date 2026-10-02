import json
import time
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    "telemetry",
    bootstrap_servers="localhost:29092",
    auto_offset_reset="latest",
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
)

print("DETECTION EVALUATION")
print("=" * 60)
print("Waiting for incidents...")
print("Run the failure injector in another terminal.")
print("Press Ctrl+C after the injector finishes.")
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
                failure_type = event.get("failure_type")

                if not incident_id:
                    continue

                if incident_id not in incidents:

                    incidents[incident_id] = {
                        "failure_type": failure_type,
                        "detected": False
                    }

                    print(
                        f"Incident received: {failure_type}"
                    )

                if is_anomaly(event):
                    incidents[incident_id]["detected"] = True

except KeyboardInterrupt:

    print("\nStopping evaluation...")

finally:

    consumer.close()


print()
print("=" * 60)
print("DETECTION RESULTS")
print("=" * 60)

detected = 0

for incident_id, data in incidents.items():

    status = "DETECTED" if data["detected"] else "MISSED"

    if data["detected"]:
        detected += 1

    print(
        f"{data['failure_type']:<35} {status}"
    )

total = len(incidents)

print()
print("=" * 60)

if total > 0:

    recall = detected / total * 100

    print(f"Total incidents       : {total}")
    print(f"Detected incidents    : {detected}")
    print(f"Missed incidents      : {total - detected}")
    print(f"Incident recall       : {recall:.2f}%")

else:

    print("No incidents captured.")

print("=" * 60)