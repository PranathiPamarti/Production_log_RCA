import json
import statistics
from kafka import KafkaConsumer


consumer = KafkaConsumer(
    "spark-detections",
    bootstrap_servers="localhost:29092",
    auto_offset_reset="latest",
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode("utf-8")),
)

print()
print("=" * 70)
print("SPARK DETECTION LATENCY BENCHMARK")
print("=" * 70)
print("Waiting for Spark anomaly detections...")
print("Run the failure injector in another terminal.")
print("Run all 8 failure modes.")
print("Press Ctrl+C when finished.")
print()


incidents = {}


try:
    while True:

        records = consumer.poll(timeout_ms=1000)

        for _, messages in records.items():

            for message in messages:

                event = message.value

                incident_id = event.get(
                    "ground_truth_incident"
                )

                failure_type = event.get(
                    "ground_truth_failure"
                )

                predicted = event.get(
                    "predicted_anomaly",
                    0
                )

                latency = event.get(
                    "detection_latency_ms"
                )

                if not incident_id:
                    continue

                if predicted != 1:
                    continue

                if latency is None:
                    continue

                if incident_id not in incidents:

                    incidents[incident_id] = {
                        "failure_type": failure_type,
                        "latency_ms": float(latency)
                    }

                    print(
                        f"Detected: "
                        f"{failure_type:<35} "
                        f"{float(latency):8.2f} ms"
                    )

                else:

                    # Keep the earliest Spark detection.
                    if float(latency) < incidents[incident_id]["latency_ms"]:

                        incidents[incident_id][
                            "latency_ms"
                        ] = float(latency)


except KeyboardInterrupt:

    print()
    print("Stopping benchmark...")

finally:

    consumer.close()


print()
print("=" * 70)
print("SPARK DETECTION LATENCY RESULTS")
print("=" * 70)


latencies = []

for incident_id, data in incidents.items():

    latency = data["latency_ms"]

    latencies.append(latency)

    print(
        f"{data['failure_type']:<35} "
        f"{latency:8.2f} ms"
    )


print()
print("=" * 70)

print(
    f"Incidents detected : {len(latencies)}"
)


if latencies:

    print(
        f"Mean latency       : "
        f"{statistics.mean(latencies):.2f} ms"
    )

    print(
        f"Median latency     : "
        f"{statistics.median(latencies):.2f} ms"
    )

    print(
        f"Minimum latency    : "
        f"{min(latencies):.2f} ms"
    )

    print(
        f"Maximum latency    : "
        f"{max(latencies):.2f} ms"
    )

print("=" * 70)