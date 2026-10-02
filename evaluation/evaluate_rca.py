import random
import time

from rca.rca_engine import rank_root_causes


FAILURES = {
    "cpu_spike": "order-service",
    "memory_leak": "user-service",
    "database_latency": "database-service",
    "database_connection_exhaustion": "database-service",
    "http_5xx_spike": "payment-service",
    "network_latency": "inventory-service",
    "service_crash": "payment-service",
    "traffic_spike": "api-gateway",
}


def make_observations(root_service):

    services = [
        "api-gateway",
        "user-service",
        "order-service",
        "payment-service",
        "inventory-service",
        "database-service",
    ]

    observations = {}

    for service in services:

        # Normal background signal
        observations[service] = {
            "anomaly_score": random.choice([0, 0, 1]),
            "error_rate": random.uniform(0, 0.05),
            "avg_latency": random.uniform(80, 150),
            "cpu_percent": random.uniform(30, 65),
            "memory_percent": random.uniform(40, 70),
            "db_latency": random.uniform(10, 40),
            "db_connection_count": random.uniform(20, 50),
            "p95_latency": random.uniform(150, 250),
            "p99_latency": random.uniform(200, 300),
            "http_5xx_rate": random.uniform(0, 0.02),
            "db_error_rate": random.uniform(0, 0.02),
            "request_rate": random.uniform(10, 30),
        }

    # Inject strong root-cause signal
    r = observations[root_service]

    r["anomaly_score"] = 6
    r["error_rate"] = 0.8
    r["avg_latency"] = 700
    r["cpu_percent"] = 95
    r["memory_percent"] = 95
    r["db_latency"] = 500
    r["db_connection_count"] = 95
    r["p95_latency"] = 900
    r["p99_latency"] = 1200
    r["http_5xx_rate"] = 0.7
    r["db_error_rate"] = 0.5
    r["request_rate"] = 100

    return observations


def main():

    results = []

    print("\nRCA EVALUATION")
    print("=" * 60)

    for failure_type, expected_root in FAILURES.items():

        observations = make_observations(
            expected_root
        )

        start = time.perf_counter()

        ranked = rank_root_causes(
            observations
        )

        elapsed = (
            time.perf_counter() - start
        )

        predicted_root = ranked[0][0]

        correct = (
            predicted_root == expected_root
        )

        results.append({
            "failure": failure_type,
            "expected": expected_root,
            "predicted": predicted_root,
            "correct": correct,
            "time_ms": elapsed * 1000,
        })

        print(
            f"{failure_type:40} "
            f"expected={expected_root:20} "
            f"predicted={predicted_root:20} "
            f"{'PASS' if correct else 'FAIL'}"
        )

    accuracy = (
        sum(r["correct"] for r in results)
        / len(results)
        * 100
    )

    mean_time = (
        sum(r["time_ms"] for r in results)
        / len(results)
    )

    print("\n" + "=" * 60)
    print(f"RCA top-1 accuracy : {accuracy:.2f}%")
    print(f"Mean RCA time      : {mean_time:.3f} ms")
    print("=" * 60)


if __name__ == "__main__":
    main()