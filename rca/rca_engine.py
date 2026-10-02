import networkx as nx
import pandas as pd


# ---------------------------------------------------------
# SERVICE DEPENDENCY GRAPH
# ---------------------------------------------------------

GRAPH = nx.DiGraph()

GRAPH.add_edges_from([
    ("api-gateway", "user-service"),
    ("api-gateway", "order-service"),
    ("user-service", "order-service"),
    ("order-service", "inventory-service"),
    ("order-service", "payment-service"),
    ("payment-service", "database-service"),
])


# ---------------------------------------------------------
# FAILURE ROOT CAUSE PROFILES
# ---------------------------------------------------------

ROOT_PROFILES = {
    "cpu_spike": "order-service",
    "memory_leak": "user-service",
    "database_latency": "database-service",
    "database_connection_exhaustion": "database-service",
    "http_5xx_spike": "payment-service",
    "network_latency": "inventory-service",
    "service_crash": "payment-service",
    "traffic_spike": "api-gateway",
}


# ---------------------------------------------------------
# RCA FEATURE ENGINEERING
# ---------------------------------------------------------

def calculate_features(row):

    features = {
        "anomaly_severity": row.get("anomaly_score", 0),
        "error_contribution": row.get("error_rate", 0),
        "latency_contribution": row.get("avg_latency", 0) / 1000,
        "cpu_pressure": row.get("cpu_percent", 0) / 100,
        "memory_pressure": row.get("memory_percent", 0) / 100,
        "db_latency_pressure": row.get("db_latency", 0) / 500,
        "db_connection_pressure": (
            row.get("db_connection_count", 0) / 100
        ),
        "p95_latency": row.get("p95_latency", 0) / 1000,
        "p99_latency": row.get("p99_latency", 0) / 1000,
        "http_5xx_rate": row.get("http_5xx_rate", 0),
        "db_error_rate": row.get("db_error_rate", 0),
        "request_rate": row.get("request_rate", 0),
    }

    return features


# ---------------------------------------------------------
# ROOT CAUSE RANKING
# ---------------------------------------------------------

def rank_root_causes(observations):

    scores = {}

    for service, row in observations.items():

        f = calculate_features(row)

        direct_signal = (
            f["anomaly_severity"] * 0.25
            + f["error_contribution"] * 0.15
            + f["latency_contribution"] * 0.10
            + f["cpu_pressure"] * 0.10
            + f["memory_pressure"] * 0.10
            + f["db_latency_pressure"] * 0.10
            + f["db_connection_pressure"] * 0.05
            + f["http_5xx_rate"] * 0.05
            + f["db_error_rate"] * 0.05
            + f["request_rate"] / 100 * 0.05
        )

        # Services with many downstream dependencies
        # can affect a larger portion of the system.
        blast_radius = len(
            nx.descendants(GRAPH, service)
        )

        blast_score = blast_radius / max(
            1,
            len(GRAPH.nodes)
        )

        # Upstream influence.
        upstream_count = len(
            nx.ancestors(GRAPH, service)
        )

        upstream_score = upstream_count / max(
            1,
            len(GRAPH.nodes)
        )

        final_score = (
            direct_signal * 0.70
            + blast_score * 0.20
            + upstream_score * 0.10
        )

        scores[service] = {
            "direct_signal": round(
                direct_signal, 4
            ),
            "blast_radius": blast_radius,
            "upstream_dependencies": upstream_count,
            "root_cause_score": round(
                final_score, 4
            ),
        }

    ranked = sorted(
        scores.items(),
        key=lambda x: x[1]["root_cause_score"],
        reverse=True,
    )

    return ranked


# ---------------------------------------------------------
# DEMO / TEST
# ---------------------------------------------------------

if __name__ == "__main__":

    observations = {

        "api-gateway": {
            "anomaly_score": 1,
            "error_rate": 0.1,
            "avg_latency": 180,
            "cpu_percent": 60,
            "memory_percent": 60,
            "db_latency": 20,
            "db_connection_count": 30,
            "p95_latency": 250,
            "p99_latency": 300,
            "http_5xx_rate": 0.05,
            "db_error_rate": 0,
            "request_rate": 20,
        },

        "payment-service": {
            "anomaly_score": 5,
            "error_rate": 0.7,
            "avg_latency": 700,
            "cpu_percent": 70,
            "memory_percent": 65,
            "db_latency": 300,
            "db_connection_count": 40,
            "p95_latency": 900,
            "p99_latency": 1200,
            "http_5xx_rate": 0.6,
            "db_error_rate": 0.2,
            "request_rate": 20,
        },

        "database-service": {
            "anomaly_score": 2,
            "error_rate": 0.2,
            "avg_latency": 400,
            "cpu_percent": 70,
            "memory_percent": 70,
            "db_latency": 500,
            "db_connection_count": 90,
            "p95_latency": 700,
            "p99_latency": 900,
            "http_5xx_rate": 0.1,
            "db_error_rate": 0.4,
            "request_rate": 20,
        },
    }

    ranked = rank_root_causes(observations)

    print("\nROOT CAUSE RANKING")
    print("=" * 60)

    for rank, (service, data) in enumerate(
        ranked,
        start=1
    ):
        print(
            f"{rank}. {service} "
            f"score={data['root_cause_score']} "
            f"blast_radius={data['blast_radius']}"
        )