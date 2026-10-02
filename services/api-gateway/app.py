from datetime import datetime, timezone
import os
import uuid

import requests
from fastapi import FastAPI

app = FastAPI(
    title="Chicken - Production Log Intelligence",
    version="1.0.0",
)

SERVICE_NAME = os.getenv("SERVICE_NAME", "api-gateway")

USER_SERVICE_URL = os.getenv(
    "USER_SERVICE_URL",
    "http://user-service:8001"
)

ORDER_SERVICE_URL = os.getenv(
    "ORDER_SERVICE_URL",
    "http://order-service:8002"
)


@app.get("/")
def root():
    return {
        "service": SERVICE_NAME,
        "status": "running"
    }


@app.get("/health")
def health():
    return {
        "service": SERVICE_NAME,
        "status": "healthy",
        "timestamp": datetime.now(timezone.utc).isoformat()
    }


@app.get("/process")
def process():
    trace_id = str(uuid.uuid4())

    user_response = requests.get(
        f"{USER_SERVICE_URL}/process",
        headers={"X-Trace-ID": trace_id},
        timeout=5
    )

    order_response = requests.get(
        f"{ORDER_SERVICE_URL}/process",
        headers={"X-Trace-ID": trace_id},
        timeout=5
    )

    return {
        "service": SERVICE_NAME,
        "trace_id": trace_id,
        "dependencies": {
            "user-service": user_response.json(),
            "order-service": order_response.json()
        },
        "timestamp": datetime.now(timezone.utc).isoformat()
    }