from datetime import datetime, timezone
import os

import requests
from fastapi import FastAPI, Header

app = FastAPI(
    title="Chicken - User Service",
    version="1.0.0",
)

SERVICE_NAME = os.getenv("SERVICE_NAME", "user-service")

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
def process(trace_id: str | None = Header(default=None, alias="X-Trace-ID")):
    response = requests.get(
        f"{ORDER_SERVICE_URL}/health",
        headers={"X-Trace-ID": trace_id} if trace_id else {},
        timeout=2
    )

    return {
        "service": SERVICE_NAME,
        "trace_id": trace_id,
        "downstream": "order-service",
        "downstream_status": response.json(),
        "timestamp": datetime.now(timezone.utc).isoformat()
    }