from datetime import datetime, timezone
import os
import uuid

import requests
from fastapi import FastAPI, Header

app = FastAPI(
    title="Chicken - Payment Service",
    version="1.0.0",
)

SERVICE_NAME = os.getenv("SERVICE_NAME", "payment-service")

DATABASE_SERVICE_URL = os.getenv(
    "DATABASE_SERVICE_URL",
    "http://database-service:8005"
)


@app.get("/")
def root():
    return {
        "service": SERVICE_NAME,
        "status": "running"
    }


@app.get("/health")
def health(trace_id: str | None = Header(default=None, alias="X-Trace-ID")):
    return {
        "service": SERVICE_NAME,
        "status": "healthy",
        "trace_id": trace_id,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }


@app.get("/process")
def process(trace_id: str | None = Header(default=None, alias="X-Trace-ID")):
    if trace_id is None:
        trace_id = str(uuid.uuid4())

    database_response = requests.get(
        f"{DATABASE_SERVICE_URL}/health",
        headers={"X-Trace-ID": trace_id},
        timeout=2
    )

    return {
        "service": SERVICE_NAME,
        "trace_id": trace_id,
        "downstream": {
            "database-service": database_response.json()
        },
        "timestamp": datetime.now(timezone.utc).isoformat()
    }