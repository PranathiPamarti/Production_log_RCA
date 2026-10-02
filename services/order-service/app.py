from datetime import datetime, timezone
import os
import uuid

import requests
from fastapi import FastAPI, Header, HTTPException

app = FastAPI(
    title="Chicken - Order Service",
    version="1.0.0",
)

SERVICE_NAME = os.getenv("SERVICE_NAME", "order-service")

INVENTORY_SERVICE_URL = os.getenv(
    "INVENTORY_SERVICE_URL",
    "http://inventory-service:8004"
)

PAYMENT_SERVICE_URL = os.getenv(
    "PAYMENT_SERVICE_URL",
    "http://payment-service:8003"
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
def process(
    trace_id: str | None = Header(default=None, alias="X-Trace-ID")
):
    if trace_id is None:
        trace_id = str(uuid.uuid4())

    inventory_response = requests.get(
        f"{INVENTORY_SERVICE_URL}/health",
        headers={"X-Trace-ID": trace_id},
        timeout=2
    )

    payment_response = requests.get(
        f"{PAYMENT_SERVICE_URL}/health",
        headers={"X-Trace-ID": trace_id},
        timeout=2
    )

    if inventory_response.status_code != 200:
        raise HTTPException(
            status_code=502,
            detail="Inventory service unavailable"
        )

    if payment_response.status_code != 200:
        raise HTTPException(
            status_code=502,
            detail="Payment service unavailable"
        )

    return {
        "service": SERVICE_NAME,
        "trace_id": trace_id,
        "dependencies": {
            "inventory-service": inventory_response.json(),
            "payment-service": payment_response.json()
        },
        "timestamp": datetime.now(timezone.utc).isoformat()
    }