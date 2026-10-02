from datetime import datetime, timezone
import os

from fastapi import FastAPI, Header

app = FastAPI(
    title="Chicken - Database Service",
    version="1.0.0",
)

SERVICE_NAME = os.getenv("SERVICE_NAME", "database-service")


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