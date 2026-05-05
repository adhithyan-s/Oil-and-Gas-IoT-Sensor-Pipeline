"""
Anomaly Detection — FastAPI endpoint

Exposes the trained Isolation Forest model as a REST API.
Integrates with the existing IoT pipeline's PostgreSQL and MinIO.

Endpoints:
  GET  /health              — service health check
  GET  /anomaly/{well_id}   — score a single well
  GET  /anomaly/all         — score all wells
"""

import logging
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List
import uvicorn

from predict import predict

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

app = FastAPI(
    title="IoT Anomaly Detection API",
    description="Isolation Forest anomaly detection on oilfield sensor data",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

WELLS = ["WELL-001", "WELL-002", "WELL-003", "WELL-004", "WELL-005"]


class SensorContribution(BaseModel):
    sensor: str
    deviation: float


class AnomalyResult(BaseModel):
    well_id: str
    is_anomaly: bool
    anomaly_score: float
    risk_level: str
    health_score: float
    top_sensors: List[SensorContribution]


@app.get("/health")
def health_check():
    return {"status": "ok", "model": "IsolationForest", "version": "1.0.0"}


@app.get("/anomaly/{well_id}", response_model=AnomalyResult)
def score_well(well_id: str):
    """
    Score a single well for anomalous sensor behaviour.
    Reads latest sensor data from Silver layer and returns anomaly score, risk level, and top contributing sensors.
    """
    if well_id not in WELLS:
        raise HTTPException(
            status_code=404,
            detail=f"Well {well_id} not found. Valid wells: {WELLS}"
        )
    try:
        result = predict(well_id)
        return result
    except Exception as e:
        log.error(f"Prediction failed for {well_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/anomaly", response_model=List[AnomalyResult])
def score_all_wells():
    """
    Score all 5 wells and return results sorted by anomaly score descending.
    Most at-risk wells appear first.
    """
    results = []
    for well_id in WELLS:
        try:
            results.append(predict(well_id))
        except Exception as e:
            log.error(f"Skipping {well_id}: {e}")

    return sorted(results, key=lambda x: x["anomaly_score"], reverse=True)


if __name__ == "__main__":
    uvicorn.run("api:app", host="0.0.0.0", port=8001, reload=True)