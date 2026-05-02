"""
Anomaly Detection — Prediction Module

Loads the trained Isolation Forest model and scores a well's latest sensor readings from PostgreSQL.

Returns:
  - anomaly_score: float (higher = more anomalous)
  - is_anomaly: bool
  - risk_level: LOW / MEDIUM / HIGH
  - sensor_contributions: which sensors deviate most from normal
"""

import os
import pickle
import logging
import numpy as np
import pandas as pd
import psycopg2

log = logging.getLogger(__name__)

MODEL_DIR   = os.path.join(os.path.dirname(__file__), "model")
MODEL_PATH  = os.path.join(MODEL_DIR, "anomaly_detector.pkl")
SCALER_PATH = os.path.join(MODEL_DIR, "scaler.pkl")
SENSORS     = ["pressure", "temperature", "flow_rate", "vibration", "rpm"]
FEATURES    = [f"avg_{s}" for s in SENSORS]

PG_HOST = os.getenv("PG_HOST", "127.0.0.1")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DB   = os.getenv("PG_DB",   "iotdb")
PG_USER = os.getenv("PG_USER", "iotuser")
PG_PASS = os.getenv("PG_PASS", "iotpass")


def load_model():
    with open(MODEL_PATH, "rb") as f:
        model = pickle.load(f)
    with open(SCALER_PATH, "rb") as f:
        scaler = pickle.load(f)
    return model, scaler


def get_well_features(well_id: str) -> dict:
    """
    Pull actual per-sensor averages for a well from Silver layer Parquet.
    More accurate than using alert proxies from PostgreSQL.
    """
    from pyarrow import fs
    import pyarrow.parquet as pq

    MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
    MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")

    s3 = fs.S3FileSystem(
        endpoint_override=MINIO_ENDPOINT.replace("http://", ""),
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        scheme="http"
    )

    # Read silver layer and filter to this well
    dataset = pq.read_table(
        "silver/sensor_readings/",
        filesystem=s3,
        filters=[("well_id", "=", well_id)]
    )
    df = dataset.to_pandas()

    if df.empty:
        raise ValueError(f"No Silver data found for {well_id}")

    # Compute actual per-sensor averages
    sensor_avgs = (
        df.groupby("sensor_type", observed=True)["value"]
        .mean()
        .to_dict()
    )

    features = {f"avg_{sensor}": sensor_avgs.get(sensor, 0.0)
                for sensor in SENSORS}

    # Get health score from PostgreSQL
    conn = psycopg2.connect(
        host=PG_HOST, port=PG_PORT,
        dbname=PG_DB, user=PG_USER, password=PG_PASS
    )
    cur = conn.cursor()
    cur.execute(
        "SELECT health_score, avg_anomaly_rate FROM well_health WHERE well_id = %s",
        (well_id,)
    )
    row = cur.fetchone()
    cur.close()
    conn.close()

    health_score = float(row[0]) if row else 0.0
    avg_anomaly_rate = float(row[1]) if row else 0.0

    return {
        "well_id":           well_id,
        "health_score":      health_score,
        "avg_anomaly_rate":  avg_anomaly_rate,
        "features":          features
    }


def predict(well_id: str) -> dict:
    """
    Score a well for anomalous behaviour.

    Returns a dict with anomaly score, risk level, and which sensors are contributing most to the anomaly signal.
    """
    model, scaler = load_model()
    well_data = get_well_features(well_id)
    features = well_data["features"]

    X = np.array([[features[f] for f in FEATURES]])
    X_scaled = scaler.transform(X)

    # Isolation Forest: -1 = anomaly, +1 = normal
    raw_pred = model.predict(X_scaled)[0]
    # score_samples: lower = more anomalous, negate so higher = more anomalous
    anomaly_score = float(-model.score_samples(X_scaled)[0])
    is_anomaly = raw_pred == -1

    # Risk level based on anomaly score percentile thresholds
    if anomaly_score > 0.55:
        risk_level = "HIGH"
    elif anomaly_score > 0.50:
        risk_level = "MEDIUM"
    else:
        risk_level = "LOW"

    # Sensor contributions — which features deviate most from normal
    # Use absolute scaled values as a proxy for contribution
    contributions = {
        FEATURES[i]: float(abs(X_scaled[0][i]))
        for i in range(len(FEATURES))
    }
    top_sensors = sorted(
        contributions.items(), key=lambda x: x[1], reverse=True
    )[:3]

    return {
        "well_id":        well_id,
        "is_anomaly":     bool(is_anomaly),
        "anomaly_score":  round(anomaly_score, 4),
        "risk_level":     risk_level,
        "health_score":   well_data["health_score"],
        "top_sensors":    [{"sensor": k, "deviation": round(v, 4)}
                           for k, v in top_sensors],
    }


if __name__ == "__main__":
    import json
    logging.basicConfig(level=logging.INFO)
    for well in ["WELL-001", "WELL-002", "WELL-003", "WELL-004", "WELL-005"]:
        result = predict(well)
        print(json.dumps(result, indent=2))