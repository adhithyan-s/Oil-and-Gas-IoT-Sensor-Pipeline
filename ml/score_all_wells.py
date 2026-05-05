"""
Scores all 5 wells and writes results to PostgreSQL ml_anomaly_scores table.
Run this after each silver_to_gold cycle to keep Grafana panel current.

Usage:
    python ml/score_all_wells.py

In production this would be added as Task 5 in the Airflow DAG:
    silver_to_gold >> score_anomalies >> pipeline_summary
"""

import os
import logging
import psycopg2
from datetime import datetime, timezone
from predict import predict

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger(__name__)

PG_HOST = os.getenv("PG_HOST", "127.0.0.1")
PG_PORT = int(os.getenv("PG_PORT", "5432"))
PG_DB   = os.getenv("PG_DB",   "iotdb")
PG_USER = os.getenv("PG_USER", "iotuser")
PG_PASS = os.getenv("PG_PASS", "iotpass")

WELLS = ["WELL-001", "WELL-002", "WELL-003", "WELL-004", "WELL-005"]


def write_scores_to_postgres(scores: list):
    conn = psycopg2.connect(
        host=PG_HOST, port=PG_PORT,
        dbname=PG_DB, user=PG_USER, password=PG_PASS
    )
    cur = conn.cursor()

    for s in scores:
        sensors = s["top_sensors"]
        cur.execute("""
            INSERT INTO ml_anomaly_scores
                (well_id, scored_at, anomaly_score, is_anomaly,
                 risk_level, health_score,
                 top_sensor_1, top_sensor_2, top_sensor_3)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            s["well_id"],
            datetime.now(timezone.utc),
            s["anomaly_score"],
            s["is_anomaly"],
            s["risk_level"],
            s["health_score"],
            sensors[0]["sensor"] if len(sensors) > 0 else None,
            sensors[1]["sensor"] if len(sensors) > 1 else None,
            sensors[2]["sensor"] if len(sensors) > 2 else None,
        ))

    conn.commit()
    cur.close()
    conn.close()
    log.info(f"Wrote {len(scores)} anomaly scores to PostgreSQL")


def run():
    log.info("Scoring all wells ...")
    scores = []
    for well_id in WELLS:
        try:
            result = predict(well_id)
            scores.append(result)
            log.info(
                f"  {well_id}: score={result['anomaly_score']} "
                f"risk={result['risk_level']} "
                f"anomaly={result['is_anomaly']}"
            )
        except Exception as e:
            log.error(f"  {well_id}: failed — {e}")

    if scores:
        write_scores_to_postgres(scores)
    log.info("Done.")


if __name__ == "__main__":
    run()
