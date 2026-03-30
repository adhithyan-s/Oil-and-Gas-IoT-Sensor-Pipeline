"""
sensor_pipeline_dag.py
-----------------------
Airflow DAG: Orchestrates the full IoT sensor data pipeline.
 
What this DAG does:
  Runs every hour and executes the full medallion pipeline:
 
  [check_bronze_data] → [bronze_to_silver] → [silver_to_gold] → [pipeline_summary]
 
  Each box is a "task". Tasks run in the order defined by >> (the dependency operator). 
  If a task fails, downstream tasks don't run.
 
Schedule: every hour at minute 5 (e.g. 10:05, 11:05, 12:05 ...)
  Why minute 5? The consumer writes batches every 30 seconds, so by
  minute 5 we're guaranteed the previous hour's data is fully written.
  Starting at minute 0 risks a race condition with the last batch.
 
Key Airflow concepts used here:
  - TaskFlow API (@task decorator): cleaner way to write Python tasks
  - XCom: how tasks pass data to each other (e.g. record counts)
  - execution_date: Airflow passes the scheduled run time to each task, so each run processes exactly the right time window
"""



from datetime import datetime, timedelta, timezone
 
from airflow.decorators import dag, task
import logging
 
log = logging.getLogger(__name__)
 
# ── Default arguments ──────────────────────────────────────────────────────────
# These apply to every task in this DAG unless overridden per-task.
# retries=2 means if a task fails, Airflow automatically retries it twice before marking it as failed and stopping downstream tasks.
# This is why orchestration matters — a cron job would just fail silently.
default_args = {
  "owner": "adhithyan",
  "depends_on_past": False,       # each run is independent
  "retries": 2,
  "retry_delay": timedelta(minutes=2),
  "retry_exponential_backoff": True,   # wait 2min, then 4min between retries
  "email_on_failure": False,       # set to True + add email in production
}
 
 
# ── DAG definition ─────────────────────────────────────────────────────────────
# @dag decorator turns this function into an Airflow DAG.
# schedule="5 * * * *" is cron syntax for "minute 5 of every hour".
# Cron format: minute hour day month weekday
#   "5 * * * *" = at minute 5, every hour, every day
#   "0 2 * * *" = at 2:00 AM every day
#   "0 2 * * 1" = at 2:00 AM every Monday
 
@dag(
  dag_id="iot_sensor_pipeline",
  description="Hourly pipeline: Kafka → MinIO Bronze → Silver → Gold → PostgreSQL",
  schedule="5 * * * *",
  start_date=datetime(2026, 3, 25),          # start scheduling from yesterday
  catchup=False,                   # don't backfill missed runs on first deploy
  default_args=default_args,
  tags=["iot", "oil-gas", "medallion", "sensor-data"],
)
def iot_sensor_pipeline():
  """
  IoT Sensor Pipeline DAG

  Processes one hour of sensor data per run through the full
  Bronze → Silver → Gold medallion architecture.
  """

  # ── Task 1: Check Bronze data exists ──────────────────────────────────────
  # Before running expensive Spark jobs, verify data actually landed in MinIO.
  # This is called a "data availability check" — common in production pipelines.
  # Failing fast here saves time vs discovering no data halfway through Spark.
  @task
  def check_bronze_data(**context):
    """
    Verify that Bronze (raw) data exists in MinIO for this run's time window.

    context["execution_date"] is injected by Airflow — it's the scheduled
    run time, not the current time. This is crucial for correctness:
    if the DAG is re-run for a past date, it processes that past date's data,
    not today's. This property is called "determinism."
    """

    import boto3
    from botocore.client import Config

    # Airflow passes execution_date as the scheduled time of this run
    execution_date = context["execution_date"]
    # We want the PREVIOUS hour's data (the run at 10:05 processes 09:xx data)
    target_hour = execution_date.replace(minute=0, second=0, microsecond=0)
    target_hour -= timedelta(hours=1)

    prefix = (
      f"year={target_hour.year}/"
      f"month={target_hour.month:02d}/"
      f"day={target_hour.day:02d}/"
      f"hour={target_hour.hour:02d}/"
    )

    client = boto3.client(
        "s3",
        endpoint_url="http://minio:9000",     # minio:9000 inside Docker network
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
        region_name="us-east-1",
    )

    response = client.list_objects_v2(
        Bucket="raw-sensor-data",
        Prefix=prefix,
        MaxKeys=1
    )

    file_count = response.get("KeyCount", "0")

    if file_count == 0:
        raise ValueError(
          f"No Bronze data found for window {prefix}. "
          f"Check that the Kafka consumer is running."
        )
    
    log.info(f"Bronze data check passed — found files at: {prefix}")

    # Return value is stored as XCom — downstream tasks can read it
    return {"target_hour": target_hour.isoformat(), "prefix": prefix}
  
  # ── Task 2: Run Bronze → Silver ────────────────────────────────────────────
  @task
  def run_bronze_to_silver(**context):
    """
      Run the Bronze→Silver PySpark transformation.

      Imports and calls the run() function from our existing script.
      This keeps the DAG thin — it's just orchestration, not logic.
      The logic lives in the spark_jobs/ scripts.

      This separation is important: you can run spark_jobs scripts manually
      for debugging without touching Airflow at all.
    """

    import sys
    sys.path.insert(0, "/opt/airflow/processing") 

    from processing.spark_jobs.bronze_to_silver import run as bronze_to_silver_run

    execution_date = context["execution_date"]
    window_end = execution_date.replace(minute=0, second=0, microsecond=0)
    window_start = window_end - timedelta(hours=1)

    log.info(f"Running Bronze→Silver for window: {window_start} → {window_end}")
    bronze_to_silver_run(window_start=window_start, window_end=window_end)
    log.info("Bronze→Silver complete.")

    return {"window_start": window_start.isoformat()}
  
  # ── Task 3: Run Silver → Gold ──────────────────────────────────────────────
  @task
  def run_silver_to_gold(**context):
    """
      Run the Silver→Gold PySpark transformation.
      Depends on bronze_to_silver completing successfully — Airflow
      enforces this via the >> dependency operator below.
    """
    import sys
    sys.path.insert(0, "/opt/airflow/processing")
    from processing.spark_jobs.silver_to_gold import run as silver_to_gold_run

    execution_date = context["execution_date"]
    window_end = execution_date.replace(minute=0, second=0, microsecond=0)
    window_start = window_end - timedelta(hours=1)

    log.info(f"Running Silver→Gold for window: {window_start} → {window_end}")
    silver_to_gold_run(window_start=window_start, window_end=window_end)
    log.info("Silver→Gold complete.")

    return {"window_start": window_start.isoformat()}
  
  # ── Task 4: Pipeline summary ───────────────────────────────────────────────
  @task
  def pipeline_summary(**context):
    """
      Final task — logs a summary and checks PostgreSQL has updated rows.

      In production this task would also:
        - Send a Slack/email notification with row counts
        - Write pipeline metadata to a runs audit table
        - Trigger downstream dependent DAGs

      For now it gives us a clean "pipeline complete" log entry and
      verifies the end-to-end write to PostgreSQL worked.
    """
    import psycopg2

    conn = psycopg2.connect(
      host="postgres",
      port=5432,
      dbname="iotdb",
      user="iotuser",
      password="iotpass"
    )

    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM well_health;")
    well_count = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM sensor_alerts;")
    alert_count = cur.fetchone()[0]

    cur.execute("""
        SELECT well_id, health_score, status
        FROM well_health
        ORDER BY health_score ASC
        LIMIT 5;
    """)
    rows = cur.fetchall()

    cur.close()
    conn.close()

    log.info("── Pipeline Summary ──────────────────────")
    log.info(f"  Wells tracked:   {well_count}")
    log.info(f"  Total alerts:    {alert_count}")
    log.info("  Well health status:")
    for well_id, score, status in rows:
        log.info(f"    {well_id}: {score} ({status})")
    log.info("──────────────────────────────────────────")

    return {
        "well_count":  well_count,
        "alert_count": alert_count,
    }
  
  # ── Wire up task dependencies ──────────────────────────────────────────────
  # >> means "then". This defines the execution order.
  # If any task fails, all tasks to its right are skipped.
  #
  # check_bronze_data → bronze_to_silver → silver_to_gold → pipeline_summary
  #
  # This is the DAG structure (Directed Acyclic Graph):
  # - Directed: flows left to right
  # - Acyclic: no loops
  # - Graph: tasks are nodes, >> are edges

  bronze_check = check_bronze_data()
  b_to_s = run_bronze_to_silver()
  s_to_g = run_silver_to_gold()
  summary = pipeline_summary()

  bronze_check >> b_to_s >> s_to_g >> summary


# Instantiate the DAG — Airflow looks for a DAG object at module level
dag_instance = iot_sensor_pipeline()
