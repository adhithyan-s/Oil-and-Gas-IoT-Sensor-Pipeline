"""
PySpark job: Silver layer -> Gold layer
 
What this script does:
  1. Reads clean Parquet from MinIO 'silver' bucket
  2. Computes per-well KPIs:
       - Rolling averages (1-hour window) per sensor
       - Anomaly rate per well (% of readings flagged)
       - Equipment health score (0-100) based on sensor deviations
       - Alert counts by severity (WARNING / CRITICAL)
  3. Writes Gold Parquet to MinIO 'gold' bucket
  4. Also writes aggregated metrics to PostgreSQL for Grafana dashboards
"""


import logging, os
from datetime import datetime, timezone, timedelta
 
import psycopg2
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F


# -- Logging --------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger(__name__)
 
# -- Config ---------------------------------------------------------------------
# Environment variables allow same code to run locally and inside Docker.
# Locally:        MINIO_ENDPOINT=http://localhost:9000, PG_HOST=127.0.0.1, PG_PORT=5433
# Inside Docker:  MINIO_ENDPOINT=http://minio:9000,    PG_HOST=postgres,   PG_PORT=5432
MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT",   "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
 
SILVER_PATH = "s3a://silver/sensor_readings/"
GOLD_PATH   = "s3a://gold/equipment_health/"
 
# PostgreSQL — where Grafana reads from
PG_HOST = os.getenv("PG_HOST", "127.0.0.1")
PG_PORT = int(os.getenv("PG_PORT", "5433"))
PG_DB   = os.getenv("PG_DB",   "iotdb")
PG_USER = os.getenv("PG_USER", "iotuser")
PG_PASS = os.getenv("PG_PASS", "iotpass")

# -- Health score thresholds ------------------------------------------------------
# A "health score" is a single 0-100 number summarising equipment condition.
# 100 = perfect, 0 = critical failure imminent.
# Interviewers love asking how you designed domain-specific derived metrics.
#
# Formula:
#   Start at 100.
#   Subtract points for anomaly rate and out-of-range readings.
#   Cap between 0 and 100.
#
# Penalty weights (tunable):
ANOMALY_RATE_PENALTY    = 40   # if 100% readings are anomalies -> -40 points
INVALID_VALUE_PENALTY   = 30   # if 100% readings are invalid -> -30 points
HIGH_VIBRATION_PENALTY  = 20   # vibration > 5 mm/s is a strong wear signal
LOW_RPM_PENALTY         = 10   # low RPM = pump struggling


def create_spark_session() -> SparkSession:
    log.info("Initialising Spark session ...")

    spark = (
        SparkSession.builder
        .appName("SilverToGold")
        .master("local[*]")
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262"
        )
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    log.info("Spark session ready.")
    return spark


def read_silver(spark: SparkSession, window_start: datetime):
    """
    Read Silver Parquet for the processing window.
    Parquet reads are much faster than JSON because Spark can push down
    column filters — it only reads the columns it actually needs.
    """

    path = (
        f"{SILVER_PATH}"
        f"year={window_start.year}/"
        f"month={window_start.month:02d}/"
        f"day={window_start.day:02d}/"
    )
    log.info(f"Reading Silver data from: {path}")
    df = spark.read.parquet(path)
    log.info(f"  Records read: {df.count():,}")
    return df


def compute_sensor_averages(df):
    """
    Compute per-well per-sensor rolling statistics.
 
    For each well + sensor combination, compute:
      - avg_value:    mean reading over the window
      - min_value:    minimum reading
      - max_value:    maximum reading
      - stddev_value: standard deviation (measure of stability)
      - reading_count: how many readings contributed
 
    Standard deviation is important: a pressure sensor with avg=1000 PSI
    and stddev=5 is stable. Same avg but stddev=200 is erratic — worth alerting.
    """

    log.info("Computing sensor averages ...")

    return (
        df.groupBy("well_id", "sensor_type", "location")
        .agg(
            F.round(F.avg("value"), 2).alias("avg_value"),
            F.round(F.min("value"), 2).alias("min_value"),
            F.round(F.max("value"), 2).alias("max_value"),
            F.round(F.stddev("value"), 2).alias("stddev_value"),
            F.count("*") .alias("reading_count"),
            F.sum(F.col("is_anomaly").cast("int")).alias("anomaly_count"),
        )
        .withColumn(
            "anomaly_rate",
            F.round(F.col("anomaly_count") / F.col("reading_count"), 4)
        )
    )


def compute_health_scores(df_stats):
    """
    Compute a single equipment health score (0-100) per well.
 
    Aggregates across all sensors for a well, then applies penalty formula.
    """
    log.info("Computing equipment health scores ...")

    # Roll up to well level — average the per-sensor anomaly rates
    well_summary = (
        df_stats
        .groupBy("well_id", "location")
        .agg(
            F.round(F.avg("anomaly_rate"), 4).alias("avg_anomaly_rate"),
            F.round(F.avg("avg_value"), 2).alias("overall_avg_value"),
            F.sum("reading_count").alias("total_readings"),
            F.sum("anomaly_count").alias("total_anomalies"),
        )
    )

    # Health score formula:
    #   100 - (anomaly_rate × ANOMALY_RATE_PENALTY)
    #   clamped between 0 and 100
    health = well_summary.withColumn(
        "health_score",
        F.round(
            F.greatest(
                F.lit(0.0),
                F.least(
                    F.lit(100.0),
                    F.lit(100.0) - (F.col("avg_anomaly_rate") * ANOMALY_RATE_PENALTY)
                )
            ),
            1
        )
    )

    # Add human-readable status label
    health = health.withColumn(
        "status",
        F.when(F.col("health_score") >= 80, "HEALTHY")
         .when(F.col("health_score") >= 60, "WARNING")
         .otherwise("CRITICAL")
    )
 
    return health


def compute_alert_summary(df):
    """
    Count alerts by well and severity for the Grafana alert panel.
 
    WARNING  -> is_anomaly=True but value still within outer valid range
    CRITICAL -> is_anomaly=True AND value_valid=False (out of all bounds)
 
    Having a pre-aggregated alert table means Grafana queries are instant —
    no need to scan millions of raw records to render a dashboard.
    """
    log.info("Computing alert summary ...")
    return (
        df.filter(F.col("is_anomaly") == True)
        .withColumn(
            "severity",
            F.when(F.col("value_valid") == False, "CRITICAL")
             .otherwise("WARNING")
        )
        .groupBy("well_id", "sensor_type", "severity")
        .agg(F.count("*").alias("alert_count"))
        .orderBy(F.col("alert_count").desc())
    )


def write_gold(df_health, df_sensor_stats, df_alerts, window_start: datetime):
    """Write all Gold tables to MinIO as Parquet."""
    base = (
        f"{GOLD_PATH}"
        f"year={window_start.year}/"
        f"month={window_start.month:02d}/"
        f"day={window_start.day:02d}/"
        f"hour={window_start.hour:02d}/"
    )
 
    for df, name in [
        (df_health,       "well_health_scores"),
        (df_sensor_stats, "sensor_statistics"),
        (df_alerts,       "alert_summary"),
    ]:
        path = f"{base}{name}/"
        log.info(f"Writing Gold table '{name}' to: {path}")
        df.write.mode("overwrite").parquet(path)
        # overwrite (not append) for Gold — each run produces the latest snapshot
 
    log.info("Gold write complete.")


def write_to_postgres(df_health, df_alerts, window_start: datetime):
    """
    Write Gold metrics to PostgreSQL so Grafana can query them.
 
    We use psycopg2 directly (not Spark JDBC) to keep it simple.
    For each well, upsert the latest health score.
    For alerts, insert the latest batch.
 
    UPSERT (INSERT ... ON CONFLICT DO UPDATE) means running this job
    multiple times is safe — it won't create duplicate rows.
    This property is called "idempotency" — a key concept in data engineering.
    Idempotent jobs can be safely retried without corrupting data.
    """
    log.info("Writing to PostgreSQL ...")

    conn = psycopg2.connect(
        host=PG_HOST, port=PG_PORT,
        dbname=PG_DB, user=PG_USER, password=PG_PASS
    )
    cur = conn.cursor()

    # Create tables if they don't exist yet
    cur.execute("""
        CREATE TABLE IF NOT EXISTS well_health (
            well_id          TEXT PRIMARY KEY,
            location         TEXT,
            health_score     NUMERIC(5,1),
            status           TEXT,
            avg_anomaly_rate NUMERIC(6,4),
            total_readings   BIGINT,
            total_anomalies  BIGINT,
            updated_at       TIMESTAMPTZ DEFAULT NOW()
        );
    """)
 
    cur.execute("""
        CREATE TABLE IF NOT EXISTS sensor_alerts (
            id           SERIAL PRIMARY KEY,
            well_id      TEXT,
            sensor_type  TEXT,
            severity     TEXT,
            alert_count  INT,
            window_start TIMESTAMPTZ,
            inserted_at  TIMESTAMPTZ DEFAULT NOW()
        );
    """)
    conn.commit()

    # Upsert health scores — one row per well, always reflects latest state
    health_rows = df_health.collect()
    for row in health_rows:
        cur.execute("""
            INSERT INTO well_health
                (well_id, location, health_score, status,
                 avg_anomaly_rate, total_readings, total_anomalies, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (well_id) DO UPDATE SET
                health_score     = EXCLUDED.health_score,
                status           = EXCLUDED.status,
                avg_anomaly_rate = EXCLUDED.avg_anomaly_rate,
                total_readings   = EXCLUDED.total_readings,
                total_anomalies  = EXCLUDED.total_anomalies,
                updated_at       = NOW();
        """, (
            row.well_id, row.location, float(row.health_score),
            row.status, float(row.avg_anomaly_rate),
            int(row.total_readings), int(row.total_anomalies)
        ))

    # Insert alert counts for this window (append-only — historical record)
    alert_rows = df_alerts.collect()
    for row in alert_rows:
        cur.execute("""
            INSERT INTO sensor_alerts
                (well_id, sensor_type, severity, alert_count, window_start)
            VALUES (%s, %s, %s, %s, %s);
        """, (
            row.well_id, row.sensor_type, row.severity,
            int(row.alert_count), window_start.isoformat()
        ))
 
    conn.commit()
    cur.close()
    conn.close()
 
    log.info(f"  Upserted {len(health_rows)} well health records")
    log.info(f"  Inserted {len(alert_rows)} alert records")


def run(window_start=None, window_end=None):
    if window_start is None:
        now = datetime.now(timezone.utc)
        window_end = now.replace(minute=0, second=0, microsecond=0)
        window_start = window_end - timedelta(hours=1)

    log.info(f"Processing window: {window_start} -> {window_end}")

    spark = create_spark_session()

    try:
        df_silver = read_silver(spark, window_start)

        if df_silver.rdd.isEmpty():
            log.info("No Silver data found for this window. Exiting.")
            return
        
        df_sensor_stats = compute_sensor_averages(df_silver)
        df_health = compute_health_scores(df_sensor_stats)
        df_alerts = compute_alert_summary(df_silver)

        # Show summaries in terminal
        log.info("── Well Health Scores ──")
        df_health.select(
            "well_id", "health_score", "status",
            "avg_anomaly_rate", "total_readings"
        ).show(truncate=False)
 
        log.info("── Top Alerts ──")
        df_alerts.show(10, truncate=False)
 
        write_gold(df_health, df_sensor_stats, df_alerts, window_start)
        write_to_postgres(df_health, df_alerts, window_start)
 
        log.info("Silver -> Gold complete.")

    finally:
        spark.stop()
        log.info("Spark session stopped.")


if __name__ == "__main__":
    run()