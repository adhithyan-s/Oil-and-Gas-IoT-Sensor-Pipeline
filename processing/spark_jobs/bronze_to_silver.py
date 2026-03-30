"""
bronze_to_silver.py
--------------------
PySpark job: Bronze layer → Silver layer
 
What this script does:
  1. Reads raw NDJSON files from MinIO 'raw-sensor-data' bucket (Bronze)
  2. Enforces schema — rejects malformed records
  3. Cleans data — handles nulls, casts types, standardises values
  4. Validates business rules — flags out-of-range readings
  5. Deduplicates — removes duplicate messages (Kafka can occasionally
     deliver the same message twice — this is called "at-least-once delivery")
  6. Writes clean Parquet files to MinIO 'silver' bucket
 
Why Parquet instead of JSON for Silver/Gold?
  - Parquet is columnar — reading just 'pressure' values doesn't touch
    temperature/vibration/etc. columns at all. Much faster for analytics.
  - Parquet is compressed — typically 5-10x smaller than JSON.
  - Parquet stores schema — no need to infer types every time you read it.
  - Industry standard for data lakes. You'll see this everywhere.
"""

import logging, os
from datetime import datetime, timezone, timedelta

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, BooleanType, TimestampType
)

# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger(__name__)
 
# ── MinIO / S3 config ──────────────────────────────────────────────────────────
#   - Local (laptop): http://localhost:9000
#   - Docker/Airflow: http://minio:9000
# Set in docker-compose under airflow environment, defaults to localhost for manual runs.
MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
 
BRONZE_PATH = "s3a://raw-sensor-data/"
SILVER_PATH = "s3a://silver/sensor_readings/"
# s3a:// is the Hadoop S3 connector protocol — this is what Spark uses to talk to S3-compatible storage.
# 's3://' is AWS-only. 's3a://' works with MinIO too.
 
# ── Business rules: valid sensor ranges ───────────────────────────────────────
# If a reading falls outside these ranges it's either an anomaly or a sensor fault. 
# We flag it but keep the record — never silently discard data.
VALID_RANGES = {
    "pressure":    (0.0,    2000.0),
    "temperature": (-10.0,  200.0),
    "flow_rate":   (-5.0,   500.0),
    "vibration":   (0.0,    20.0),
    "rpm":         (0.0,    5000.0),
}

# ── Bronze schema ──────────────────────────────────────────────────────────────
# Explicitly defining the schema does two things:
#   1. Spark doesn't need to scan files to infer types (faster)
#   2. Malformed records that don't match are caught immediately
BRONZE_SCHEMA = StructType([
    StructField("well_id",     StringType(),    nullable=False),
    StructField("location",    StringType(),    nullable=True),
    StructField("depth_m",     DoubleType(),    nullable=True),
    StructField("timestamp",   StringType(),    nullable=False),
    StructField("sensor_type", StringType(),    nullable=False),
    StructField("value",       DoubleType(),    nullable=False),
    StructField("unit",        StringType(),    nullable=True),
    StructField("is_anomaly",  BooleanType(),   nullable=True),
])


def create_spark_session() -> SparkSession:
    """
    Create a SparkSession configured to talk to MinIO via S3A protocol.
 
    The hadoop-aws and aws-java-sdk packages are what give Spark the ability
    to read/write S3-compatible storage. Without these, Spark only knows
    about local filesystems and HDFS.
 
    spark.hadoop.fs.s3a.path.style.access = true
      MinIO requires path-style URLs: http://localhost:9000/bucket/key
      AWS S3 uses virtual-hosted style: http://bucket.s3.amazonaws.com/key
      This setting forces Spark to use path-style, which MinIO needs.
    """

    log.info("Initialising Spark session ...")

    spark = (
        SparkSession.builder
        .appName("BronzeToSilver")
        .master("local[*]")   # local[*] = use all CPU cores on the machine
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

    spark.sparkContext.setLogLevel("WARN")  # suppress verbose Spark logs
    log.info("Spark session ready.")
    return spark


def get_processing_window():
    """
    Return the time window to process — defaults to the past 1 hour.
 
    In production Airflow passes the execution date as a parameter.
    For manual runs we default to the last hour.
 
    This is how incremental processing works — each run only processes
    new data, not the entire dataset from the beginning.
    """

    now = datetime.now(timezone.utc)
    window_end = now.replace(minute=0, second=0, microsecond=0)
    window_start = window_end - timedelta(hours=1)

    return window_start, window_end


def read_bronze(spark: SparkSession, window_start: datetime) -> "DataFrame":
    """
    Read Bronze JSON files for the processing window from MinIO.
 
    We construct the S3 path using the partition structure we built
    in the consumer (year=/month=/day=/hour=/).
    This means Spark only reads the relevant hour's files, not everything.
    """

    path = (
        f"{BRONZE_PATH}"
        f"year={window_start.year}/"
        f"month={window_start.month:02d}/"
        f"day={window_start.day:02d}/"
        f"hour={window_start.hour:02d}/"
    )
    log.info(f"Reading Bronze data from: {path}")

    df = spark.read.schema(BRONZE_SCHEMA).json(path)
    raw_count = df.count()
    log.info(f"  Raw records read: {raw_count:,}")

    return df


def clean_and_validate(df) -> "DataFrame":
    """
    Apply all cleaning and validation transformations.
 
    Steps:
      1. Drop records with null well_id, timestamp, sensor_type, or value
         (these are the non-negotiable fields — useless without them)
      2. Parse timestamp string → proper TimestampType
      3. Trim whitespace from string fields
      4. Add 'value_valid' flag: True if value is within known sensor range
      5. Add 'processed_at' column: when this ETL job ran
      6. Add 'silver_version': simple version tracking for this schema
 
    Key principle: we NEVER delete anomalous records.
    We flag them with is_anomaly=True and value_valid=False.
    Deleting data is irreversible. Flagging is reversible.
    Downstream consumers can decide what to do with flagged records.
    """

    log.info("Cleaning and validating ...")

    # Step 1: Drop records missing critical fields
    before = df.count()
    df = df.dropna(subset=["well_id", "timestamp", "sensor_type", "value"])
    dropped = before - df.count()

    if dropped:
        log.warning(f"  Dropped {dropped} records with null critical fields")

    # Step 2: Parse ISO timestamp string to proper Spark timestamp
    df = df.withColumn(
        "timestamp",
        F.to_timestamp(F.col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSS")
    )

    # Step 3: Trim strings
    df = df.withColumn("well_id", F.trim(F.col("well_id")))
    df = df.withColumn("sensor_type", F.trim(F.lower(F.col("sensor_type"))))

    # Step 4: Validate value ranges using business rules
    # Build a CASE WHEN expression dynamically from VALID_RANGES dict
    valid_expr = F.lit(True)    # default: assume valid
    for sensor, (min_val, max_val) in VALID_RANGES.items():
        valid_expr = F.when(
            F.col("sensor_type") == sensor,
            F.col("value").between(min_val, max_val)
        ).otherwise(valid_expr)

    df = df.withColumn("value_valid", valid_expr)

    # Step 5: Add audit columns
    df = df.withColumn("processed_at", F.lit(datetime.now(timezone.utc).isoformat()))
    df = df.withColumn("silver_version", F.lit("1.0"))

    log.info(f"  Records after cleaning: {df.count():,}")
    return df


def duplicate(df) -> "DataFrame":
    """
    Remove duplicate records.
 
    A record is a duplicate if it has the same:
      well_id + timestamp + sensor_type
 
    Why duplicates happen:
      Kafka guarantees "at-least-once" delivery — in rare network hiccups, a message might be delivered twice. 
      The consumer might also write the same batch twice if it crashes mid-write.
 
    We use dropDuplicates on the natural key of the record.
    """

    before = df.count()
    df = df.dropDuplicates(["well_id", "timestamp", "sensor_type"])
    dupes = before - df.count()

    if dupes:
        log.warning(f"  Removed {dupes} duplicate records")
    else:
        log.info("  No duplicates found")

    return df


def write_silver(df, window_start: datetime):
    """
    Write the cleaned DataFrame to the Silver bucket as Parquet.
 
    Partitioned by well_id and date — this is different from the Bronze
    partition (which was by ingest time). Silver is partitioned by the
    data's own timestamp, which is better for analytical queries like
    "show me all readings from WELL-001 in March."
 
    mode="append": adds to existing data, doesn't overwrite.
    This is correct for incremental processing — each hourly run
    adds its slice to the Silver table.
    """

    output_path = (
        f"{SILVER_PATH}"
        f"year={window_start.year}/"
        f"month={window_start.month:02d}/"
        f"day={window_start.day:02d}/"
    )
    log.info(f"Writing Silver Parquet to: {output_path}")
 
    (
        df.write
        .mode("append")
        .partitionBy("well_id", "sensor_type")
        .parquet(output_path)
    )
    log.info(f"  Silver write complete.")


def log_quality_summary(df):
    """
    Print a data quality summary — how many anomalies, invalid values, etc.
    """

    total = df.count()
    anomalies = df.filter(F.col("is_anomaly") == True).count()
    invalid = df.filter(F.col("value_valid") == False).count()
    null_locs = df.filter(F.col("location").isNull()).count()

    log.info("── Data Quality Summary ──────────────────")
    log.info(f"  Total records:      {total:,}")
    log.info(f"  Anomaly flagged:    {anomalies:,}  ({100*anomalies/total:.1f}%)")
    log.info(f"  Out-of-range:       {invalid:,}   ({100*invalid/total:.1f}%)")
    log.info(f"  Missing location:   {null_locs:,}")
    log.info("─────────────────────────────────────────")

    # Per-sensor breakdown
    log.info("  Anomalies by sensor:")
    (
        df.filter(F.col("is_anomaly") == True)
        .groupBy("sensor_type")
        .count()
        .orderBy(F.col("count").desc())
        .show(truncate=False)
    ) 


def run(window_start=None, window_end=None):
    """
    Main entry point. Called directly or by Airflow.
 
    When Airflow calls this, it passes the DAG execution window
    so each run processes exactly the right slice of data.
    """

    if window_start is None:
        window_start, window_end = get_processing_window()

    log.info(f"Processing window: {window_start} → {window_end}")

    spark = create_spark_session()

    try:
        df = read_bronze(spark, window_start)

        if df.rdd.isEmpty():
            log.info("No data found for this window. Exiting.")
            return
        
        df = clean_and_validate(df)
        df.dropDuplicates()
        log_quality_summary(df)
        write_silver(df, window_start)

        log.info("Bronze → Silver complete.")

    finally:
        spark.stop()
        log.info("Spark session stopped.")


if __name__ == "__main__":
    run()