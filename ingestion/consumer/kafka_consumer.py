"""
Reads sensor readings from Kafka and writes them to MinIO (Bronze layer).
 
What this script does:
  - Connects to Kafka and subscribes to the 'sensor-readings' topic
  - Reads messages in batches (every 30 seconds OR every 100 messages)
  - Writes each batch as a JSON file into MinIO bucket 'raw-sensor-data'
  - This is the Bronze layer — raw, unmodified, append-only
 
Why batching instead of writing every single message?
  Writing one file per message = thousands of tiny files (bad for Spark).
  Batching into time windows = fewer, larger files (good for Spark).
  This pattern is called "micro-batching" — common in real pipelines.
 
File naming convention in MinIO:
  raw-sensor-data/
  └── year=2024/
      └── month=03/
          └── day=18/
              └── hour=10/
                  └── batch_20240318_102300.json
 
  This folder structure (called "partitioning by date") means Spark can
  read just one day's data without scanning the entire bucket. Very important
  for performance at scale.
"""

import json, time, logging
from datetime import datetime, timezone
from io import BytesIO

import boto3
from botocore.client import Config
from kafka import KafkaConsumer

# -- Logging ------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger(__name__)
 
# -- Kafka config -------------------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS = "localhost:29092"
KAFKA_TOPIC             = "sensor-readings"
KAFKA_GROUP_ID          = "sensor-consumer-group"
# Consumer group ID is important:
# If you run multiple consumers with the same group ID, Kafka automatically
# splits the work between them (load balancing). Each message goes to only
# one consumer in the group. This is how to scale consumers in production.
 
# -- MinIO (S3) config ---------------------------------------------------------
MINIO_ENDPOINT    = "http://localhost:9000"   # MinIO's S3 API port
MINIO_ACCESS_KEY  = "minioadmin"
MINIO_SECRET_KEY  = "minioadmin"
BRONZE_BUCKET     = "raw-sensor-data"        # Bronze layer bucket
 
# -- Batching config -----------------------------------------------------------
BATCH_SIZE         = 100    # write to MinIO after 100 messages
BATCH_INTERVAL_SEC = 30     # OR after 30 seconds, whichever comes first


def create_minio_client():
    """
    Create and return a boto3 S3 client pointed at MinIO.
 
    This is identical to how we'd connect to real AWS S3 —
    the only difference is endpoint_url points to localhost instead of AWS.
 
    In production you'd swap:
      endpoint_url="http://localhost:9000"
    for:
      (nothing — boto3 defaults to AWS automatically)
    """
    log.info(f"Connecting to MinIO at {MINIO_ENDPOINT} ...")
    client = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),  # MinIO requires v4 signatures
        region_name="us-east-1",                  # required by boto3, value doesn't matter for MinIO
    )
    log.info("MinIO client connected.")
    return client


def createKafkaConsumer() -> KafkaConsumer:
    """
    Create and return a Kafka consumer subscribed to the sensor topic.
 
    auto_offset_reset='earliest':
      If this consumer has never run before, start reading from the
      very beginning of the topic (don't miss old messages).
 
    enable_auto_commit=True:
      Kafka tracks which messages each consumer group has read.
      Auto-commit means "mark messages as read automatically."
      This prevents re-reading the same messages on restart.
    """
    log.info(f"Connecting to Kafka at {KAFKA_BOOTSTRAP_SERVERS} ...")

    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers = KAFKA_BOOTSTRAP_SERVERS,
        group_id = KAFKA_GROUP_ID,
        auto_offset_reset = "earliest",
        enable_auto_commit = True,
        value_deserializer = lambda v: json.loads(v.decode("utf-8")),
        consumer_timeout_ms = 1000,
    ) 

    log.info(f"Kafka consumer connected. Subscribed to topic: '{KAFKA_TOPIC}'")
    return consumer


def build_minio_path(batch_timestamp: datetime) -> str:
    """
    Build the S3/MinIO object key (file path) for a batch.
 
    Uses date-partitioned folder structure:
      year=2024/month=03/day=18/hour=10/batch_20240318_102300.json
 
    Why this structure?
      When Spark reads this data later, you can tell it:
      "only read year=2024/month=03/day=18/" and it skips all other days.
      This is called "partition pruning" — massively speeds up queries
      on large datasets. Standard practice in data lakes.
    """
    return (
        f"year={batch_timestamp.year}/"
        f"month={batch_timestamp.month:02d}/"
        f"day={batch_timestamp.day:02d}/"
        f"hour={batch_timestamp.hour:02d}/"
        f"batch_{batch_timestamp.strftime('%Y%m%d_%H%M%S')}.json"
    )


def write_batch_to_minio(client, batch: list, batch_timestamp: datetime):
    """
    Write a batch of sensor readings to MinIO as a JSON file.
 
    We store as newline-delimited JSON (one JSON object per line).
    This format is called NDJSON or JSON Lines.
 
    Why NDJSON instead of a JSON array?
      Spark reads NDJSON natively and efficiently.
      A JSON array requires loading the entire file before parsing.
      NDJSON lets Spark read line-by-line in parallel.
    """

    object_key = build_minio_path(batch_timestamp)

    # Convert list of dicts to NDJSON bytes
    ndjson_content = "\n".join(json.dumps(record) for record in batch)
    content_bytes = ndjson_content.encode("utf-8")

    client.put_object(
        Bucket = BRONZE_BUCKET,
        Key = object_key,
        Body = BytesIO(content_bytes),
        ContentLength = len(content_bytes),
        ContentType = "application/json"
    )

    log.info(
        f"✅ Wrote {len(batch)} records to MinIO: "
        f"{BRONZE_BUCKET}/{object_key}"
    )


def run_consumer(consumer: KafkaConsumer, minio_client):
    """
    Main consumer loop.
 
    Collects messages into a batch.
    Flushes the batch to MinIO when:
      - batch reaches BATCH_SIZE (100 messages), OR
      - BATCH_INTERVAL_SEC (30 seconds) have passed
 
    This dual-trigger pattern ("size OR time, whichever first") is
    standard in production streaming pipelines. It means:
      - High-traffic periods: flush frequently by size
      - Low-traffic periods: still flush regularly by time
    """

    log.info(
        f"Starting consumer loop — "
        f"flushing every {BATCH_SIZE} messages OR {BATCH_INTERVAL_SEC} seconds\n"
    )

    batch = []
    batch_start = time.time()
    total_written = 0

    while True:
        try:
            # Poll Kafka for messages (returns after consumer_timeout_ms if empty)
            for message in consumer:
                batch.append(message.value)

                # Flush condition 1: batch is full
                if len(batch) >= BATCH_SIZE:
                    ts = datetime.now(timezone.utc)
                    write_batch_to_minio(minio_client, batch, ts)
                    total_written += len(batch)
                    batch = []
                    batch_start = time.time()

            # Flush condition 2: time interval elapsed (even if batch not full)
            elapsed = time.time() - batch_start
            if batch and elapsed >= BATCH_INTERVAL_SEC:
                ts = datetime.now(timezone.utc)
                write_batch_to_minio(minio_client, batch, ts)
                total_written += len(batch)
                log.info(f"Total records written to MinIO so far: {total_written}")
                batch = []
                batch_start = time.time()
            elif not batch:
                log.info("Waiting for messages from Kafka ...")
        except KeyboardInterrupt:
            raise


if __name__ == "__main__":
    consumer = None
    minio_client = None

    try:
        minio_client = create_minio_client()
        consumer = createKafkaConsumer()
        run_consumer(consumer, minio_client)
    except KeyboardInterrupt:
        log.info("\nConsumer stopped by user.")
    except Exception as e:
        log.error(f"Consumer error: {e}")
        raise
    finally:
        if consumer:
            consumer.close()
            log.info("Kafka consumer closed cleanly.")