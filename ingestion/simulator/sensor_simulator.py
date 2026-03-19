"""
sensor_simulator.py
--------------------
Simulates real-time sensor telemetry from oilfield equipment.
 
What this script does:
  - Pretends to be 5 oil wells, each with 5 sensors
  - Generates a realistic sensor reading every 5 seconds per well
  - Occasionally injects anomalies (pressure spikes, overheating, etc.)
  - Sends each reading as a JSON message to a Kafka topic

Kafka topic writes to: sensor-readings
Message format (JSON):
{
    "well_id": "WELL-001",
    "timestamp": "2024-03-18T10:23:45.123456",
    "sensor_type": "pressure",
    "value": 1023.4,
    "unit": "PSI",
    "is_anomaly": false
}
"""

import json, time, random, logging
from datetime import datetime
from kafka import KafkaProducer

# ── Logging setup ──────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger(__name__)

# ── Config ─────────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = "localhost:29092"
KAFKA_TOPIC = "sensor-readings"
EMIT_INTERVAL_SECONDS = 5                       # one reading per well every 5 seconds
ANOMALY_PROBABILITY = 0.05                      # 5% chance any reading is anomalous

# ── Wells ──────────────────────────────────────────────────────────────────────
# In a real system these would come from a config file or database.
# Each well ID maps to a geographic location.
WELLS = [
    {"well_id": "WELL-001", "location": "North Sea Block A", "depth_m": 2400},
    {"well_id": "WELL-002", "location": "North Sea Block B", "depth_m": 3100},
    {"well_id": "WELL-003", "location": "Gulf Platform C",   "depth_m": 1800},
    {"well_id": "WELL-004", "location": "Gulf Platform D",   "depth_m": 2900},
    {"well_id": "WELL-005", "location": "Offshore Rig E",    "depth_m": 3500},
]

# ── Sensor definitions ─────────────────────────────────────────────────────────
# Each sensor has:
#   normal_range  → what a healthy reading looks like (min, max)
#   anomaly_range → what a spike/fault looks like (min, max)
#   unit          → unit of measurement
#
# These ranges are realistic for oilfield equipment.
# Pressure: wellhead pressure in PSI
# Temperature: downhole temperature in Celsius
# Flow Rate: production flow in cubic metres per hour
# Vibration: pump vibration in mm/s (higher = more wear)
# RPM: pump rotational speed

SENSORS = {
    "pressure": {
        "normal_range":  (800.0,  1200.0),
        "anomaly_range": (1400.0, 1800.0),   # overpressure spike
        "unit": "PSI"
    },
    "temperature": {
        "normal_range":  (60.0,  90.0),
        "anomaly_range": (110.0, 140.0),      # overheating
        "unit": "celsius"
    },
    "flow_rate": {
        "normal_range":  (50.0,  150.0),
        "anomaly_range": (0.0,   20.0),       # near-zero = blocked or failed pump
        "unit": "m3/hr"
    },
    "vibration": {
        "normal_range":  (0.5,  3.0),
        "anomaly_range": (5.0,  9.0),         # high vibration = bearing failure
        "unit": "mm/s"
    },
    "rpm": {
        "normal_range":  (1200.0, 1800.0),
        "anomaly_range": (200.0,  700.0),     # low RPM = stalled or failing pump
        "unit": "rpm"
    },
}

def generate_reading(well: dict, sensor_name: str, sensor_config: dict) -> dict:
    """
    Generate one sensor reading for one well.
 
    Randomly decides if this reading is an anomaly (5% chance).
    Picks a value from the appropriate range using a normal distribution for realistic clustering around a mean — not purely random flat noise.
    """
    is_anomaly = random.random() < ANOMALY_PROBABILITY

    if is_anomaly:
        low, high = sensor_config["anomaly_range"]
    else:
        low, high = sensor_config["normal_range"]

    # Use normal distribution centred at the midpoint of the range.
    # This gives realistic sensor data — most readings cluster near the middle,
    # with occasional readings near the edges. Real sensors behave this way.
    midpoint = (low + high) / 2
    std_dev = (high - low) / 6
    value = random.gauss(midpoint, std_dev)
    value = round(max(low, min(high, value)), 2)

    return {
        "well_id":     well["well_id"],
        "location":    well["location"],
        "depth_m":     well["depth_m"],
        "timestamp":   datetime.utcnow().isoformat(),
        "sensor_type": sensor_name,
        "value":       value,
        "unit":        sensor_config["unit"],
        "is_anomaly":  is_anomaly,
    }

def create_producer() -> KafkaProducer:
    """
    Create and return a Kafka producer.
 
    KafkaProducer is the object that sends messages to Kafka.
    value_serializer converts our Python dict → JSON bytes automatically, so we never have to call json.dumps() manually.
 
    retries=5 means if Kafka is temporarily unavailable, it tries 5 times before giving up — important for startup ordering.
    """

    log.info(f"Connecting to Kafka at {KAFKA_BOOTSTRAP_SERVERS} ...")
    producer = KafkaProducer(
        bootstrap_servers = KAFKA_BOOTSTRAP_SERVERS,
        value_serializer = lambda v: json.dumps(v).encode("utf-8"),
        key_serializer = lambda k: k.encode("utf-8"),                    # key = well_id (for partitioning)
        retries = 5,
        acks = "all"                                        # wait for Kafka to confirm the message was stored
    )
    log.info("Kafka producer connected.")
    return producer

def run_simulator(producer: KafkaProducer):
    """
    Main loop. Runs forever until you press Ctrl+C.
 
    Every EMIT_INTERVAL_SECONDS seconds, generates one reading for every sensor on every well and sends it to Kafka.
 
    With 5 wells * 5 sensors * every 5 seconds = 25 messages per cycle = 300 messages per minute.
    """

    log.info(
        f"Starting simulator: {len(WELLS)} wells * {len(SENSORS)} sensors "
        f"every {EMIT_INTERVAL_SECONDS} seconds → topic '{KAFKA_TOPIC}'"
    )
    log.info("Press Ctrl+C to stop.\n")

    total_sent = 0

    while True:
        cycle_start = time.time()

        for well in WELLS:
            for sensor_name, sensor_config in SENSORS.items():

                reading = generate_reading(well, sensor_name, sensor_config)

                # Send to Kafka.
                # key=well_id ensures all readings from the same well go to the same Kafka partition — preserves ordering per well.
                producer.send(
                    topic = KAFKA_TOPIC,
                    key = reading["well_id"],
                    value = reading,
                )

                total_sent += 1

                if reading["is_anomaly"]:
                    log.warning(
                        f"⚠️  ANOMALY | {reading['well_id']} | "
                        f"{sensor_name}: {reading['value']} {reading['unit']}"
                    )

        producer.flush()

        log.info(
            f"Cycle complete — sent {len(WELLS) * len(SENSORS)} messages "
            f"(total: {total_sent}) | "
            f"Next cycle in {EMIT_INTERVAL_SECONDS}s"
        )

        # Sleep for the remainder of the interval.
        # We subtract elapsed time so the cycle is truly every 5 seconds,
        # not 5 seconds PLUS however long generation took.
        elapsed = time.time() - cycle_start
        sleep_time = max(0, EMIT_INTERVAL_SECONDS - elapsed)
        time.sleep(sleep_time)


if __name__ == "__main__":
    producer = None
    try:
        producer = create_producer()
        run_simulator(producer)
    except KeyboardInterrupt:
        log.info("\nSimulator stopped by user.")
    except Exception as e:
        log.error(f"Simulator error: {e}")
        raise
    finally:
        if producer:
            producer.flush()
            producer.close()
            log.info("Kafka producer closed cleanly.")