
"""
Unit tests for the sensor simulator's core logic.
"""
 
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
 
from ingestion.simulator.sensor_simulator import (
    generate_reading,
    WELLS,
    SENSORS,
)

def test_generate_reading_has_required_fields():
    """Every reading must have all required fields."""
    well   = WELLS[0]
    sensor = "pressure"
    reading = generate_reading(well, sensor, SENSORS[sensor])
 
    required_fields = [
        "well_id", "location", "depth_m",
        "timestamp", "sensor_type", "value", "unit", "is_anomaly"
    ]
    for field in required_fields:
        assert field in reading, f"Missing field: {field}"
 
 
def test_generate_reading_correct_well():
    """Reading's well_id must match the well passed in."""
    well   = WELLS[2]   # WELL-003
    sensor = "temperature"
    reading = generate_reading(well, sensor, SENSORS[sensor])
 
    assert reading["well_id"]     == "WELL-003"
    assert reading["sensor_type"] == "temperature"
 
 
def test_generate_reading_value_in_range():
    """
    Normal readings must fall within the sensor's normal range.
    We run 50 iterations to account for randomness — if even one
    falls outside range something is wrong with the clamping logic.
    """
    sensor_name   = "pressure"
    sensor_config = SENSORS[sensor_name]
    low, high     = sensor_config["normal_range"]
 
    for _ in range(50):
        reading = generate_reading(WELLS[0], sensor_name, sensor_config)
        if not reading["is_anomaly"]:
            assert low <= reading["value"] <= high, (
                f"Normal reading {reading['value']} outside range [{low}, {high}]"
            )
 
 
def test_generate_reading_is_anomaly_is_bool():
    """is_anomaly must always be a boolean."""
    reading = generate_reading(WELLS[0], "vibration", SENSORS["vibration"])
    assert isinstance(reading["is_anomaly"], bool)
 
 
def test_all_sensors_produce_readings():
    """Every sensor type must produce a valid reading."""
    well = WELLS[0]
    for sensor_name, sensor_config in SENSORS.items():
        reading = generate_reading(well, sensor_name, sensor_config)
        assert reading["sensor_type"] == sensor_name
        assert isinstance(reading["value"], float)
 
 
def test_minio_path_structure():
    """
    Test that the MinIO path builder produces the correct
    date-partitioned structure.
    """
    from ingestion.consumer.kafka_consumer import build_minio_path
    from datetime import datetime, timezone
 
    dt   = datetime(2026, 3, 26, 14, 30, 0, tzinfo=timezone.utc)
    path = build_minio_path(dt)
 
    assert "year=2026"  in path
    assert "month=03"   in path
    assert "day=26"     in path
    assert "hour=14"    in path
    assert path.endswith(".json")