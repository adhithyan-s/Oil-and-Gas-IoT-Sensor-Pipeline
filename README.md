# 🛢️ Oil & Gas IoT Sensor Pipeline

![CI](https://github.com/adhithyan-s/Oil-and-Gas-IoT-Sensor-Pipeline/actions/workflows/ci.yml/badge.svg)
![Python](https://img.shields.io/badge/Python-3.11-blue?logo=python)
![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-7.4-black?logo=apachekafka)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.8.1-017CEE?logo=apacheairflow)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.5-E25A1C?logo=apachespark)
![MinIO](https://img.shields.io/badge/MinIO-S3--compatible-C72E49?logo=minio)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-336791?logo=postgresql)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker)
![Grafana](https://img.shields.io/badge/Grafana-Dashboard-F46800?logo=grafana)

A production-style, end-to-end data engineering pipeline that simulates real-time sensor telemetry from oilfield equipment. Sensor readings stream through Apache Kafka, land in a local S3-compatible data lake (MinIO), get transformed by PySpark through a Bronze-Silver-Gold medallion architecture, orchestrated hourly by Apache Airflow, and visualised live in Grafana.

---

## 📊 Dashboard

![Grafana Dashboard](docs/grafana_dashboard.png)

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        INGESTION LAYER                          │
│                                                                 │
│   Sensor Simulator (Python)                                     │
│   5 wells × 5 sensors × every 5s = 300 msgs/min                 │
│          │                                                      │
│          ▼                                                      │
│   Apache Kafka  ←──────── topic: sensor-readings                │
│          │                                                      │
│          ▼                                                      │
│   Kafka Consumer → batches every 30s or 100 msgs                │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│                     STORAGE LAYER (MinIO)                       │
│                                                                 │
│   Bronze: raw-sensor-data/year=/month=/day=/hour=/              │
│           Raw NDJSON, append-only, full audit trail             │
│                          │                                      │
│   Silver: silver/sensor_readings/year=/month=/day=/             │
│           Cleaned Parquet, schema enforced, anomalies flagged   │
│                          │                                      │
│   Gold:   gold/equipment_health/year=/month=/day=/hour=/        │
│           KPIs, health scores, alert summaries                  │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│               ORCHESTRATION (Apache Airflow)                    │
│                                                                 │
│   DAG: iot_sensor_pipeline  │  Schedule: hourly at :05          │
│                                                                 │
│   check_bronze_data                                             │
│          │                                                      │
│          ▼                                                      │
│   bronze_to_silver  (PySpark)                                   │
│          │                                                      │
│          ▼                                                      │
│   silver_to_gold    (PySpark)                                   │
│          │                                                      │
│          ▼                                                      │
│   pipeline_summary  (PostgreSQL check)                          │
└──────────────────────────┬──────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│              WAREHOUSE + VISUALISATION                          │
│                                                                 │
│   PostgreSQL ← well_health, sensor_alerts tables                │
│          │                                                      │
│          ▼                                                      │
│   Grafana Dashboard (auto-refreshes every 30s)                  │
└─────────────────────────────────────────────────────────────────┘
```

---

## 📡 Simulated Sensors

5 oilfield wells, each emitting readings every 5 seconds across 5 sensor types:

| Sensor      | Unit  | Normal Range | Anomaly Range |
|-------------|-------|--------------|---------------|
| Pressure    | PSI   | 800 – 1200   | > 1400        |
| Temperature | °C    | 60 – 90      | > 110         |
| Flow Rate   | m³/hr | 50 – 150     | < 20          |
| Vibration   | mm/s  | 0.5 – 3.0    | > 5.0         |
| RPM         | rpm   | 1200 – 1800  | < 800         |

Anomalies are injected at ~5% probability using a normal distribution — not flat random — to simulate realistic sensor behaviour.

---

## 🧱 Medallion Architecture

| Layer      | Location           | What happens                                                                                                                |
|------------|--------------------|-----------------------------------------------------------------------------------------------------------------------------|
| **Bronze** | `raw-sensor-data/` | Raw NDJSON events as-is from Kafka. No transformation. Full audit trail. Date-partitioned by ingest time. |
| **Silver** | `silver/`          | Schema enforced, nulls handled, duplicates removed, anomalies flagged, values validated against business rules. Parquet format.                       |
| **Gold**   | `gold/`            | Per-well KPIs: rolling averages, standard deviation, anomaly rates, equipment health scores (0-100), alert summaries. Written to PostgreSQL for dashboards. |

---

## 🛠️ Tech Stack

| Layer            | Tool                    | Purpose                                |
|------------------|-------------------------|----------------------------------------|
| Streaming        | Apache Kafka            | Real-time sensor event broker          |
| Object Storage   | MinIO (S3-compatible)   | Data lake — Bronze/Silver/Gold         |
| Orchestration    | Apache Airflow          | Hourly DAG scheduling with retry logic |
| Processing       | Apache PySpark          | Distributed medallion transforms       |
| Warehouse        | PostgreSQL              | Aggregated metrics for dashboards      |
| Visualisation    | Grafana                 | Live equipment health monitoring       |
| Containerisation | Docker + docker-compose | Full stack in one command              |
| CI/CD            | GitHub Actions          | Linting + tests on every push          |

---

## 🚀 How to Run the Project

### Prerequisites
- Docker Desktop installed and running
- Python 3.11+
- Git

### Step 1 — Clone and set up

```bash
git clone https://github.com/adhithyan-s/Oil-and-Gas-IoT-Sensor-Pipeline.git
cd Oil-and-Gas-IoT-Sensor-Pipeline

python -m venv .venv
source .venv/bin/activate          
pip install -r requirements.txt
```

### Step 2 — Start all services

```bash
docker-compose up -d
```

First run downloads images (~5 min). Subsequent runs are instant.

Verify everything is healthy:

```bash
docker-compose ps
```

All services should show `Up` or `healthy`.

### Step 3 — Start the sensor simulator (Terminal 1)

```bash
source .venv/bin/activate
python ingestion/simulator/sensor_simulator.py
```

Leave this running. You will see a log line every 5 seconds confirming messages are sent to Kafka.

### Step 4 — Start the Kafka consumer (Terminal 2)

```bash
source .venv/bin/activate
python ingestion/consumer/kafka_consumer.py
```

Leave this running. Every 30 seconds you will see a log confirming raw JSON files are written to MinIO.

### Step 5 — Let Airflow run the pipeline automatically

Airflow runs the full medallion pipeline every hour at :05 automatically.

To trigger it manually right now:
1. Open http://localhost:8088 and login with `admin / admin`
2. Find `iot_sensor_pipeline` and click Trigger DAG
3. Watch the 4 tasks turn green one by one

### Step 6 — View the dashboard

Open http://localhost:3000 and login with `admin / admin` then go to Dashboards and open IoT Sensor Pipeline — Equipment Health.

You will see live equipment health scores, active alerts, and anomaly rates across all 5 wells.

---

### Running transforms manually (for debugging)

If you need to run PySpark jobs directly without Airflow, check MinIO at http://localhost:9001 to find what hour your Bronze data landed in, then use this pattern:

```bash
python - <<'EOF'
from datetime import datetime, timezone
import sys
sys.path.insert(0, '.')
from processing.spark_jobs.bronze_to_silver import run

window_start = datetime(2026, 3, 30, 5, 0, 0, tzinfo=timezone.utc)
window_end   = datetime(2026, 3, 30, 6, 0, 0, tzinfo=timezone.utc)
run(window_start=window_start, window_end=window_end)
EOF
```

Use the same pattern for `silver_to_gold`. Replace the hour with whatever hour your data is in.

---

## 🌐 Service URLs and Credentials

| Service           | URL                   | Username     | Password     | What you can do                        |
|-------------------|-----------------------|--------------|--------------|----------------------------------------|
| **Kafka UI**      | http://localhost:8080 | —            | —            | View topics, messages, consumer groups |
| **MinIO Console** | http://localhost:9001 | `minioadmin` | `minioadmin` | Browse Bronze/Silver/Gold buckets      |
| **Airflow UI**    | http://localhost:8088 | `admin`      | `admin`      | Trigger and monitor DAGs               |
| **Grafana**       | http://localhost:3000 | `admin`      | `admin`      | Equipment health dashboard             |
| **PostgreSQL**    | localhost:5433        | `iotuser`    | `iotpass`    | DB: `iotdb` — tables: `well_health`, `sensor_alerts` |

PostgreSQL is exposed on port 5433 (not 5432) to avoid conflicts with any local PostgreSQL installation. Inside Docker, services communicate on port 5432 as normal.

---

## 📁 Project Structure

```
Oil-and-Gas-IoT-Sensor-Pipeline/
├── docker-compose.yml                   # Starts all 7 services with one command
├── requirements.txt                     # Full Python dependencies
├── requirements-ci.txt                  # Lightweight deps for CI (no Spark/Airflow)
├── .github/
│   └── workflows/
│       └── ci.yml                       # GitHub Actions: lint + test on every push
├── ingestion/
│   ├── simulator/
│   │   └── sensor_simulator.py          # Produces sensor events to Kafka
│   └── consumer/
│       └── kafka_consumer.py            # Consumes Kafka, writes NDJSON to MinIO
├── processing/
│   └── spark_jobs/
│       ├── bronze_to_silver.py          # Cleans raw data, validates, flags anomalies
│       └── silver_to_gold.py            # Computes KPIs, health scores, writes to PG
├── orchestration/
│   └── dags/
│       └── sensor_pipeline_dag.py       # Airflow DAG — hourly medallion pipeline
├── monitoring/
│   └── grafana/
│       └── provisioning/
│           ├── datasources/
│           │   └── postgres_datasource.yml
│           └── dashboards/
│               ├── dashboard_provider.yml
│               └── iot_dashboard.json
├── tests/
│   ├── __init__.py
│   └── test_simulator.py                # Unit tests for simulator and path builder
└── docs/
    └── grafana_dashboard.png            # Dashboard screenshot
```

---

## 🔄 CI/CD

GitHub Actions runs on every push to main:

1. Installs lightweight dependencies from `requirements-ci.txt`
2. Runs `flake8` — checks for syntax errors and style issues
3. Runs `pytest` — executes all unit tests

The green badge at the top of this README is live and reflects the current state of the last push.

---

## 💡 Key Engineering Decisions

**Why Kafka over a simple REST API?**
Kafka decouples the producer (sensors) from the consumer (storage). If the consumer goes down, Kafka retains messages and delivers them when it recovers. This mirrors how real industrial IoT systems handle unreliable network conditions. A REST API would lose data during downtime.

**Why MinIO instead of real AWS S3?**
MinIO exposes an identical S3 API. The same boto3 code that writes to MinIO works against real AWS S3 — only the endpoint_url changes. This makes local development free while keeping the code production-ready. Environment variables control which endpoint is used so the same scripts run locally and inside Docker without any code changes.

**Why NDJSON for Bronze, Parquet for Silver/Gold?**
Bronze is append-only raw storage — NDJSON is human-readable and easy to inspect when debugging. Silver and Gold use Parquet because it is columnar (reading one sensor type does not touch others), compressed (5-10x smaller), and schema-aware. PySpark reads Parquet significantly faster than JSON at scale.

**Why date-partitioned folder structure?**
The year=/month=/day=/hour=/ structure means Spark only reads the folders it needs. Processing one hour of data does not scan the rest of the dataset. This is called partition pruning — on large datasets it makes queries dramatically faster and is standard practice in data lakes.

**Why Airflow instead of cron?**
Cron runs scripts but gives zero visibility into failures, retries, or execution history. Airflow provides a visual UI, automatic retries with exponential backoff, dependency management between tasks, and deterministic time-windowed execution via execution_date. A cron job that fails silently is invisible. An Airflow task that fails is immediately visible, retried automatically, and logged in full.

**Why a health score instead of raw metrics?**
Operators do not want to monitor 25 individual sensor streams per well. A single 0-100 health score with HEALTHY/WARNING/CRITICAL status lets them identify which well needs attention at a glance. The score is derived from anomaly rate with tunable penalty weights — adjustable for different equipment types without changing pipeline code.

