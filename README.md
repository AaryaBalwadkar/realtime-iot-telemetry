# IoT Sensor Data Processing Using Spark Streaming

<div align="center">
  <img src="https://img.shields.io/badge/Python-3776AB?style=flat-square&logo=python&logoColor=white" alt="Python" />
  <img src="https://img.shields.io/badge/Kafka-231F20?style=flat-square&logo=apachekafka&logoColor=white" alt="Kafka" />
  <img src="https://img.shields.io/badge/MQTT-660066?style=flat-square&logo=mqtt&logoColor=white" alt="MQTT" />
  <img src="https://img.shields.io/badge/Spark-E25A1C?style=flat-square&logo=apachespark&logoColor=white" alt="Spark" />
  <img src="https://img.shields.io/badge/MinIO-C7202C?style=flat-square&logo=minio&logoColor=white" alt="MinIO" />
  <img src="https://img.shields.io/badge/PostgreSQL-4169E1?style=flat-square&logo=postgresql&logoColor=white" alt="PostgreSQL" />
  <img src="https://img.shields.io/badge/Grafana-F46800?style=flat-square&logo=grafana&logoColor=white" alt="Grafana" />
  <img src="https://img.shields.io/badge/Docker-2496ED?style=flat-square&logo=docker&logoColor=white" alt="Docker" />

  <img src="https://readme-typing-svg.herokuapp.com?font=Bricolage+Grotesque&weight=800&size=24&pause=1000&color=FF991C&center=true&vCenter=true&width=600&lines=Simulating+telemetry+via+Python...;Bridging+messages+with+MQTT...;Processing+events+in+Apache+Kafka...;Aggregating+windows+in+PySpark...;Archiving+raw+batches+to+MinIO...;Persisting+anomalies+in+PostgreSQL...;Visualizing+metrics+with+Grafana..." alt="Typing SVG" />
</div>

An end-to-end, containerized pipeline that simulates enterprise-grade edge IoT
sensor telemetry, streams it through Apache Kafka, processes it in real time
with PySpark Structured Streaming, archives raw batches to MinIO (S3-compatible
object storage), persists anomalies and rolling aggregates to PostgreSQL, and
visualizes everything in Grafana.

![IoT Pipeline Architecture](assests/architecture.png)

## Directory Layout

```
.
├── docker-compose.yml
├── db/
│   └── init.sql
├── minio_init/
│   └── init_buckets.sh
├── producer/
│   ├── producer.py
│   ├── Dockerfile
│   └── requirements.txt
├── spark_processor/
│   ├── stream_processor.py
│   ├── Dockerfile
│   └── requirements.txt
├── grafana/
│   └── provisioning/
├── mosquitto/
│   └── config/
└── .github/
    └── workflows/
        └── ci.yml
```

> **Note:** place `producer/requirements.txt` (kafka-python, paho-mqtt, numpy,
> python-dateutil) and `spark_processor/requirements.txt` (pyspark, boto3,
> botocore, psycopg2-binary, py4j) alongside each component's Dockerfile as
> shown above — the CI workflow and Dockerfiles both expect them there.

## Prerequisites

- Docker Engine 24+ and Docker Compose v2 (`docker compose version`)
- ~6 GB free RAM for the full stack (Kafka + Spark are the heaviest consumers)
- Ports `1883, 2181, 3000, 4040, 5432, 9000, 9001, 9090, 9092, 29092` free on the host

## 1. Start the Full Stack

From the repository root:

```bash
docker compose up --build
```

This builds the `producer` and `spark-processor` images, then starts, in
dependency order: Zookeeper → Kafka → Mosquitto → MinIO → `minio-init`
(bucket bootstrap) → PostgreSQL (auto-runs `db/init.sql`) → `producer` →
`spark-processor` → Grafana.

To run it in the background instead:

```bash
docker compose up --build -d
```

Check that every service is healthy:

```bash
docker compose ps
```

## 2. View Logs

```bash
# All services, tailed live
docker compose logs -f

# Just the simulator (confirm it's emitting readings)
docker compose logs -f producer

# Just the Spark job (confirm batches are processing)
docker compose logs -f spark-processor

# Kafka / MinIO / Postgres individually
docker compose logs -f kafka
docker compose logs -f minio
docker compose logs -f postgres
```

You can also watch the Spark Structured Streaming UI at
**http://localhost:4040** while `spark-processor` is running.

## 3. Verify Raw Archives in MinIO

**Web console** — open **http://localhost:9090** and log in with:
- Username: `minioadmin`
- Password: `minioadmin123`

Browse the `iot-telemetry-raw` bucket → `raw/year=.../month=.../day=.../` to
see archived NDJSON batches, and `iot-telemetry-processed` for the secondary
bucket created during init.

**CLI (via the `mc` client inside a throwaway container):**

```bash
docker run --rm --network iot-net_default minio/mc:RELEASE.2024-08-17T11-33-50Z \
  sh -c "mc alias set localminio http://minio:9000 minioadmin minioadmin123 && \
         mc ls --recursive localminio/iot-telemetry-raw"
```

> Replace `iot-net_default` with your actual Compose network name if it
> differs — check with `docker network ls`. It's typically
> `<project-directory-name>_iot-net`.

## 4. Query PostgreSQL

Open a `psql` shell inside the running Postgres container:

```bash
docker compose exec postgres psql -U iot_user -d iot_db
```

Then run:

```sql
-- Recent anomaly events
SELECT device_id, event_timestamp, temperature, vibration, anomaly_type, severity
FROM iot.anomaly_events
ORDER BY event_timestamp DESC
LIMIT 20;

-- Per-device anomaly counts
SELECT * FROM iot.anomaly_counts_by_device;

-- Latest rolling window aggregates
SELECT device_id, window_start, window_end, avg_temperature, max_vibration, reading_count
FROM iot.aggregated_metrics
ORDER BY window_end DESC
LIMIT 20;

-- Raw batch archive manifest (cross-reference with MinIO)
SELECT batch_id, s3_bucket, s3_key, record_count, written_at
FROM iot.raw_batch_manifest
ORDER BY written_at DESC
LIMIT 20;
```

One-liner from the host (no interactive shell):

```bash
docker compose exec -T postgres psql -U iot_user -d iot_db -c \
  "SELECT count(*) FROM iot.anomaly_events;"
```

## 5. Connect Grafana

1. Open **http://localhost:3000**
2. Log in with `admin` / `admin123`
3. Add a **PostgreSQL** data source (Configuration → Data sources → Add
   data source → PostgreSQL) if it isn't already provisioned:
   - Host: `postgres:5432`
   - Database: `iot_db`
   - User: `iot_user`
   - Password: `iot_password`
   - TLS/SSL Mode: `disable`
4. Build panels against `iot.anomaly_events`, `iot.aggregated_metrics`, and
   the `iot.recent_anomalies` / `iot.anomaly_counts_by_device` views — e.g. a
   time series panel on `avg_temperature` grouped by `device_id`, and a table
   panel on the most recent anomalies.

## 6. Port Reference

| Service          | Host Port | Purpose                              |
|------------------|-----------|---------------------------------------|
| Kafka            | 9092      | Internal broker listener              |
| Kafka            | 29092     | Host-accessible broker listener       |
| Mosquitto        | 1883      | MQTT                                  |
| Mosquitto        | 9001      | MQTT over WebSockets                  |
| MinIO API        | 9000      | S3 API                                |
| MinIO Console    | 9090      | Web console                           |
| PostgreSQL       | 5432      | SQL                                   |
| Spark UI         | 4040      | Structured Streaming job UI           |
| Grafana          | 3000      | Dashboards                            |

## 7. Stopping & Cleanup

```bash
# Stop containers, keep volumes (data persists)
docker compose down

# Stop and remove all volumes (full reset, including MinIO/Postgres data)
docker compose down -v
```

## 8. Troubleshooting

- **`spark-processor` restarts repeatedly on first boot** — it waits for
  Kafka, MinIO, and Postgres healthchecks via `depends_on`, but Spark's own
  package resolution (`spark.jars.packages`) needs outbound network access on
  first run to fetch the Kafka/S3A/Postgres JDBC connector jars from Maven
  Central. Confirm the container has internet access, then re-run
  `docker compose up --build spark-processor`.
- **No anomalies appearing** — the producer only injects anomalies with
  probability `ANOMALY_PROBABILITY` (default `0.05`) per device per round;
  give it a minute, or temporarily raise `ANOMALY_PROBABILITY` /
  lower `TEMP_THRESHOLD_C` / `VIBRATION_THRESHOLD_HZ` in `docker-compose.yml`.
- **Port already in use** — stop the conflicting local service or remap the
  host-side port in `docker-compose.yml` (left side of each `"host:container"`
  pair).
- **Grafana data source can't connect** — make sure you used the Docker
  service name `postgres` (not `localhost`) as the host, since Grafana
  resolves it over the `iot-net` Docker network.
