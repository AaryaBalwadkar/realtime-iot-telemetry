#!/usr/bin/env python3
"""
IoT Sensor Data Processing - Spark Structured Streaming Processor
====================================================================
Consumes real-time IoT telemetry from Apache Kafka, computes rolling
windowed aggregations (30s / 1m) of temperature and vibration, flags
threshold-breach anomalies (temp > 85C and vibration > 90Hz) as
"ANOMALY_DETECTED", archives raw telemetry batches to MinIO (S3A /
boto3) as JSON, and writes anomaly events + aggregated metrics to
PostgreSQL for structured querying and Grafana visualization.
"""

import json
import logging
import os
import sys
from datetime import datetime, timezone

import boto3
from botocore.client import Config as BotoConfig
from botocore.exceptions import ClientError

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, BooleanType, TimestampType
)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] stream_processor :: %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("stream-processor")

# ---------------------------------------------------------------------------
# Configuration (env-driven, aligned with docker-compose.yml)
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "iot-sensor-telemetry")

MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.environ.get("MINIO_SECRET_KEY", "minioadmin123")
MINIO_BUCKET = os.environ.get("MINIO_BUCKET", "iot-telemetry-raw")

POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.environ.get("POSTGRES_PORT", "5432")
POSTGRES_DB = os.environ.get("POSTGRES_DB", "iot_db")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "iot_user")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "iot_password")

TEMP_THRESHOLD_C = float(os.environ.get("TEMP_THRESHOLD_C", "85.0"))
VIBRATION_THRESHOLD_HZ = float(os.environ.get("VIBRATION_THRESHOLD_HZ", "90.0"))

SPARK_CHECKPOINT_DIR = os.environ.get("SPARK_CHECKPOINT_DIR", "/tmp/spark-checkpoints")

JDBC_URL = f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
JDBC_PROPERTIES = {
    "user": POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "driver": "org.postgresql.Driver",
}

# ---------------------------------------------------------------------------
# Telemetry schema (matches producer.py JSON payload)
# ---------------------------------------------------------------------------
TELEMETRY_SCHEMA = StructType([
    StructField("device_id", StringType(), nullable=False),
    StructField("device_type", StringType(), nullable=True),
    StructField("location", StringType(), nullable=True),
    StructField("timestamp", StringType(), nullable=False),
    StructField("temperature", DoubleType(), nullable=True),
    StructField("humidity", DoubleType(), nullable=True),
    StructField("pressure", DoubleType(), nullable=True),
    StructField("vibration", DoubleType(), nullable=True),
    StructField("simulated_anomaly", BooleanType(), nullable=True),
    StructField("reading_id", StringType(), nullable=True),
])


# ---------------------------------------------------------------------------
# Spark session
# ---------------------------------------------------------------------------
def build_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("IoTSensorStreamProcessor")
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    logger.info("Spark session initialized: %s", spark.version)
    return spark


# ---------------------------------------------------------------------------
# boto3 S3 (MinIO) client for raw batch archival
# ---------------------------------------------------------------------------
def build_s3_client():
    client = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=BotoConfig(signature_version="s3v4"),
        region_name="us-east-1",
    )
    return client


def ensure_bucket_exists(s3_client, bucket_name: str):
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        logger.info("Confirmed MinIO bucket exists: %s", bucket_name)
    except ClientError:
        logger.info("Bucket '%s' not found via head_bucket; attempting creation.", bucket_name)
        try:
            s3_client.create_bucket(Bucket=bucket_name)
            logger.info("Created MinIO bucket: %s", bucket_name)
        except ClientError as exc:
            logger.warning("Could not create bucket '%s' (may already exist): %s", bucket_name, exc)


# ---------------------------------------------------------------------------
# Read stream from Kafka
# ---------------------------------------------------------------------------
def read_kafka_stream(spark: SparkSession) -> DataFrame:
    raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed_df = (
        raw_df
        .selectExpr("CAST(value AS STRING) AS json_value", "timestamp AS kafka_timestamp")
        .withColumn("data", F.from_json(F.col("json_value"), TELEMETRY_SCHEMA))
        .select("data.*", "kafka_timestamp")
        .withColumn("event_time", F.to_timestamp(F.col("timestamp")))
        .withColumn(
            "event_time",
            F.coalesce(F.col("event_time"), F.col("kafka_timestamp"))
        )
        .withWatermark("event_time", "2 minutes")
    )
    return parsed_df


# ---------------------------------------------------------------------------
# Anomaly detection logic
# ---------------------------------------------------------------------------
def detect_anomalies(telemetry_df: DataFrame) -> DataFrame:
    """
    Flags readings that breach configured thresholds as ANOMALY_DETECTED.
    Business rule: temperature > TEMP_THRESHOLD_C OR vibration > VIBRATION_THRESHOLD_HZ
    triggers an anomaly; when BOTH breach simultaneously severity is CRITICAL.
    """
    anomalies_df = (
        telemetry_df
        .withColumn(
            "temp_breach",
            F.col("temperature") > F.lit(TEMP_THRESHOLD_C)
        )
        .withColumn(
            "vibration_breach",
            F.col("vibration") > F.lit(VIBRATION_THRESHOLD_HZ)
        )
        .filter(F.col("temp_breach") | F.col("vibration_breach"))
        .withColumn("anomaly_type", F.lit("ANOMALY_DETECTED"))
        .withColumn(
            "severity",
            F.when(F.col("temp_breach") & F.col("vibration_breach"), F.lit("CRITICAL"))
             .otherwise(F.lit("HIGH"))
        )
        .withColumn("temp_threshold", F.lit(TEMP_THRESHOLD_C))
        .withColumn("vibration_threshold", F.lit(VIBRATION_THRESHOLD_HZ))
        .select(
            F.col("device_id"),
            F.col("event_time").alias("event_timestamp"),
            F.col("temperature"),
            F.col("humidity"),
            F.col("pressure"),
            F.col("vibration"),
            F.col("anomaly_type"),
            F.col("temp_threshold"),
            F.col("vibration_threshold"),
            F.col("severity"),
        )
    )
    return anomalies_df


# ---------------------------------------------------------------------------
# Rolling window aggregations (30s and 1m)
# ---------------------------------------------------------------------------
def compute_windowed_aggregations(telemetry_df: DataFrame, window_duration: str, slide_duration: str = None) -> DataFrame:
    window_col = (
        F.window(F.col("event_time"), window_duration, slide_duration)
        if slide_duration else
        F.window(F.col("event_time"), window_duration)
    )

    agg_df = (
        telemetry_df
        .groupBy(F.col("device_id"), window_col)
        .agg(
            F.avg("temperature").alias("avg_temperature"),
            F.max("temperature").alias("max_temperature"),
            F.min("temperature").alias("min_temperature"),
            F.avg("humidity").alias("avg_humidity"),
            F.avg("pressure").alias("avg_pressure"),
            F.avg("vibration").alias("avg_vibration"),
            F.max("vibration").alias("max_vibration"),
            F.count(F.lit(1)).alias("reading_count"),
        )
        .select(
            F.col("device_id"),
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            F.lit(window_duration).alias("window_duration"),
            F.col("avg_temperature"),
            F.col("max_temperature"),
            F.col("min_temperature"),
            F.col("avg_humidity"),
            F.col("avg_pressure"),
            F.col("avg_vibration"),
            F.col("max_vibration"),
            F.col("reading_count"),
        )
    )
    return agg_df


# ---------------------------------------------------------------------------
# Sink: archive raw telemetry batch to MinIO (S3) via boto3
# ---------------------------------------------------------------------------
def archive_raw_batch_to_minio(batch_df: DataFrame, batch_id: int, s3_client, jdbc_url: str, jdbc_props: dict):
    row_count = batch_df.count()
    if row_count == 0:
        logger.info("[batch=%d] Empty raw telemetry batch, skipping MinIO archive.", batch_id)
        return

    records = [row.asDict(recursive=True) for row in batch_df.collect()]

    def _json_default(obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return str(obj)

    payload_str = "\n".join(json.dumps(r, default=_json_default) for r in records)

    now = datetime.now(timezone.utc)
    partition_prefix = now.strftime("year=%Y/month=%m/day=%d/hour=%H")
    object_key = f"raw/{partition_prefix}/batch-{batch_id}-{now.strftime('%Y%m%dT%H%M%S%f')}.json"

    try:
        s3_client.put_object(
            Bucket=MINIO_BUCKET,
            Key=object_key,
            Body=payload_str.encode("utf-8"),
            ContentType="application/x-ndjson",
        )
        logger.info(
            "[batch=%d] Archived %d raw records to s3://%s/%s",
            batch_id, row_count, MINIO_BUCKET, object_key,
        )
    except ClientError:
        logger.exception("[batch=%d] Failed to write raw batch to MinIO", batch_id)
        return

    # Record manifest entry in Postgres so the archive is cross-referenced
    manifest_df = batch_df.sparkSession.createDataFrame(
        [(batch_id, MINIO_BUCKET, object_key, row_count, "json")],
        schema=["batch_id", "s3_bucket", "s3_key", "record_count", "file_format"],
    )
    try:
        (
            manifest_df.write
            .jdbc(url=jdbc_url, table="iot.raw_batch_manifest", mode="append", properties=jdbc_props)
        )
    except Exception:  # noqa: BLE001
        logger.exception("[batch=%d] Failed to write manifest row to Postgres", batch_id)


# ---------------------------------------------------------------------------
# Sink: write anomaly events to PostgreSQL
# ---------------------------------------------------------------------------
def write_anomalies_to_postgres(batch_df: DataFrame, batch_id: int, jdbc_url: str, jdbc_props: dict):
    row_count = batch_df.count()
    if row_count == 0:
        return

    enriched_df = batch_df.withColumn("batch_id", F.lit(batch_id))

    try:
        (
            enriched_df.write
            .jdbc(url=jdbc_url, table="iot.anomaly_events", mode="append", properties=jdbc_props)
        )
        logger.info("[batch=%d] Wrote %d anomaly event(s) to PostgreSQL.", batch_id, row_count)
    except Exception:  # noqa: BLE001
        logger.exception("[batch=%d] Failed to write anomaly events to PostgreSQL", batch_id)


# ---------------------------------------------------------------------------
# Sink: upsert windowed aggregations to PostgreSQL
# ---------------------------------------------------------------------------
def write_aggregations_to_postgres(batch_df: DataFrame, batch_id: int, jdbc_url: str, jdbc_props: dict):
    row_count = batch_df.count()
    if row_count == 0:
        return

    enriched_df = batch_df.withColumn("batch_id", F.lit(batch_id))

    try:
        (
            enriched_df.write
            .jdbc(url=jdbc_url, table="iot.aggregated_metrics", mode="append", properties=jdbc_props)
        )
        logger.info("[batch=%d] Wrote %d windowed aggregation row(s) to PostgreSQL.", batch_id, row_count)
    except Exception:  # noqa: BLE001
        logger.exception("[batch=%d] Failed to write aggregations to PostgreSQL", batch_id)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    logger.info("=" * 70)
    logger.info("IoT Sensor Stream Processor starting up")
    logger.info("Kafka: %s / topic=%s", KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC)
    logger.info("MinIO: %s / bucket=%s", MINIO_ENDPOINT, MINIO_BUCKET)
    logger.info("Postgres: %s", JDBC_URL)
    logger.info("Thresholds: temp>%.1fC OR vibration>%.1fHz => ANOMALY_DETECTED",
                TEMP_THRESHOLD_C, VIBRATION_THRESHOLD_HZ)
    logger.info("=" * 70)

    spark = build_spark_session()
    s3_client = build_s3_client()
    ensure_bucket_exists(s3_client, MINIO_BUCKET)

    telemetry_df = read_kafka_stream(spark)

    # --- Stream 1: raw batch archival to MinIO -----------------------------
    raw_query = (
        telemetry_df
        .writeStream
        .foreachBatch(
            lambda batch_df, batch_id: archive_raw_batch_to_minio(
                batch_df, batch_id, s3_client, JDBC_URL, JDBC_PROPERTIES
            )
        )
        .option("checkpointLocation", f"{SPARK_CHECKPOINT_DIR}/raw_archive")
        .trigger(processingTime="15 seconds")
        .start()
    )

    # --- Stream 2: anomaly detection -> PostgreSQL --------------------------
    anomalies_df = detect_anomalies(telemetry_df)
    anomaly_query = (
        anomalies_df
        .writeStream
        .foreachBatch(
            lambda batch_df, batch_id: write_anomalies_to_postgres(
                batch_df, batch_id, JDBC_URL, JDBC_PROPERTIES
            )
        )
        .option("checkpointLocation", f"{SPARK_CHECKPOINT_DIR}/anomalies")
        .trigger(processingTime="10 seconds")
        .start()
    )

    # --- Stream 3: 30-second rolling window aggregation ---------------------
    agg_30s_df = compute_windowed_aggregations(telemetry_df, "30 seconds")
    agg_30s_query = (
        agg_30s_df
        .writeStream
        .outputMode("update")
        .foreachBatch(
            lambda batch_df, batch_id: write_aggregations_to_postgres(
                batch_df, batch_id, JDBC_URL, JDBC_PROPERTIES
            )
        )
        .option("checkpointLocation", f"{SPARK_CHECKPOINT_DIR}/agg_30s")
        .trigger(processingTime="30 seconds")
        .start()
    )

    # --- Stream 4: 1-minute rolling window aggregation -----------------------
    agg_1m_df = compute_windowed_aggregations(telemetry_df, "1 minute")
    agg_1m_query = (
        agg_1m_df
        .writeStream
        .outputMode("update")
        .foreachBatch(
            lambda batch_df, batch_id: write_aggregations_to_postgres(
                batch_df, batch_id, JDBC_URL, JDBC_PROPERTIES
            )
        )
        .option("checkpointLocation", f"{SPARK_CHECKPOINT_DIR}/agg_1m")
        .trigger(processingTime="60 seconds")
        .start()
    )

    # --- Stream 5: console sink for live debugging (stdout) ------------------
    console_query = (
        anomalies_df
        .writeStream
        .outputMode("append")
        .format("console")
        .option("truncate", "false")
        .option("numRows", 20)
        .trigger(processingTime="10 seconds")
        .start()
    )

    logger.info("All streaming queries started. Awaiting termination...")

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
