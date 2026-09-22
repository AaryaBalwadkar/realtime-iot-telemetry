#!/usr/bin/env python3
"""
IoT Sensor Data Producer / Simulator
=====================================
Simulates a fleet of multi-sensor IoT edge devices emitting telemetry
(device_id, timestamp, temperature, humidity, pressure, vibration) and
publishes the JSON payloads continuously to either Apache Kafka or
Eclipse Mosquitto (MQTT), selectable via the PUBLISH_MODE env var.

Deliberately injects periodic edge-case anomalies (temperature spikes,
vibration spikes) so downstream Spark Structured Streaming anomaly
detection logic has real signal to catch.
"""

import json
import logging
import os
import random
import signal
import sys
import time
import uuid
from datetime import datetime, timezone

import numpy as np

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] producer :: %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("iot-producer")

# ---------------------------------------------------------------------------
# Configuration (env-driven, defaults align with docker-compose.yml)
# ---------------------------------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "iot-sensor-telemetry")

MQTT_BROKER_HOST = os.environ.get("MQTT_BROKER_HOST", "mosquitto")
MQTT_BROKER_PORT = int(os.environ.get("MQTT_BROKER_PORT", "1883"))
MQTT_TOPIC = os.environ.get("MQTT_TOPIC", "iot/sensors/telemetry")

NUM_DEVICES = int(os.environ.get("NUM_DEVICES", "10"))
EMIT_INTERVAL_SECONDS = float(os.environ.get("EMIT_INTERVAL_SECONDS", "1"))
ANOMALY_PROBABILITY = float(os.environ.get("ANOMALY_PROBABILITY", "0.05"))
PUBLISH_MODE = os.environ.get("PUBLISH_MODE", "kafka").strip().lower()  # "kafka" | "mqtt" | "both"

# Baseline "normal operating" sensor ranges per metric
TEMP_BASELINE_MEAN_C = 45.0
TEMP_BASELINE_STD_C = 8.0
HUMIDITY_BASELINE_MEAN_PCT = 50.0
HUMIDITY_BASELINE_STD_PCT = 10.0
PRESSURE_BASELINE_MEAN_KPA = 101.3
PRESSURE_BASELINE_STD_KPA = 1.5
VIBRATION_BASELINE_MEAN_HZ = 35.0
VIBRATION_BASELINE_STD_HZ = 12.0

# Anomaly injection ranges (deliberately breach downstream thresholds:
# temp > 85C and vibration > 90Hz as per architecture spec)
ANOMALY_TEMP_MIN_C = 86.0
ANOMALY_TEMP_MAX_C = 120.0
ANOMALY_VIBRATION_MIN_HZ = 91.0
ANOMALY_VIBRATION_MAX_HZ = 150.0

DEVICE_TYPES = ["thermal-sensor", "vibration-sensor", "multi-sensor-node", "compressor-monitor"]
LOCATIONS = [
    "factory-floor-a", "factory-floor-b", "warehouse-north",
    "warehouse-south", "cold-storage-1", "assembly-line-3",
]

_shutdown_requested = False


def _handle_shutdown(signum, frame):  # noqa: ARG001
    global _shutdown_requested
    logger.info("Received signal %s, shutting down gracefully...", signum)
    _shutdown_requested = True


signal.signal(signal.SIGTERM, _handle_shutdown)
signal.signal(signal.SIGINT, _handle_shutdown)


# ---------------------------------------------------------------------------
# Device fleet initialization
# ---------------------------------------------------------------------------
def build_device_fleet(num_devices: int):
    """Create a deterministic, reproducible fleet of simulated devices."""
    fleet = []
    for i in range(num_devices):
        device_id = f"device-{i:04d}-{uuid.uuid4().hex[:6]}"
        fleet.append(
            {
                "device_id": device_id,
                "device_type": random.choice(DEVICE_TYPES),
                "location": random.choice(LOCATIONS),
                # Small per-device baseline drift so not all devices look identical
                "temp_offset": np.random.normal(0, 3.0),
                "humidity_offset": np.random.normal(0, 4.0),
                "pressure_offset": np.random.normal(0, 0.5),
                "vibration_offset": np.random.normal(0, 5.0),
            }
        )
    logger.info("Initialized simulated fleet of %d devices.", len(fleet))
    return fleet


# ---------------------------------------------------------------------------
# Telemetry generation
# ---------------------------------------------------------------------------
def generate_reading(device: dict, force_anomaly: bool = False) -> dict:
    """Generate one realistic (or deliberately anomalous) telemetry reading."""
    is_anomaly = force_anomaly or (random.random() < ANOMALY_PROBABILITY)

    if is_anomaly:
        # Randomly choose whether the anomaly is temperature-driven,
        # vibration-driven, or both (compound failure scenario)
        anomaly_kind = random.choice(["temperature", "vibration", "both"])

        temperature = round(
            np.random.uniform(ANOMALY_TEMP_MIN_C, ANOMALY_TEMP_MAX_C), 2
        ) if anomaly_kind in ("temperature", "both") else round(
            max(0.0, np.random.normal(TEMP_BASELINE_MEAN_C + device["temp_offset"], TEMP_BASELINE_STD_C)), 2
        )

        vibration = round(
            np.random.uniform(ANOMALY_VIBRATION_MIN_HZ, ANOMALY_VIBRATION_MAX_HZ), 2
        ) if anomaly_kind in ("vibration", "both") else round(
            max(0.0, np.random.normal(VIBRATION_BASELINE_MEAN_HZ + device["vibration_offset"], VIBRATION_BASELINE_STD_HZ)), 2
        )
    else:
        temperature = round(
            max(0.0, np.random.normal(TEMP_BASELINE_MEAN_C + device["temp_offset"], TEMP_BASELINE_STD_C)), 2
        )
        vibration = round(
            max(0.0, np.random.normal(VIBRATION_BASELINE_MEAN_HZ + device["vibration_offset"], VIBRATION_BASELINE_STD_HZ)), 2
        )

    humidity = round(
        min(100.0, max(0.0, np.random.normal(
            HUMIDITY_BASELINE_MEAN_PCT + device["humidity_offset"], HUMIDITY_BASELINE_STD_PCT
        ))), 2
    )
    pressure = round(
        max(0.0, np.random.normal(
            PRESSURE_BASELINE_MEAN_KPA + device["pressure_offset"], PRESSURE_BASELINE_STD_KPA
        )), 2
    )

    payload = {
        "device_id": device["device_id"],
        "device_type": device["device_type"],
        "location": device["location"],
        "timestamp": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
        "temperature": temperature,
        "humidity": humidity,
        "pressure": pressure,
        "vibration": vibration,
        "simulated_anomaly": is_anomaly,
        "reading_id": str(uuid.uuid4()),
    }
    return payload


# ---------------------------------------------------------------------------
# Kafka publisher
# ---------------------------------------------------------------------------
class KafkaPublisher:
    def __init__(self, bootstrap_servers: str, topic: str):
        from kafka import KafkaProducer
        from kafka.errors import NoBrokersAvailable

        self.topic = topic
        self._NoBrokersAvailable = NoBrokersAvailable
        self._producer = None
        self._bootstrap_servers = bootstrap_servers
        self._connect_with_retry()

    def _connect_with_retry(self, max_retries: int = 30, delay_seconds: float = 3.0):
        from kafka import KafkaProducer

        attempt = 0
        while attempt < max_retries:
            try:
                self._producer = KafkaProducer(
                    bootstrap_servers=self._bootstrap_servers,
                    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                    key_serializer=lambda k: k.encode("utf-8") if k else None,
                    acks="all",
                    retries=5,
                    linger_ms=50,
                )
                logger.info("Connected to Kafka broker(s): %s", self._bootstrap_servers)
                return
            except self._NoBrokersAvailable:
                attempt += 1
                logger.warning(
                    "Kafka broker not yet available (attempt %d/%d). Retrying in %.1fs...",
                    attempt, max_retries, delay_seconds,
                )
                time.sleep(delay_seconds)
        raise RuntimeError(f"Could not connect to Kafka after {max_retries} attempts.")

    def publish(self, payload: dict):
        self._producer.send(self.topic, key=payload["device_id"], value=payload)

    def flush(self):
        if self._producer:
            self._producer.flush()

    def close(self):
        if self._producer:
            self._producer.flush()
            self._producer.close()


# ---------------------------------------------------------------------------
# MQTT publisher
# ---------------------------------------------------------------------------
class MqttPublisher:
    def __init__(self, host: str, port: int, topic: str):
        import paho.mqtt.client as mqtt

        self.topic = topic
        self._client = mqtt.Client(client_id=f"iot-producer-{uuid.uuid4().hex[:8]}")
        self._connect_with_retry(host, port)

    def _connect_with_retry(self, host: str, port: int, max_retries: int = 30, delay_seconds: float = 3.0):
        attempt = 0
        while attempt < max_retries:
            try:
                self._client.connect(host, port, keepalive=60)
                self._client.loop_start()
                logger.info("Connected to MQTT broker at %s:%d", host, port)
                return
            except (ConnectionRefusedError, OSError) as exc:
                attempt += 1
                logger.warning(
                    "MQTT broker not yet available (%s). Attempt %d/%d. Retrying in %.1fs...",
                    exc, attempt, max_retries, delay_seconds,
                )
                time.sleep(delay_seconds)
        raise RuntimeError(f"Could not connect to MQTT broker after {max_retries} attempts.")

    def publish(self, payload: dict):
        device_topic = f"{self.topic}/{payload['device_id']}"
        self._client.publish(device_topic, json.dumps(payload), qos=0)
        # Also publish to the aggregate topic for consumers that subscribe broadly
        self._client.publish(self.topic, json.dumps(payload), qos=0)

    def flush(self):
        pass

    def close(self):
        self._client.loop_stop()
        self._client.disconnect()


# ---------------------------------------------------------------------------
# Main emit loop
# ---------------------------------------------------------------------------
def main():
    logger.info("=" * 70)
    logger.info("IoT Sensor Producer starting up")
    logger.info("PUBLISH_MODE=%s | NUM_DEVICES=%d | EMIT_INTERVAL=%.2fs | ANOMALY_PROB=%.2f",
                PUBLISH_MODE, NUM_DEVICES, EMIT_INTERVAL_SECONDS, ANOMALY_PROBABILITY)
    logger.info("=" * 70)

    fleet = build_device_fleet(NUM_DEVICES)

    publishers = []
    if PUBLISH_MODE in ("kafka", "both"):
        publishers.append(KafkaPublisher(KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC))
    if PUBLISH_MODE in ("mqtt", "both"):
        publishers.append(MqttPublisher(MQTT_BROKER_HOST, MQTT_BROKER_PORT, MQTT_TOPIC))

    if not publishers:
        logger.error("No valid PUBLISH_MODE configured (expected 'kafka', 'mqtt', or 'both'). Exiting.")
        sys.exit(1)

    total_emitted = 0
    total_anomalies = 0
    cycle = 0

    try:
        while not _shutdown_requested:
            cycle += 1
            # Force at least one guaranteed anomaly every ~20 cycles across
            # a random device, ensuring the downstream pipeline reliably
            # sees ANOMALY_DETECTED events even under low random probability.
            forced_device_idx = random.randrange(len(fleet)) if cycle % 20 == 0 else None

            for idx, device in enumerate(fleet):
                force = forced_device_idx is not None and idx == forced_device_idx
                reading = generate_reading(device, force_anomaly=force)

                for publisher in publishers:
                    publisher.publish(reading)

                total_emitted += 1
                if reading["simulated_anomaly"]:
                    total_anomalies += 1
                    logger.info(
                        "[ANOMALY] device=%s temp=%.2fC vibration=%.2fHz",
                        reading["device_id"], reading["temperature"], reading["vibration"],
                    )

            for publisher in publishers:
                publisher.flush()

            if cycle % 10 == 0:
                logger.info(
                    "Progress: cycle=%d total_readings=%d total_anomalies=%d",
                    cycle, total_emitted, total_anomalies,
                )

            time.sleep(EMIT_INTERVAL_SECONDS)

    finally:
        logger.info("Shutting down publishers. Total readings emitted: %d (anomalies: %d)",
                     total_emitted, total_anomalies)
        for publisher in publishers:
            try:
                publisher.close()
            except Exception:  # noqa: BLE001
                logger.exception("Error while closing publisher")

    logger.info("Producer stopped cleanly.")


if __name__ == "__main__":
    main()
