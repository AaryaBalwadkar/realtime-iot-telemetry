# =====================================================================
# IoT Sensor Producer - Dockerfile
# =====================================================================
FROM python:3.11-slim

LABEL maintainer="iot-platform-team"
LABEL description="Simulated IoT edge device telemetry producer (Kafka/MQTT)"

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# Install OS-level build deps required by some Python wheels, then clean up
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        gcc \
        libc6-dev \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY producer.py .

# Run as a non-root user for security
RUN useradd --create-home --shell /bin/bash iotuser \
    && chown -R iotuser:iotuser /app
USER iotuser

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD python -c "import sys; sys.exit(0)" || exit 1

ENTRYPOINT ["python", "producer.py"]
