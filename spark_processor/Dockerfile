# =====================================================================
# IoT Spark Structured Streaming Processor - Dockerfile
# =====================================================================
# Uses the official, freely available Apache Spark image (Docker Hub
# "apache/spark") instead of bitnami/spark, since Bitnami moved
# version-pinned tags behind their paid "Secure Images" catalog and
# 3.5.1 is no longer resolvable on the free tier.
# =====================================================================
FROM apache/spark:3.5.1

LABEL maintainer="iot-platform-team"
LABEL description="PySpark Structured Streaming processor: Kafka -> MinIO (S3A/boto3) + PostgreSQL"

USER root

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    SPARK_HOME=/opt/spark \
    PATH="/opt/spark/bin:${PATH}"

WORKDIR /app

# ---------------------------------------------------------------------
# OS packages needed for building Python wheels (psycopg2, etc.) and
# for downloading extra connector JARs.
# ---------------------------------------------------------------------
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        curl \
        gcc \
        libc6-dev \
        libpq-dev \
        python3-pip \
    && rm -rf /var/lib/apt/lists/*

# ---------------------------------------------------------------------
# Fetch JARs required for Kafka Structured Streaming, S3A (MinIO), and
# the PostgreSQL JDBC driver. Versions are pinned to match Spark 3.5.1
# and the Hadoop 3.3.x client bundled with the apache/spark:3.5.1 image.
# ---------------------------------------------------------------------
ENV SPARK_JARS_DIR=/opt/spark/jars

RUN curl -fsSL -o ${SPARK_JARS_DIR}/spark-sql-kafka-0-10_2.12-3.5.1.jar \
        https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar \
    && curl -fsSL -o ${SPARK_JARS_DIR}/spark-token-provider-kafka-0-10_2.12-3.5.1.jar \
        https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.1/spark-token-provider-kafka-0-10_2.12-3.5.1.jar \
    && curl -fsSL -o ${SPARK_JARS_DIR}/kafka-clients-3.4.1.jar \
        https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar \
    && curl -fsSL -o ${SPARK_JARS_DIR}/commons-pool2-2.11.1.jar \
        https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar \
    && curl -fsSL -o ${SPARK_JARS_DIR}/hadoop-aws-3.3.4.jar \
        https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
    && curl -fsSL -o ${SPARK_JARS_DIR}/aws-java-sdk-bundle-1.12.262.jar \
        https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
    && curl -fsSL -o ${SPARK_JARS_DIR}/postgresql-42.7.3.jar \
        https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar

# ---------------------------------------------------------------------
# Python dependencies
# ---------------------------------------------------------------------
COPY requirements.txt .
RUN pip3 install --no-cache-dir --break-system-packages -r requirements.txt || \
    pip3 install --no-cache-dir -r requirements.txt

# ---------------------------------------------------------------------
# Application code
# ---------------------------------------------------------------------
COPY stream_processor.py .

# The apache/spark image ships a built-in "spark" user (UID 185).
# Reuse it here rather than inventing a new UID.
RUN mkdir -p /tmp/spark-checkpoints \
    && chown -R spark:spark /app /tmp/spark-checkpoints

USER spark

HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 \
    CMD curl -f http://localhost:4040/ || exit 1

ENTRYPOINT ["/opt/spark/bin/spark-submit", \
    "--master", "local[*]", \
    "--jars", "/opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.1.jar,/opt/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.1.jar,/opt/spark/jars/kafka-clients-3.4.1.jar,/opt/spark/jars/commons-pool2-2.11.1.jar,/opt/spark/jars/hadoop-aws-3.3.4.jar,/opt/spark/jars/aws-java-sdk-bundle-1.12.262.jar,/opt/spark/jars/postgresql-42.7.3.jar", \
    "/app/stream_processor.py"]