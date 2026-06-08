FROM python:3.11-slim AS base

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    default-jdk-headless \
    curl \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Download PostgreSQL JDBC driver (for Spark JDBC reads/writes)
RUN mkdir -p /app/jars && \
    curl -sL -o /app/jars/postgresql-42.7.4.jar \
    "https://jdbc.postgresql.org/download/postgresql-42.7.4.jar"

# Download Spark Kafka connector (for Structured Streaming with Kafka)
RUN     curl -sL -o /app/jars/spark-sql-kafka-0-10_2.13-3.5.5.jar \
    "https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.13/3.5.5/spark-sql-kafka-0-10_2.13-3.5.5.jar" && \
    curl -sL -o /app/jars/spark-token-provider-kafka-0-10_2.13-3.5.5.jar \
    "https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.13/3.5.5/spark-token-provider-kafka-0-10_2.13-3.5.5.jar" && \
    curl -sL -o /app/jars/kafka-clients-3.7.1.jar \
    "https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.7.1/kafka-clients-3.7.1.jar" && \
    curl -sL -o /app/jars/commons-pool2-2.11.1.jar \
    "https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar"

COPY training/requirements.txt /app/training-requirements.txt
COPY streaming/requirements.txt /app/streaming-requirements.txt
COPY simulator/requirements.txt /app/simulator-requirements.txt
RUN cat /app/training-requirements.txt /app/streaming-requirements.txt /app/simulator-requirements.txt | sort -u > /app/requirements.txt && \
    rm /app/training-requirements.txt /app/streaming-requirements.txt /app/simulator-requirements.txt && \
    pip install --no-cache-dir \
    -r requirements.txt \
    psycopg2-binary boto3 kafka-python-ng minio

COPY simulator/ /app/simulator/
COPY training/ /app/training/
COPY streaming/ /app/streaming/
COPY docker/local_config.py /app/local_config.py

ENV PYTHONPATH=/app

# ── Simulator target ─────────────────────────────────────────────────
FROM base AS simulator
ENTRYPOINT ["python", "-m", "simulator.main"]

# ── Training target ──────────────────────────────────────────────────
FROM base AS training
ENTRYPOINT ["python", "-m", "training.train"]

# ── Streaming target ─────────────────────────────────────────────────
FROM base AS streaming
ENTRYPOINT ["python", "-m", "streaming.inference"]
