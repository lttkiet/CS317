#!/usr/bin/env python3
"""Init container — creates Kafka topic, PostgreSQL tables, and MinIO buckets.

Runs once at startup before any application service.
"""
import os
import sys
import time
import json
import psycopg2
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError, NoBrokersAvailable
from minio import Minio

KAFKA_BOOTSTRAP = os.environ["KAFKA_BOOTSTRAP"]
KAFKA_RETENTION_MS = os.environ.get("KAFKA_RETENTION_MS", "604800000")
POSTGRES_HOST = os.environ["POSTGRES_HOST"]
POSTGRES_DB = os.environ["POSTGRES_DB"]
POSTGRES_USER = os.environ["POSTGRES_USER"]
POSTGRES_PASSWORD = os.environ["POSTGRES_PASSWORD"]
MINIO_ENDPOINT = os.environ["MINIO_ENDPOINT"].replace("http://", "").replace("https://", "")
MINIO_ACCESS_KEY = os.environ["MINIO_ACCESS_KEY"]
MINIO_SECRET_KEY = os.environ["MINIO_SECRET_KEY"]

EXPECTED_PARTITIONS = 10


def wait_for_kafka(bootstrap: str, timeout: int = 60):
    print(f"[init] Waiting for Kafka at {bootstrap}...")
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            admin = KafkaAdminClient(bootstrap_servers=bootstrap, request_timeout_ms=5000)
            admin.list_topics()
            admin.close()
            print("[init] Kafka is ready")
            return
        except Exception:
            time.sleep(2)
    print("[init] ERROR: Kafka not ready within timeout")
    sys.exit(1)


def create_kafka_topic(bootstrap: str, topic: str, partitions: int = 10, rf: int = 1):
    print(f"[init] Creating Kafka topic: {topic}")
    admin = KafkaAdminClient(bootstrap_servers=bootstrap)
    try:
        new_topics = [
            NewTopic(
                name=topic,
                num_partitions=partitions,
                replication_factor=rf,
                topic_configs={
                    "retention.ms": KAFKA_RETENTION_MS,
                    "cleanup.policy": "delete",
                    "min.insync.replicas": "1",
                },
            )
        ]
        admin.create_topics(new_topics)
        print(f"[init] Topic '{topic}' created ({partitions} partitions)")
    except TopicAlreadyExistsError:
        existing = admin.describe_topics([topic])
        actual = len(existing[topic].partitions)
        if actual != EXPECTED_PARTITIONS:
            print(f"[init] WARNING: Topic '{topic}' has {actual} partitions, expected {EXPECTED_PARTITIONS}")
        else:
            print(f"[init] Topic '{topic}' already exists with {actual} partitions")
    finally:
        admin.close()


def init_postgres():
    print("[init] Initializing PostgreSQL schema...")
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )
    conn.autocommit = True
    cur = conn.cursor()

    with open("/app/01_schema.sql", "r") as f:
        sql = f.read()
    cur.execute(sql)
    print("[init] PostgreSQL schema created")

    cur.close()
    conn.close()


def init_minio():
    print("[init] Creating MinIO buckets...")
    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False,
    )
    for bucket in ["fraud-models", "fraud-staging", "fraud-checkpoints", "fraud-pipeline-root"]:
        if not client.bucket_exists(bucket):
            client.make_bucket(bucket)
            print(f"[init] Bucket '{bucket}' created")
        else:
            print(f"[init] Bucket '{bucket}' exists")


def main():
    print("=" * 60)
    print("  Fraud Detection Pipeline — Local Init")
    print("=" * 60)

    wait_for_kafka(KAFKA_BOOTSTRAP)
    create_kafka_topic(KAFKA_BOOTSTRAP, "transactions_topic", partitions=10)
    init_postgres()
    init_minio()

    print("=" * 60)
    print("  Init complete — all services ready")
    print("=" * 60)


if __name__ == "__main__":
    main()
