"""Local service adapters — replaces GCP clients with local equivalents.

Used only in Docker local-dev mode. The production code uses real GCP clients.
"""
import os
import json
import psycopg2
import psycopg2.extras
from minio import Minio
from io import BytesIO


# ── PostgreSQL adapter (BigQuery substitute) ─────────────────────────
def get_postgres_conn():
    return psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=int(os.environ.get("POSTGRES_PORT", "5432")),
        dbname=os.environ.get("POSTGRES_DB", "fraud_detection"),
        user=os.environ.get("POSTGRES_USER", "fraud"),
        password=os.environ.get("POSTGRES_PASSWORD", "fraud_local_2024"),
    )


def bq_insert_rows(table_id: str, rows: list[dict]) -> int:
    """Insert rows into PostgreSQL table (mimics BigQuery insert_rows_json)."""
    if not rows:
        return 0

    conn = get_postgres_conn()
    cur = conn.cursor()

    # Extract table name from fully-qualified ID
    table_name = table_id.split(".")[-1]

    cols = list(rows[0].keys())
    placeholders = ", ".join(["%s"] * len(cols))
    col_names = ", ".join([f'"{c}"' for c in cols])
    sql = f'INSERT INTO {table_name} ({col_names}) VALUES ({placeholders})'

    values = []
    for row in rows:
        values.append(tuple(row[c] for c in cols))

    psycopg2.extras.execute_batch(cur, sql, values, page_size=500)
    conn.commit()
    cur.close()
    conn.close()
    return len(rows)


def bq_query(sql: str) -> list[dict]:
    """Run a query against PostgreSQL (mimics BigQuery query)."""
    conn = get_postgres_conn()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(sql)
    rows = [dict(r) for r in cur.fetchall()]
    cur.close()
    conn.close()
    return rows


# ── MinIO adapter (GCS substitute) ──────────────────────────────────
def get_minio_client():
    return Minio(
        os.environ.get("MINIO_ENDPOINT", "localhost:9000").replace("http://", "").replace("https://", ""),
        access_key=os.environ.get("MINIO_ACCESS_KEY", "minioadmin"),
        secret_key=os.environ.get("MINIO_SECRET_KEY", "minioadmin"),
        secure=False,
    )


def minio_ensure_bucket(bucket_name: str):
    client = get_minio_client()
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)


def minio_put_object(bucket_name: str, object_name: str, data: bytes, content_type: str = "application/octet-stream"):
    client = get_minio_client()
    client.put_object(
        bucket_name,
        object_name,
        BytesIO(data),
        len(data),
        content_type=content_type,
    )


def minio_get_object(bucket_name: str, object_name: str) -> bytes:
    client = get_minio_client()
    response = client.get_object(bucket_name, object_name)
    data = response.read()
    response.close()
    response.release_conn()
    return data


def minio_list_objects(bucket_name: str, prefix: str = "") -> list[str]:
    client = get_minio_client()
    return [obj.object_name for obj in client.list_objects(bucket_name, prefix=prefix, recursive=True)]


def minio_object_exists(bucket_name: str, object_name: str) -> bool:
    client = get_minio_client()
    try:
        client.stat_object(bucket_name, object_name)
        return True
    except Exception:
        return False
