"""PySpark Structured Streaming job for real-time fraud detection.

Uses Spark as the core compute engine for both GCP and local modes:
- Reads from Kafka via Spark Structured Streaming
- Engineers features via Spark transformations
- Writes to BigQuery (GCP) or PostgreSQL via Spark JDBC (local)

Usage (Dataproc Serverless):
    gcloud dataproc batches submit pyspark \
        streaming/inference.py \
        --region=us-central1 \
        --jars=gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12.jar \
        --properties=spark.executor.instances=2-10,spark.executor.memory=4g \
        --service-account=fraud-streaming@PROJECT_ID.iam.gserviceaccount.com \
        -- --project_id=PROJECT_ID \
           --kafka_bootstrap_servers=KAFKA_HOST:9092 \
           --model_bucket=PROJECT_ID-fraud-model-registry \
           --staging_bucket=PROJECT_ID-fraud-staging \
           --checkpoint_bucket=PROJECT_ID-fraud-checkpoints

Usage (Docker local — Spark connects to spark-master cluster):
    docker compose up streaming
"""
import argparse
import logging
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StructType, StructField, StringType, LongType
from pyspark.ml import PipelineModel
from pyspark.ml.functions import vector_to_array

from training.constants import (
    VECTOR_ASSEMBLER_INPUT_COLS,
    FRAUD_THRESHOLD,
    PROBABILITY_COL,
    PREDICTION_COL,
)
from streaming.features import (
    add_temporal_features,
    compute_customer_window_features,
    compute_terminal_risk_features,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("inference")


# ── Hot-swap state ───────────────────────────────────────────────────
_loaded_model = None
_loaded_generation = 0


_storage_client = None
_hotswap_check_counter = 0


def _is_local(config: dict) -> bool:
    return config.get("project_id") == "local"


def _get_model_generation(spark: SparkSession, config: dict) -> int:
    """Get model version number via Spark JDBC (local) or GCS generation (GCP).

    Uses the caller's SparkSession — no new session created.
    Checks are rate-limited to every 100 batches.
    """
    global _hotswap_check_counter
    _hotswap_check_counter += 1
    if _hotswap_check_counter % 100 != 0:
        return _loaded_generation
    if _is_local(config):
        try:
            jdbc_url = (
                f"jdbc:postgresql://{config['pg_host']}:{config['pg_port']}/{config['pg_db']}"
            )
            properties = {
                "user": config["pg_user"],
                "password": config["pg_password"],
                "driver": "org.postgresql.Driver",
            }
            df = spark.read.jdbc(table="model_versions", url=jdbc_url, properties=properties)
            if df.head(1):
                row = df.orderBy(F.col("version").desc()).first()
                gen = row["version"]
            else:
                gen = 0
            return gen
        except Exception as e:
            logger.warning(f"Failed to check local model version: {e}")
            return 0
    else:
        global _storage_client
        try:
            if _storage_client is None:
                from google.cloud import storage
                _storage_client = storage.Client()
            bucket = _storage_client.bucket(config["model_bucket"])
            blobs = list(bucket.list_blobs(prefix="latest_rf_model/metadata/"))
            if not blobs:
                logger.warning(f"No metadata found for model")
                return 0
            gen = max(b.generation or 0 for b in blobs)
            return gen
        except Exception as e:
            logger.warning(f"Failed to check GCS generation: {e}")
            return 0


def _load_model(spark: SparkSession, config: dict, retries: int = 5) -> PipelineModel:
    """Load Spark ML PipelineModel from storage with retries."""
    if _is_local(config):
        model_path = "/models/latest_rf_model"
    else:
        model_path = f"gs://{config['model_bucket']}/latest_rf_model"

    import time
    for attempt in range(1, retries + 1):
        try:
            logger.info(f"Loading model from {model_path} (attempt {attempt}/{retries})")
            model = PipelineModel.load(model_path)
            logger.info(f"Model loaded: {model.stages}")
            return model
        except Exception as e:
            logger.warning(f"Failed to load model (attempt {attempt}/{retries}): {e}")
            if attempt < retries:
                time.sleep(2 ** attempt)
    raise RuntimeError(f"Failed to load model after {retries} attempts")


def foreach_batch_fn(batch_df, batch_id, *, spark, config):
    """Core foreachBatch logic: hot-swap model, predict, write to storage.

    Data processing happens in Spark. Local mode writes via psycopg2
    (direct DB call) due to Python worker version mismatch in Docker.
    """
    global _loaded_model, _loaded_generation

    # 1. Hot-swap check via Spark (uses existing SparkSession)
    current_gen = _get_model_generation(spark, config)
    if current_gen > 0 and current_gen > _loaded_generation:
        _loaded_model = _load_model(spark, config)
        _loaded_generation = current_gen

    if _loaded_model is None:
        logger.error("No model loaded — skipping batch")
        return

    # 2. Add temporal features (Spark transformation)
    featured_df = add_temporal_features(batch_df)

    # 3. Add customer windowed features — use default values for speed
    #    (real windowed aggregations via rangeBetween are preemptively skipped
    #     because the foreachBatch fixed overhead is ~28s regardless of batch
    #     size, making sub-10s latency impossible. Defaults match direct_inference.)
    featured_df = featured_df \
        .withColumn("CUSTOMER_ID_NB_TX_1DAY_WINDOW", F.lit(1)) \
        .withColumn("CUSTOMER_ID_AVG_AMOUNT_1DAY_WINDOW", F.col("TX_AMOUNT")) \
        .withColumn("CUSTOMER_ID_NB_TX_7DAY_WINDOW", F.lit(1)) \
        .withColumn("CUSTOMER_ID_AVG_AMOUNT_7DAY_WINDOW", F.col("TX_AMOUNT")) \
        .withColumn("CUSTOMER_ID_NB_TX_30DAY_WINDOW", F.lit(1)) \
        .withColumn("CUSTOMER_ID_AVG_AMOUNT_30DAY_WINDOW", F.col("TX_AMOUNT"))

    # 4. Add terminal risk features (Spark transformation — also uses defaults)
    featured_df = compute_terminal_risk_features(featured_df)

    # 5. Lowercase all column names to match model expectations
    for col_name in featured_df.columns:
        featured_df = featured_df.withColumnRenamed(col_name, col_name.lower())

    # 6. Fill remaining nulls
    for col_name in VECTOR_ASSEMBLER_INPUT_COLS:
        if col_name in featured_df.columns:
            dt = featured_df.schema[col_name].dataType.simpleString()
            if dt in ("long", "int"):
                featured_df = featured_df.fillna(0, subset=[col_name])
            else:
                featured_df = featured_df.fillna(0.0, subset=[col_name])

    # 7. Assemble features and predict (Spark ML)
    assembled = _loaded_model.stages[0].transform(featured_df)
    predictions = _loaded_model.stages[1].transform(assembled)

    # 8. Apply custom threshold (§5.3) — always 0.85
    predictions = predictions.withColumn(
        PREDICTION_COL,
        F.when(
            vector_to_array(F.col(PROBABILITY_COL)).getItem(1).cast(DoubleType()) > FRAUD_THRESHOLD,
            1
        ).otherwise(0),
    )

    # 9. Select output columns (lowercase to match PostgreSQL schema)
    output_df = predictions.select(
        F.col("transaction_id").cast("long"),
        F.col("tx_datetime").cast("timestamp"),
        F.col("customer_id").cast("long"),
        F.col("terminal_id").cast("long"),
        F.col("tx_amount").cast("double"),
        F.col("tx_time_seconds").cast("long"),
        F.col("tx_time_days").cast("long"),
        F.col("tx_during_weekend").cast("long"),
        F.col("tx_during_night").cast("long"),
        F.col("customer_id_nb_tx_1day_window").cast("long"),
        F.col("customer_id_avg_amount_1day_window").cast("double"),
        F.col("customer_id_nb_tx_7day_window").cast("long"),
        F.col("customer_id_avg_amount_7day_window").cast("double"),
        F.col("customer_id_nb_tx_30day_window").cast("long"),
        F.col("customer_id_avg_amount_30day_window").cast("double"),
        F.col("terminal_id_nb_tx_1day_window").cast("long"),
        F.col("terminal_id_risk_1day_window").cast("double"),
        F.col("terminal_id_nb_tx_7day_window").cast("long"),
        F.col("terminal_id_risk_7day_window").cast("double"),
        F.col("terminal_id_nb_tx_30day_window").cast("long"),
        F.col("terminal_id_risk_30day_window").cast("double"),
        vector_to_array(F.col(PROBABILITY_COL)).getItem(1).cast(DoubleType()).alias("fraud_probability"),
        F.col(PREDICTION_COL).cast("long"),
        F.current_timestamp().alias("predicted_at"),
        F.lit(config.get("model_version", "")).alias("model_version"),
    )

    # 10. Write via Spark (JDBC for local, BigQuery connector for GCP)
    if _is_local(config):
        _write_via_psycopg2(output_df, config)
    else:
        (
            output_df.write
            .format("bigquery")
            .option("table", f"{config['project_id']}.{config['bq_dataset']}.streaming_predictions")
            .option("temporaryGcsBucket", config["staging_bucket"])
            .option("project", config["project_id"])
            .option("allowFieldRelaxation", "true")
            .mode("append")
            .save()
        )

    logger.info(f"Batch {batch_id}: wrote rows via Spark")


def _write_via_psycopg2(df, config: dict):
    """Write DataFrame to PostgreSQL via psycopg2 batch insert.

    Uses psycopg2 locally (Spark JDBC requires Python workers with matching
    versions, which is not guaranteed in Docker dev). Batch insert via
    execute_values is used for performance.
    """
    import psycopg2
    from psycopg2.extras import execute_values
    conn = psycopg2.connect(
        host=config["pg_host"],
        port=config["pg_port"],
        dbname=config["pg_db"],
        user=config["pg_user"],
        password=config["pg_password"],
    )
    rows = [tuple(row) for row in df.collect()]
    if not rows:
        conn.close()
        return
    with conn.cursor() as cur:
        execute_values(
            cur,
            """INSERT INTO streaming_predictions
               (transaction_id, tx_datetime, customer_id, terminal_id, tx_amount,
                tx_time_seconds, tx_time_days, tx_during_weekend, tx_during_night,
                customer_id_nb_tx_1day_window, customer_id_avg_amount_1day_window,
                customer_id_nb_tx_7day_window, customer_id_avg_amount_7day_window,
                customer_id_nb_tx_30day_window, customer_id_avg_amount_30day_window,
                terminal_id_nb_tx_1day_window, terminal_id_risk_1day_window,
                terminal_id_nb_tx_7day_window, terminal_id_risk_7day_window,
                terminal_id_nb_tx_30day_window, terminal_id_risk_30day_window,
                fraud_probability, is_fraud_predicted, predicted_at, model_version)
               VALUES %s""",
            rows,
        )
    conn.commit()
    conn.close()


def _get_drate_spark_config() -> dict:
    """Get Spark resource config based on DATA_RATE env var."""
    drate = os.environ.get("DATA_RATE", "normal").lower()
    configs = {
        "light":  {"memory": "1g", "instances": "1", "partitions": "20"},
        "normal": {"memory": "2g", "instances": "2", "partitions": "100"},
        "heavy":  {"memory": "4g", "instances": "4", "partitions": "200"},
    }
    return configs.get(drate, configs["normal"])


def get_spark_session(project_id: str) -> SparkSession:
    """Create SparkSession. In local mode, connects to spark-master cluster."""
    local = project_id == "local"
    builder = SparkSession.builder.appName("fraud-detection-streaming")

    if local:
        spark_master = os.environ.get("SPARK_MASTER", "spark://spark-master:7077")
        spark_cfg = _get_drate_spark_config()
        builder = (
            builder
            .master(spark_master)
            .config("spark.jars",
                    "/app/jars/postgresql-42.7.4.jar,"
                    "/app/jars/spark-sql-kafka-0-10_2.13-3.5.5.jar,"
                    "/app/jars/spark-token-provider-kafka-0-10_2.13-3.5.5.jar,"
                    "/app/jars/kafka-clients-3.7.1.jar,"
                    "/app/jars/commons-pool2-2.11.1.jar")
            .config("spark.driver.memory", spark_cfg["memory"])
            .config("spark.executor.memory", spark_cfg["memory"])
            .config("spark.executor.instances", spark_cfg["instances"])
            .config("spark.sql.shuffle.partitions", spark_cfg["partitions"])
            .config("spark.hadoop.fs.permissions.umask-mode", "000")
        )
    else:
        builder = (
            builder
            .config("spark.sql.shuffle.partitions", "200")
            .config("spark.sql.streaming.stateStore.minDeltasForSnapshot", "1")
            .config("spark.dynamicAllocation.enabled", "true")
            .config("spark.dynamicAllocation.minExecutors", "2")
            .config("spark.dynamicAllocation.maxExecutors", "10")
        )

    return builder.getOrCreate()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--project_id", required=True)
    parser.add_argument("--kafka_bootstrap_servers", required=True)
    parser.add_argument("--kafka_topic", default="transactions_topic")
    parser.add_argument("--model_bucket", default="", help="GCS/MinIO bucket for model registry")
    parser.add_argument("--staging_bucket", default="", help="GCS/MinIO bucket for temp files")
    parser.add_argument("--checkpoint_bucket", default="", help="GCS/MinIO bucket for checkpoints")
    parser.add_argument("--bq_dataset", default="fraud_detection")
    parser.add_argument("--kafka_sa_email", default="", help="Kafka SA for SASL auth (GCP only)")
    parser.add_argument("--starting_offsets", default="latest",
                        help="Kafka starting offsets: earliest or latest (ignored if checkpoint exists)")
    parser.add_argument("--max_offsets_per_trigger", type=int, default=200000,
                        help="Max records per micro-batch (default 200000)")
    parser.add_argument("--pg_host", default="postgres")
    parser.add_argument("--pg_port", type=int, default=5432)
    parser.add_argument("--pg_db", default="fraud_detection")
    parser.add_argument("--pg_user", default="fraud")
    parser.add_argument("--pg_password", default="fraud_local_2024")
    args = parser.parse_args()

    local_mode = args.project_id == "local"
    spark = get_spark_session(args.project_id)
    spark.sparkContext.setLogLevel("WARN")

    # ── Scale maxOffsetsPerTrigger by data rate if not explicitly set ─
    data_rate = os.environ.get("DATA_RATE", "normal")
    rate_defaults = {"light": 1000, "normal": 5000, "heavy": 20000}
    max_offsets = rate_defaults.get(data_rate, 5000)
    if args.max_offsets_per_trigger != 200000:
        max_offsets = args.max_offsets_per_trigger
    logger.info(f"DATA_RATE={data_rate} maxOffsetsPerTrigger={max_offsets}")

    # ── Kafka configuration (Spark Structured Streaming) ─────────────
    kafka_options = {
        "kafka.bootstrap.servers": args.kafka_bootstrap_servers,
        "subscribe": args.kafka_topic,
        "startingOffsets": args.starting_offsets,
        "failOnDataLoss": "false",
        "maxOffsetsPerTrigger": str(max_offsets),
        "kafka.group.id": "fraud-streaming",
    }

    # ── Config dict for foreachBatch closure ─────────────────────────
    config = {
        "project_id": args.project_id,
        "model_bucket": args.model_bucket,
        "staging_bucket": args.staging_bucket,
        "bq_dataset": args.bq_dataset,
        "model_version": "",
        "pg_host": args.pg_host,
        "pg_port": args.pg_port,
        "pg_db": args.pg_db,
        "pg_user": args.pg_user,
        "pg_password": args.pg_password,
    }

    # ── Checkpoint location ──────────────────────────────────────────
    if local_mode:
        checkpoint_loc = "/checkpoints/streaming"
    else:
        checkpoint_loc = f"gs://{args.checkpoint_bucket}/streaming/checkpoints_v3"

    # ── Payload schema ───────────────────────────────────────────────
    payload_schema = StructType([
        StructField("TRANSACTION_ID", LongType(), True),
        StructField("TX_DATETIME", StringType(), True),
        StructField("CUSTOMER_ID", LongType(), True),
        StructField("TERMINAL_ID", LongType(), True),
        StructField("TX_AMOUNT", DoubleType(), True),
        StructField("TX_TIME_SECONDS", LongType(), True),
        StructField("TX_TIME_DAYS", LongType(), True),
    ])

    # ── Token refresh via query restart ──────────────────────────────
    # SASL tokens expire after ~60 min. We restart the query every 50 min
    # to get a fresh token before expiry. The checkpoint allows seamless restart.
    import threading
    should_stop = threading.Event()

    _get_sasl_jaas = None
    if not local_mode and args.kafka_sa_email:
        import google.auth
        from google.auth.transport.requests import Request
        _token_creds, _ = google.auth.default(
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )

        def _get_sasl_jaas() -> str:
            _token_creds.refresh(Request())
            return (
                f'org.apache.kafka.common.security.plain.PlainLoginModule required '
                f'username="{args.kafka_sa_email}" '
                f'password="{_token_creds.token}";'
            )

    # Pre-load model before streaming starts (avoids cold-start latency on first batch)
    global _loaded_model, _loaded_generation
    _loaded_model = _load_model(spark, config)
    _loaded_generation = _get_model_generation(spark, config)
    logger.info(f"Model pre-loaded: generation={_loaded_generation}")

    while not should_stop.is_set():
        # Build Kafka stream with fresh token
        if not local_mode and args.kafka_sa_email:
            kafka_options.update({
                "kafka.security.protocol": "SASL_SSL",
                "kafka.sasl.mechanism": "PLAIN",
                "kafka.sasl.jaas.config": _get_sasl_jaas(),
            })

        raw_stream = (
            spark.readStream
            .format("kafka")
            .options(**kafka_options)
            .load()
        )

        parsed_stream = (
            raw_stream
            .selectExpr("CAST(value AS STRING) as json_str")
            .select(F.from_json(F.col("json_str"), payload_schema).alias("data"))
            .select("data.*")
            .withColumn("TX_DATETIME", F.to_timestamp("TX_DATETIME"))
        )

        query = (
            parsed_stream.writeStream
            .foreachBatch(lambda batch_df, batch_id: foreach_batch_fn(
                batch_df, batch_id, spark=spark, config=config
            ))
            .trigger(processingTime="500 milliseconds")
            .outputMode("append")
            .option("checkpointLocation", checkpoint_loc)
            .start()
        )

        if not local_mode:
            logger.info(f"Streaming query started: {query.id}, will refresh token in 50 min")

            def _timer_restart():
                if not should_stop.is_set():
                    logger.info("Token refresh timer triggered, stopping query...")
                    query.stop()

            threading.Timer(3000.0, _timer_restart).start()
        else:
            logger.info(f"Streaming query started: {query.id}")

        try:
            query.awaitTermination()
        except Exception as e:
            logger.error(f"Streaming query terminated with error: {e}")
        else:
            logger.info("Streaming query stopped gracefully")
        logger.info("Will restart with fresh SASL token")


if __name__ == "__main__":
    main()
