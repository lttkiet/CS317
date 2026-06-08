import sys
import logging
import tempfile
import os
from datetime import datetime, timezone, timedelta

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from training.constants import FRAUD_THRESHOLD

logger = logging.getLogger(__name__)

_model = None
_spark = None


def _compute_features(customer_id, terminal_id, tx_amount, tx_time_seconds, tx_time_days, tx_datetime: datetime):
    return [
        float(tx_amount),
        float(tx_time_seconds),
        float(tx_time_days),
        float(1 if tx_datetime.weekday() >= 5 else 0),
        float(1 if tx_datetime.hour <= 6 else 0),
        float(1),
        float(tx_amount),
        float(1),
        float(tx_amount),
        float(1),
        float(tx_amount),
        float(0),
        float(0.0),
        float(0),
        float(0.0),
        float(0),
        float(0.0),
    ]


def _load_model():
    global _model, _spark
    if _model is not None:
        return _model
    try:
        from pyspark.sql import SparkSession
        from pyspark.ml import PipelineModel
        import google.auth
        from google.cloud import storage

        logger.info("Loading Spark model from GCS for direct inference")
        _spark = SparkSession.builder \
            .appName("direct-inference") \
            .master("local[1]") \
            .config("spark.sql.shuffle.partitions", "1") \
            .getOrCreate()
        _spark.sparkContext.setLogLevel("ERROR")

        creds, project = google.auth.default()
        storage_client = storage.Client(project=project)
        bucket_name = f"{project}-fraud-model-registry"
        bucket = storage_client.bucket(bucket_name)
        blobs = list(bucket.list_blobs(prefix="latest_rf_model/"))

        tmpdir = tempfile.mkdtemp()
        model_local = os.path.join(tmpdir, "latest_rf_model")
        for blob in blobs:
            rel = blob.name[len("latest_rf_model/"):]
            if not rel or rel.endswith("/"):
                continue
            dest = os.path.join(model_local, rel)
            os.makedirs(os.path.dirname(dest), exist_ok=True)
            blob.download_to_filename(dest)

        _model = PipelineModel.load(model_local)
        logger.info("Model loaded for direct inference")
        return _model
    except Exception as e:
        logger.warning(f"Failed to load model for direct inference: {e}")
        return None


def predict(customer_id, terminal_id, tx_amount, tx_time_seconds, tx_time_days, tx_datetime: datetime):
    model = _load_model()
    if model is None:
        return None

    features = _compute_features(customer_id, terminal_id, tx_amount, tx_time_seconds, tx_time_days, tx_datetime)
    raw = _spark.createDataFrame([tuple(features)], [
        "tx_amount", "tx_time_seconds", "tx_time_days",
        "tx_during_weekend", "tx_during_night",
        "customer_id_nb_tx_1day_window", "customer_id_avg_amount_1day_window",
        "customer_id_nb_tx_7day_window", "customer_id_avg_amount_7day_window",
        "customer_id_nb_tx_30day_window", "customer_id_avg_amount_30day_window",
        "terminal_id_nb_tx_1day_window", "terminal_id_risk_1day_window",
        "terminal_id_nb_tx_7day_window", "terminal_id_risk_7day_window",
        "terminal_id_nb_tx_30day_window", "terminal_id_risk_30day_window",
    ])

    assembled = model.stages[0].transform(raw)
    result = model.stages[1].transform(assembled)
    row = result.select("probability", "prediction").collect()[0]
    prob = float(row["probability"][1])
    pred = int(prob >= FRAUD_THRESHOLD)
    return {"fraud_probability": round(prob, 6), "is_fraud_predicted": pred}
