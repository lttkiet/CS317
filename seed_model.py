"""Seed a baseline Random Forest model with synthetic data.

Run as a Dataproc Serverless batch to create the initial model
in GCS so the streaming job can start.
"""
import argparse
import logging
import os

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType, LongType, StructType, StructField,
)
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.functions import vector_to_array

from training.constants import (
    VECTOR_ASSEMBLER_INPUT_COLS,
    RF_PARAMS,
    LABEL_COL,
    FEATURES_COL,
    PROBABILITY_COL,
    PREDICTION_COL,
    RAW_PREDICTION_COL,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("seed_model")

NUM_ROWS = 50000
FRAUD_RATE = 0.34


def get_spark_session() -> SparkSession:
    builder = SparkSession.builder.appName("fraud-seed-model")
    builder = builder.config("viewsEnabled", "true").config("materializationDataset", "spark_temp")
    return builder.getOrCreate()


def generate_synthetic_data(spark: SparkSession, num_rows: int) -> DataFrame:
    logger.info(f"Generating {num_rows} synthetic rows...")
    df = spark.range(num_rows).select(
        F.rand(42).alias("_rand1"),
        F.rand(99).alias("_rand2"),
        F.rand(199).alias("_rand3"),
    )

    # Create label: fraud if specific pattern of random values
    df = df.withColumn(
        "tx_fraud",
        F.when(
            (F.col("_rand1") < FRAUD_RATE),
            F.lit(1)
        ).otherwise(F.lit(0))
    ).cache()

    features = df.select(
        F.col("tx_fraud"),
        # Raw transaction features — these carry the fraud signal
        (F.col("_rand1") * 1000 + 10).cast("double").alias("tx_amount"),
        (F.col("_rand2") * 86400).cast("long").alias("tx_time_seconds"),
        (F.col("_rand3") * 365).cast("long").alias("tx_time_days"),
        F.when(F.col("_rand1") > 0.5, 1).otherwise(0).cast("long").alias("tx_during_weekend"),
        F.when(F.col("_rand2") > 0.7, 1).otherwise(0).cast("long").alias("tx_during_night"),

        # Customer window features — match streaming pipeline defaults
        F.lit(1).cast("long").alias("customer_id_nb_tx_1day_window"),
        F.col("tx_amount").cast("double").alias("customer_id_avg_amount_1day_window"),
        F.lit(1).cast("long").alias("customer_id_nb_tx_7day_window"),
        F.col("tx_amount").cast("double").alias("customer_id_avg_amount_7day_window"),
        F.lit(1).cast("long").alias("customer_id_nb_tx_30day_window"),
        F.col("tx_amount").cast("double").alias("customer_id_avg_amount_30day_window"),

        # Terminal risk features — match streaming pipeline defaults (cold start)
        F.lit(0).cast("long").alias("terminal_id_nb_tx_1day_window"),
        F.lit(0.0).cast("double").alias("terminal_id_risk_1day_window"),
        F.lit(0).cast("long").alias("terminal_id_nb_tx_7day_window"),
        F.lit(0.0).cast("double").alias("terminal_id_risk_7day_window"),
        F.lit(0).cast("long").alias("terminal_id_nb_tx_30day_window"),
        F.lit(0.0).cast("double").alias("terminal_id_risk_30day_window"),
    )

    # Inject signal: fraud -> higher amounts, weekend preference
    features = features.withColumn(
        "tx_amount",
        F.when(F.col(LABEL_COL) == 1, F.col("tx_amount") * 3).otherwise(F.col("tx_amount"))
    )
    features = features.withColumn(
        "tx_during_weekend",
        F.when(F.col(LABEL_COL) == 1, F.lit(1)).otherwise(F.col("tx_during_weekend"))
    )

    # Drop helper columns
    features = features.drop("_rand1", "_rand2", "_rand3")
    return features


def build_pipeline() -> Pipeline:
    assembler = VectorAssembler(
        inputCols=VECTOR_ASSEMBLER_INPUT_COLS,
        outputCol=FEATURES_COL,
        handleInvalid="skip",
    )
    rf = RandomForestClassifier(
        featuresCol=FEATURES_COL,
        labelCol=LABEL_COL,
        predictionCol=PREDICTION_COL,
        probabilityCol=PROBABILITY_COL,
        rawPredictionCol=RAW_PREDICTION_COL,
        weightCol="classWeight",
        **RF_PARAMS,
    )
    return Pipeline(stages=[assembler, rf])


def compute_class_weights(df: DataFrame) -> DataFrame:
    counts = df.groupBy(F.col(LABEL_COL).alias("label")).count().collect()
    total = sum(r["count"] for r in counts)
    fraud_count = next((r["count"] for r in counts if r["label"] == 1), 0)
    legit_count = total - fraud_count
    weight_fraud = total / (2.0 * fraud_count) if fraud_count > 0 else 1.0
    weight_legit = total / (2.0 * legit_count) if legit_count > 0 else 1.0
    logger.info(f"Class weights: fraud={weight_fraud:.2f} legit={weight_legit:.2f}")
    return df.withColumn(
        "classWeight",
        F.when(F.col(LABEL_COL) == 1, F.lit(weight_fraud)).otherwise(F.lit(weight_legit)),
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--project_id", required=True)
    parser.add_argument("--model_path", required=True)
    args = parser.parse_args()

    spark = get_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    data = generate_synthetic_data(spark, NUM_ROWS)
    logger.info(f"Generated {data.count()} rows with {data.columns}")

    counts = data.groupBy(LABEL_COL).count().collect()
    for r in counts:
        logger.info(f"  {r[LABEL_COL]}: {r['count']}")

    data = compute_class_weights(data)

    pipeline = build_pipeline()
    logger.info("Training RF model...")
    model = pipeline.fit(data)

    # Evaluate on the same data (just to get metrics)
    preds = model.transform(data)
    preds = preds.withColumn(
        "fraud_prob",
        vector_to_array(F.col(PROBABILITY_COL)).getItem(1).cast(DoubleType()),
    )
    evaluator = BinaryClassificationEvaluator(
        labelCol=LABEL_COL, rawPredictionCol=RAW_PREDICTION_COL, metricName="areaUnderROC",
    )
    roc_auc = evaluator.evaluate(preds)
    evaluator.setMetricName("areaUnderPR")
    pr_auc = evaluator.evaluate(preds)
    logger.info(f"ROC-AUC: {roc_auc:.4f}  PR-AUC: {pr_auc:.4f}")

    logger.info(f"Saving model to {args.model_path}...")
    model.write().overwrite().save(args.model_path)
    logger.info("Seed model saved successfully!")

    spark.stop()


if __name__ == "__main__":
    main()
