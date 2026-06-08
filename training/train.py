"""Training job for fraud detection Random Forest model.

Uses Spark as the core compute engine for both GCP and local modes:
- GCP: Spark reads from BigQuery via spark-bigquery connector
- Local: Spark reads from PostgreSQL via JDBC

What's new (§3.4):
1. Cross-validation with ParamGridBuilder for hyperparameter tuning
2. Threshold optimization via Youden index / F1 sweep
3. Feature importance persistence (JSON in model_versions table)

Usage (Dataproc Serverless):
    gcloud dataproc batches submit pyspark \
        training/train.py \
        --region=us-central1 \
        --jars=gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12.jar \
        --properties=spark.executor.instances=4,spark.executor.memory=8g \
        --service-account=fraud-training@PROJECT_ID.iam.gserviceaccount.com \
        -- --project_id=PROJECT_ID --model_path=gs://PROJECT_ID-fraud-model-registry/latest_rf_model

Usage (Docker local — Spark connects to spark-master cluster):
    docker compose run --rm training
"""
import argparse
import json
import logging
import os
from datetime import datetime, timezone

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.ml.functions import vector_to_array

from training.constants import (
    VECTOR_ASSEMBLER_INPUT_COLS,
    LABEL_COL,
    FEATURES_COL,
    PROBABILITY_COL,
    PREDICTION_COL,
    RAW_PREDICTION_COL,
    FRAUD_THRESHOLD,
    EVAL_GATE_MARGIN,
    CV_NUM_FOLDS,
    CV_PARALLELISM,
    CV_SEED,
    CV_METRIC,
    CV_GRID_NUM_TREES,
    CV_GRID_MAX_DEPTH,
    CV_GRID_IMPURITY,
    CV_REDUCED_NUM_TREES,
    CV_REDUCED_MAX_DEPTH,
    CV_REDUCED_IMPURITY,
    THRESHOLD_TUNE_MIN,
    THRESHOLD_TUNE_MAX,
    THRESHOLD_TUNE_STEP,
    THRESHOLD_TUNE_METRIC,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("training")


def _is_local(project_id: str) -> bool:
    return project_id == "local"


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
    local = _is_local(project_id)
    builder = SparkSession.builder.appName("fraud-model-training")

    if local:
        spark_master = os.environ.get("SPARK_MASTER", "spark://spark-master:7077")
        spark_cfg = _get_drate_spark_config()
        builder = (
            builder
            .master(spark_master)
            .config("spark.jars", "/app/jars/postgresql-42.7.4.jar")
            .config("spark.driver.memory", spark_cfg["memory"])
            .config("spark.executor.memory", spark_cfg["memory"])
            .config("spark.executor.instances", spark_cfg["instances"])
            .config("spark.sql.shuffle.partitions", spark_cfg["partitions"])
            .config("spark.hadoop.fs.permissions.umask-mode", "000")
        )
    else:
        builder = (
            builder
            .config("viewsEnabled", "true")
            .config("materializationDataset", "spark_temp")
        )

    return builder.getOrCreate()


def load_training_data(spark: SparkSession, project_id: str) -> "DataFrame":
    """Load training data via Spark — JDBC for local, BigQuery connector for GCP."""
    if _is_local(project_id):
        return _load_from_postgres_jdbc(spark)
    else:
        return _load_from_bigquery(spark, project_id)


def _load_from_postgres_jdbc(spark: SparkSession) -> "DataFrame":
    """Load training data from PostgreSQL via Spark JDBC (keeps Spark as core engine)."""
    host = os.environ.get("POSTGRES_HOST", "postgres")
    port = os.environ.get("POSTGRES_PORT", "5432")
    db = os.environ.get("POSTGRES_DB", "fraud_detection")
    user = os.environ.get("POSTGRES_USER", "fraud")
    password = os.environ.get("POSTGRES_PASSWORD", "fraud_local_2024")

    jdbc_url = f"jdbc:postgresql://{host}:{port}/{db}"
    properties = {
        "user": user,
        "password": password,
        "driver": "org.postgresql.Driver",
    }

    logger.info(f"Loading training data from PostgreSQL via Spark JDBC: {jdbc_url}")

    df = (
        spark.read
        .jdbc(
            table="mlops_training_view",
            url=jdbc_url,
            properties=properties,
        )
    )

    df = df.filter(
        F.col("predicted_at") >= F.current_timestamp() - F.expr("INTERVAL 90 DAYS")
    ).filter(F.col("tx_fraud").isNotNull())

    return df


def _load_from_bigquery(spark: SparkSession, project_id: str) -> "DataFrame":
    """Load training data from BigQuery via spark-bigquery connector."""
    query = f"""
        SELECT * FROM `{project_id}.fraud_detection.mlops_training_view`
        WHERE PREDICTED_AT >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 DAY)
          AND TX_FRAUD IS NOT NULL
    """
    df = (
        spark.read
        .format("bigquery")
        .option("query", query)
        .option("project", project_id)
        .load()
    )
    for col_name in df.columns:
        df = df.withColumnRenamed(col_name, col_name.lower())
    return df


def compute_class_weights(df: "DataFrame") -> "DataFrame":
    """Compute class weight column for imbalanced fraud detection (§3.4)."""
    counts = df.groupBy(F.col(LABEL_COL).alias("label")).count().collect()
    total = sum(r["count"] for r in counts)
    fraud_count = next((r["count"] for r in counts if r["label"] == 1), 0)
    legit_count = total - fraud_count

    weight_fraud = total / (2.0 * fraud_count) if fraud_count > 0 else 1.0
    weight_legit = total / (2.0 * legit_count) if legit_count > 0 else 1.0

    logger.info(
        f"Class distribution: total={total} fraud={fraud_count} legit={legit_count} "
        f"weight_fraud={weight_fraud:.2f} weight_legit={weight_legit:.2f}"
    )

    return df.withColumn(
        "classWeight",
        F.when(F.col(LABEL_COL) == 1, F.lit(weight_fraud)).otherwise(F.lit(weight_legit)),
    )


def split_train_test(df: "DataFrame") -> tuple:
    """Time-based 80/20 split — no random shuffle (§3.4)."""
    split_idx = int(df.count() * 0.8)
    ordered = df.orderBy("predicted_at")
    train = ordered.limit(split_idx)
    test = ordered.subtract(train)
    return train, test


def build_tunable_pipeline() -> Pipeline:
    """Build Spark ML Pipeline with tunable RandomForest for CV."""
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
        numTrees=100,
        maxDepth=10,
        impurity="gini",
    )
    return Pipeline(stages=[assembler, rf])


def build_param_grid(project_id: str, pipeline: Pipeline) -> list:
    """Build grid for cross-validation. Local uses reduced grid for speed."""
    rf = pipeline.stages[-1]
    if _is_local(project_id):
        return (
            ParamGridBuilder()
            .addGrid(rf.numTrees, CV_REDUCED_NUM_TREES)
            .addGrid(rf.maxDepth, CV_REDUCED_MAX_DEPTH)
            .addGrid(rf.impurity, CV_REDUCED_IMPURITY)
            .build()
        )
    else:
        return (
            ParamGridBuilder()
            .addGrid(rf.numTrees, CV_GRID_NUM_TREES)
            .addGrid(rf.maxDepth, CV_GRID_MAX_DEPTH)
            .addGrid(rf.impurity, CV_GRID_IMPURITY)
            .build()
        )


def run_cross_validation(train_df: "DataFrame", pipeline: Pipeline, param_grid: list) -> "PipelineModel":
    """Run k-fold CV with PR-AUC metric. Returns best PipelineModel."""
    evaluator = BinaryClassificationEvaluator(
        labelCol=LABEL_COL,
        rawPredictionCol=RAW_PREDICTION_COL,
        metricName=CV_METRIC,
    )
    cv = CrossValidator(
        estimator=pipeline,
        estimatorParamMaps=param_grid,
        evaluator=evaluator,
        numFolds=CV_NUM_FOLDS,
        parallelism=CV_PARALLELISM,
        seed=CV_SEED,
        collectSubModels=False,
    )
    logger.info(
        f"Running {CV_NUM_FOLDS}-fold CV with {len(param_grid)} param combos"
        f" ({len(param_grid) * CV_NUM_FOLDS} total fits)..."
    )
    cv_model = cv.fit(train_df)
    best_params = cv_model.bestModel.stages[-1].extractParamMap()
    best_avg_metric = max(cv_model.avgMetrics)
    logger.info(f"Best CV params ({CV_METRIC}={best_avg_metric:.6f}):")
    for p, v in best_params.items():
        logger.info(f"  {p.name}: {v}")
    return cv_model.bestModel


def evaluate_model(model, test_df, threshold: float) -> dict:
    """Evaluate model on test set at a given threshold. Returns metrics dict."""
    predictions = model.transform(test_df)
    predictions = predictions.withColumn(
        "fraud_prob",
        vector_to_array(F.col(PROBABILITY_COL)).getItem(1).cast(DoubleType()),
    )

    roc_evaluator = BinaryClassificationEvaluator(
        labelCol=LABEL_COL,
        rawPredictionCol=RAW_PREDICTION_COL,
        metricName="areaUnderROC",
    )
    pr_evaluator = BinaryClassificationEvaluator(
        labelCol=LABEL_COL,
        rawPredictionCol=RAW_PREDICTION_COL,
        metricName="areaUnderPR",
    )

    roc_auc = roc_evaluator.evaluate(predictions)
    pr_auc = pr_evaluator.evaluate(predictions)

    predictions = predictions.withColumn(
        PREDICTION_COL,
        F.when(F.col("fraud_prob") > threshold, 1).otherwise(0),
    )
    tp = predictions.filter(
        (F.col(PREDICTION_COL) == 1) & (F.col(LABEL_COL) == 1)
    ).count()
    fp = predictions.filter(
        (F.col(PREDICTION_COL) == 1) & (F.col(LABEL_COL) == 0)
    ).count()
    fn = predictions.filter(
        (F.col(PREDICTION_COL) == 0) & (F.col(LABEL_COL) == 1)
    ).count()
    tn = predictions.filter(
        (F.col(PREDICTION_COL) == 0) & (F.col(LABEL_COL) == 0)
    ).count()

    precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
    recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0
    specificity = tn / (tn + fp) if (tn + fp) > 0 else 0.0

    metrics = {
        "roc_auc": round(roc_auc, 6),
        "pr_auc": round(pr_auc, 6),
        "tp": tp,
        "fp": fp,
        "fn": fn,
        "tn": tn,
        "precision": round(precision, 6),
        "recall": round(recall, 6),
        "specificity": round(specificity, 6),
        "threshold": threshold,
        "evaluated_at": datetime.now(timezone.utc).isoformat(),
    }

    logger.info(
        f"Evaluation (threshold={threshold:.2f}): ROC-AUC={metrics['roc_auc']} "
        f"PR-AUC={metrics['pr_auc']} Precision={metrics['precision']} "
        f"Recall={metrics['recall']} Spec={metrics['specificity']}"
    )
    return metrics


def optimize_threshold(model, test_df) -> tuple:
    """Sweep thresholds on the test set, pick best by F1.

    Returns (best_threshold, full_metrics_at_best_threshold).
    """
    predictions = model.transform(test_df)
    predictions = predictions.withColumn(
        "fraud_prob",
        vector_to_array(F.col(PROBABILITY_COL)).getItem(1).cast(DoubleType()),
    )
    predictions.cache().count()

    candidates = []
    t = THRESHOLD_TUNE_MIN
    while t <= THRESHOLD_TUNE_MAX:
        preds_t = predictions.withColumn(
            PREDICTION_COL,
            F.when(F.col("fraud_prob") > t, 1).otherwise(0),
        )
        tp = preds_t.filter(
            (F.col(PREDICTION_COL) == 1) & (F.col(LABEL_COL) == 1)
        ).count()
        fp = preds_t.filter(
            (F.col(PREDICTION_COL) == 1) & (F.col(LABEL_COL) == 0)
        ).count()
        fn = preds_t.filter(
            (F.col(PREDICTION_COL) == 0) & (F.col(LABEL_COL) == 1)
        ).count()
        tn = preds_t.filter(
            (F.col(PREDICTION_COL) == 0) & (F.col(LABEL_COL) == 0)
        ).count()

        precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
        recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0
        specificity = tn / (tn + fp) if (tn + fp) > 0 else 0.0
        f1 = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0.0
        youden = recall + specificity - 1

        candidates.append({
            "threshold": round(t, 2),
            "f1": f1,
            "youden": youden,
            "precision": precision,
            "recall": recall,
            "specificity": specificity,
            "tp": tp, "fp": fp, "fn": fn, "tn": tn,
        })
        t = round(t + THRESHOLD_TUNE_STEP, 2)

    predictions.unpersist()

    if THRESHOLD_TUNE_METRIC == "youden":
        best = max(candidates, key=lambda x: x["youden"])
        logger.info(
            f"Best threshold by Youden: {best['threshold']:.2f} "
            f"(Youden={best['youden']:.4f}, F1={best['f1']:.4f})"
        )
    else:
        best = max(candidates, key=lambda x: x["f1"])
        logger.info(
            f"Best threshold by F1: {best['threshold']:.2f} "
            f"(F1={best['f1']:.4f}, Youden={best['youden']:.4f})"
        )

    return best["threshold"], best


def get_feature_importances(model) -> dict:
    """Extract feature importances as ordered dict for persistence."""
    rf_model = model.stages[-1]
    importances = rf_model.featureImportances.toArray()
    return {
        name: round(float(imp), 6)
        for name, imp in sorted(
            zip(VECTOR_ASSEMBLER_INPUT_COLS, importances),
            key=lambda x: x[1], reverse=True,
        )
    }


def get_best_params(model) -> dict:
    """Extract best RF hyperparameters for persistence."""
    rf_model = model.stages[-1]
    return {
        "numTrees": rf_model.numTrees,
        "maxDepth": rf_model.getMaxDepth(),
        "impurity": rf_model.getImpurity(),
        "featureSubsetStrategy": rf_model.getFeatureSubsetStrategy(),
        "minInstancesPerNode": rf_model.getMinInstancesPerNode(),
        "minInfoGain": rf_model.getMinInfoGain(),
        "maxBins": rf_model.getMaxBins(),
        "subsamplingRate": rf_model.getSubsamplingRate(),
    }


def load_current_model_metrics(spark: SparkSession, project_id: str) -> dict | None:
    """Load current model metrics from model_versions table."""
    if _is_local(project_id):
        return _load_metrics_jdbc(spark)
    else:
        return None


def _load_metrics_jdbc(spark: SparkSession) -> dict | None:
    """Load latest model metrics via Spark JDBC."""
    host = os.environ.get("POSTGRES_HOST", "postgres")
    port = os.environ.get("POSTGRES_PORT", "5432")
    db = os.environ.get("POSTGRES_DB", "fraud_detection")
    user = os.environ.get("POSTGRES_USER", "fraud")
    password = os.environ.get("POSTGRES_PASSWORD", "fraud_local_2024")

    jdbc_url = f"jdbc:postgresql://{host}:{port}/{db}"
    properties = {"user": user, "password": password, "driver": "org.postgresql.Driver"}

    try:
        df = spark.read.jdbc(table="model_versions", url=jdbc_url, properties=properties)
        if df.count() > 0:
            row = df.orderBy(F.col("version").desc()).first()
            return {
                "pr_auc": row["pr_auc"],
                "roc_auc": row["roc_auc"],
                "version": row["version"],
            }
    except Exception as e:
        logger.info(f"No existing model versions: {e}")
    return None


def save_model_version_log(
    spark: SparkSession, project_id: str, metrics: dict,
    version: int, model_path: str, staging_bucket: str = "",
    feature_importances: dict | None = None,
    hyperparameters: dict | None = None,
):
    """Save model metadata to model_versions table.

    Local: uses psycopg2 (small metadata insert, not pipeline data).
    GCP: uses spark-bigquery connector.
    """
    fi_json = json.dumps(feature_importances) if feature_importances else None
    hp_json = json.dumps(hyperparameters) if hyperparameters else None

    if _is_local(project_id):
        import psycopg2
        host = os.environ.get("POSTGRES_HOST", "postgres")
        port = os.environ.get("POSTGRES_PORT", "5432")
        db = os.environ.get("POSTGRES_DB", "fraud_detection")
        user = os.environ.get("POSTGRES_USER", "fraud")
        password = os.environ.get("POSTGRES_PASSWORD", "fraud_local_2024")
        conn = psycopg2.connect(host=host, port=port, dbname=db, user=user, password=password)
        cur = conn.cursor()
        cur.execute(
            """INSERT INTO model_versions
               (version, model_path, created_at, roc_auc, pr_auc,
                precision, recall, tp, fp, fn, tn,
                best_threshold, hyperparameters, feature_importances)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
            (
                version,
                model_path,
                datetime.now(timezone.utc),
                metrics["roc_auc"],
                metrics["pr_auc"],
                metrics["precision"],
                metrics["recall"],
                metrics["tp"],
                metrics["fp"],
                metrics["fn"],
                metrics["tn"],
                metrics["threshold"],
                hp_json,
                fi_json,
            ),
        )
        conn.commit()
        cur.close()
        conn.close()
    else:
        log_row = [{
            "version": version,
            "model_path": model_path,
            "created_at": datetime.now(),
            "roc_auc": metrics["roc_auc"],
            "pr_auc": metrics["pr_auc"],
            "precision": metrics["precision"],
            "recall": metrics["recall"],
            "tp": metrics["tp"],
            "fp": metrics["fp"],
            "fn": metrics["fn"],
            "tn": metrics["tn"],
            "best_threshold": metrics["threshold"],
            "hyperparameters": hp_json,
            "feature_importances": fi_json,
        }]
        df = spark.createDataFrame(log_row)
        df = df.withColumn("created_at", F.to_timestamp(F.col("created_at")))
        writer = df.write.format("bigquery").option(
            "table", f"{project_id}.fraud_detection.model_versions"
        ).option("project", project_id)
        if staging_bucket:
            writer = writer.option("temporaryGcsBucket", staging_bucket)
        writer.mode("append").save()

    logger.info(f"Saved model version log: version={version}")


def log_feature_importances(feature_importances: dict):
    """Log top-10 feature importances."""
    sorted_fi = sorted(
        feature_importances.items(), key=lambda x: x[1], reverse=True
    )[:10]
    logger.info("Top-10 feature importances:")
    for name, imp in sorted_fi:
        logger.info(f"  {name}: {imp:.6f}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--project_id", required=True, help="GCP project ID or 'local'")
    parser.add_argument("--model_path", required=True, help="GCS/local path to save model")
    parser.add_argument("--staging_bucket", default="", help="GCS bucket for BigQuery temp files")
    parser.add_argument("--pg_host", default="postgres")
    parser.add_argument("--pg_port", type=int, default=5432)
    parser.add_argument("--pg_db", default="fraud_detection")
    parser.add_argument("--pg_user", default="fraud")
    parser.add_argument("--pg_password", default="fraud_local_2024")
    parser.add_argument("--skip_cv", action="store_true", help="Skip CV, use pipeline defaults")
    parser.add_argument("--skip_threshold_tune", action="store_true", help="Skip threshold tuning, use FRAUD_THRESHOLD")
    args = parser.parse_args()

    local = _is_local(args.project_id)

    if local:
        os.environ["POSTGRES_HOST"] = args.pg_host
        os.environ["POSTGRES_PORT"] = str(args.pg_port)
        os.environ["POSTGRES_DB"] = args.pg_db
        os.environ["POSTGRES_USER"] = args.pg_user
        os.environ["POSTGRES_PASSWORD"] = args.pg_password

    effective_threshold = FRAUD_THRESHOLD

    spark = get_spark_session(args.project_id)
    spark.sparkContext.setLogLevel("WARN")

    logger.info("Loading training data via Spark...")
    raw_df = load_training_data(spark, args.project_id)
    raw_df = raw_df.drop(
        PREDICTION_COL, PROBABILITY_COL, RAW_PREDICTION_COL,
        "predicted_at", "model_version", "features"
    )
    logger.info(f"Loaded {raw_df.count()} rows")

    logger.info("Computing class weights...")
    df = compute_class_weights(raw_df)

    logger.info("Splitting train/test (time-based 80/20)...")
    train_df, test_df = split_train_test(df)
    logger.info(f"Train: {train_df.count()} | Test: {test_df.count()}")

    if args.skip_cv:
        logger.info("Skipping CV — using pipeline defaults...")
        pipeline = build_tunable_pipeline()
        model = pipeline.fit(train_df)
    else:
        logger.info("Building tunable pipeline for CV...")
        pipeline = build_tunable_pipeline()
        param_grid = build_param_grid(args.project_id, pipeline)
        model = run_cross_validation(train_df, pipeline, param_grid)

    if args.skip_threshold_tune:
        logger.info(f"Skipping threshold tuning — using FRAUD_THRESHOLD={effective_threshold}")
        metrics = evaluate_model(model, test_df, effective_threshold)
    else:
        logger.info("Optimizing threshold on test set...")
        best_threshold, threshold_metrics = optimize_threshold(model, test_df)
        effective_threshold = best_threshold
        metrics = evaluate_model(model, test_df, effective_threshold)

    # Extract feature importances and hyperparameters
    feature_importances = get_feature_importances(model)
    log_feature_importances(feature_importances)

    best_params = get_best_params(model)
    logger.info(f"Best model params: numTrees={best_params['numTrees']}, "
                f"maxDepth={best_params['maxDepth']}, impurity={best_params['impurity']}")

    current_metrics = load_current_model_metrics(spark, args.project_id)
    should_promote = True

    if current_metrics:
        current_pr_auc = current_metrics["pr_auc"]
        new_pr_auc = metrics["pr_auc"]
        if new_pr_auc >= current_pr_auc * EVAL_GATE_MARGIN:
            logger.info(
                f"Promotion: new PR-AUC ({new_pr_auc}) >= current ({current_pr_auc}) * {EVAL_GATE_MARGIN}"
            )
        else:
            logger.warning(
                f"Rejection: new PR-AUC ({new_pr_auc}) < current ({current_pr_auc}) * {EVAL_GATE_MARGIN}"
            )
            should_promote = False

    if should_promote:
        logger.info(f"Saving model to {args.model_path}...")
        old_umask = os.umask(0)
        try:
            model.write().overwrite().save(args.model_path)
        finally:
            os.umask(old_umask)

        version = (current_metrics["version"] + 1) if current_metrics else 1
        save_model_version_log(
            spark, args.project_id, metrics, version,
            args.model_path, args.staging_bucket,
            feature_importances=feature_importances,
            hyperparameters=best_params,
        )
        logger.info(f"Model promotion complete: version={version}")
    else:
        logger.info("Model NOT promoted (evaluation gate failed)")

    print(json.dumps(metrics, indent=2))
    spark.stop()


if __name__ == "__main__":
    main()
