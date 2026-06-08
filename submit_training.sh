#!/bin/bash
# Submit the initial training job to Dataproc Serverless
#
# Usage:
#   ./submit_training.sh PROJECT_ID
#
# This generates a baseline model and saves it to GCS.
# Must be run before the streaming job.

set -euo pipefail

PROJECT_ID="${1:?Usage: $0 PROJECT_ID}"
REGION="us-central1"
PREFIX="fraud"

# Derived values
MODEL_BUCKET="${PROJECT_ID}-${PREFIX}-model-registry"
STAGING_BUCKET="${PROJECT_ID}-${PREFIX}-staging"
SA_EMAIL="${PREFIX}-training@${PROJECT_ID}.iam.gserviceaccount.com"

echo "=== Submitting training job ==="
echo "Project:  ${PROJECT_ID}"
echo "Model:    gs://${MODEL_BUCKET}/latest_rf_model"

gcloud dataproc batches submit pyspark \
    training/train.py \
    --region="${REGION}" \
    --batch="fraud-initial-train-$(date +%s)" \
    --jars="gs://spark-lib/bigquery/spark-3.5-bigquery-0.44.2.jar" \
    --files="training/constants.py" \
    --py-files="training/constants.py" \
    --deps-bucket="gs://${STAGING_BUCKET}" \
    --properties=spark.executor.instances=4,spark.executor.memory=8g,spark.executor.cores=4,spark.sql.shuffle.partitions=200 \
    --service-account="${SA_EMAIL}" \
    -- \
    --project_id="${PROJECT_ID}" \
    --model_path="gs://${MODEL_BUCKET}/latest_rf_model"

echo "=== Training job submitted ==="
echo "Monitor at: https://console.cloud.google.com/dataproc/batches?project=${PROJECT_ID}"
