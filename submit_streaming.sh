#!/bin/bash
# Submit the fraud detection streaming job to Dataproc Serverless
#
# Usage:
#   ./submit_streaming.sh PROJECT_ID KAFKA_HOST:PORT
#
# Prerequisites:
#   - Terraform applied (infra/)
#   - Initial model trained and saved to GCS
#   - Kafka cluster running

set -euo pipefail

PROJECT_ID="${1:?Usage: $0 PROJECT_ID KAFKA_HOST:PORT}"
REGION="us-central1"
PREFIX="fraud"
KAFKA_HOST="${2:-bootstrap.${PREFIX}-kafka-cluster.${REGION}.managedkafka.${PROJECT_ID}.cloud.goog:9092}"

# Derived values
MODEL_BUCKET="${PROJECT_ID}-${PREFIX}-model-registry"
STAGING_BUCKET="${PROJECT_ID}-${PREFIX}-staging"
CHECKPOINT_BUCKET="${PROJECT_ID}-${PREFIX}-checkpoints"
SA_EMAIL="${PREFIX}-streaming@${PROJECT_ID}.iam.gserviceaccount.com"
KAFKA_SA="${PREFIX}-streaming@${PROJECT_ID}.iam.gserviceaccount.com"

echo "=== Submitting streaming job ==="
echo "Project:  ${PROJECT_ID}"
echo "Kafka:    ${KAFKA_HOST}"
echo "Model:    gs://${MODEL_BUCKET}/latest_rf_model"

gcloud dataproc batches submit pyspark \
    streaming/inference.py \
    --project="${PROJECT_ID}" \
    --region="${REGION}" \
    --batch="fraud-streaming-$(date +%s)" \
    --subnet="projects/${PROJECT_ID}/regions/${REGION}/subnetworks/fraud-subnet" \
    --jars="gs://spark-lib/bigquery/spark-3.5-bigquery-0.44.2.jar,gs://${STAGING_BUCKET}/deps/spark-sql-kafka-0-10_2.13-3.5.1.jar,gs://${STAGING_BUCKET}/deps/kafka-clients-3.7.1.jar,gs://${STAGING_BUCKET}/deps/commons-pool2-2.11.1.jar,gs://${STAGING_BUCKET}/deps/spark-token-provider-kafka-0-10_2.13-3.5.1.jar,gs://${STAGING_BUCKET}/deps/managed-kafka-auth-login-handler-1.0.6.jar,gs://${STAGING_BUCKET}/deps/google-api-client-1.32.1.jar,gs://${STAGING_BUCKET}/deps/google-auth-library-credentials-1.23.0.jar,gs://${STAGING_BUCKET}/deps/google-auth-library-oauth2-http-1.23.0.jar,gs://${STAGING_BUCKET}/deps/google-http-client-1.43.3.jar,gs://${STAGING_BUCKET}/deps/google-http-client-gson-1.43.3.jar,gs://${STAGING_BUCKET}/deps/google-http-client-jackson2-1.43.3.jar,gs://${STAGING_BUCKET}/deps/opencensus-api.jar,gs://${STAGING_BUCKET}/deps/opencensus-http.jar" \
    --py-files="gs://${STAGING_BUCKET}/streaming/deps_v6.zip" \
    --deps-bucket="gs://${STAGING_BUCKET}" \
    --properties=spark.dynamicAllocation.minExecutors=2,spark.dynamicAllocation.maxExecutors=2,spark.executor.memory=8g,spark.executor.cores=4,spark.driver.memory=9g,spark.driver.cores=4,spark.sql.shuffle.partitions=20 \
    --service-account="${SA_EMAIL}" \
    -- \
    --project_id="${PROJECT_ID}" \
    --kafka_bootstrap_servers="${KAFKA_HOST}" \
    --kafka_topic="transactions_topic" \
    --model_bucket="${MODEL_BUCKET}" \
    --staging_bucket="${STAGING_BUCKET}" \
    --checkpoint_bucket="${CHECKPOINT_BUCKET}" \
    --bq_dataset="fraud_detection" \
    --kafka_sa_email="${KAFKA_SA}" \
    --starting_offsets="latest" \
    --max_offsets_per_trigger=5000

echo "=== Streaming job submitted ==="
echo "Monitor at: https://console.cloud.google.com/dataproc/batches?project=${PROJECT_ID}"
