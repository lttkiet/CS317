resource "google_bigquery_dataset" "fraud_detection" {
  dataset_id    = "fraud_detection"
  friendly_name = "Fraud Detection"
  description   = "Dataset for real-time fraud detection MLOps pipeline"
  location      = "US"
  project       = var.project_id
}

resource "google_bigquery_dataset" "spark_temp" {
  dataset_id    = "spark_temp"
  friendly_name = "Spark BigQuery Temporary"
  description   = "Temporary dataset for Spark-BigQuery connector materialization"
  location      = "US"
  project       = var.project_id
}

resource "google_bigquery_dataset_iam_member" "training_spark_temp" {
  dataset_id = google_bigquery_dataset.spark_temp.dataset_id
  project    = var.project_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:fraud-training@${var.project_id}.iam.gserviceaccount.com"
}

resource "google_bigquery_dataset_iam_member" "streaming_spark_temp" {
  dataset_id = google_bigquery_dataset.spark_temp.dataset_id
  project    = var.project_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:fraud-streaming@${var.project_id}.iam.gserviceaccount.com"
}

# ── Table: streaming_predictions (append-only) ───────────────────────
resource "google_bigquery_table" "streaming_predictions" {
  dataset_id = google_bigquery_dataset.fraud_detection.dataset_id
  table_id   = "streaming_predictions"
  project    = var.project_id

  description = "Append-only streaming predictions from PySpark Structured Streaming"

  time_partitioning {
    type  = "DAY"
    field = "PREDICTED_AT"
  }

  clustering = ["CUSTOMER_ID", "TERMINAL_ID"]

  schema = <<EOF
[
  {"name": "TRANSACTION_ID",                      "type": "INT64",     "mode": "REQUIRED"},
  {"name": "TX_DATETIME",                         "type": "TIMESTAMP", "mode": "REQUIRED"},
  {"name": "CUSTOMER_ID",                         "type": "INT64",     "mode": "REQUIRED"},
  {"name": "TERMINAL_ID",                         "type": "INT64",     "mode": "REQUIRED"},
  {"name": "TX_AMOUNT",                           "type": "FLOAT64",   "mode": "REQUIRED"},
  {"name": "TX_TIME_SECONDS",                     "type": "INT64",     "mode": "NULLABLE"},
  {"name": "TX_TIME_DAYS",                        "type": "INT64",     "mode": "NULLABLE"},
  {"name": "TX_DURING_WEEKEND",                   "type": "INT64",     "mode": "NULLABLE"},
  {"name": "TX_DURING_NIGHT",                     "type": "INT64",     "mode": "NULLABLE"},
  {"name": "CUSTOMER_ID_NB_TX_1DAY_WINDOW",       "type": "INT64",     "mode": "NULLABLE"},
  {"name": "CUSTOMER_ID_AVG_AMOUNT_1DAY_WINDOW",  "type": "FLOAT64",   "mode": "NULLABLE"},
  {"name": "CUSTOMER_ID_NB_TX_7DAY_WINDOW",       "type": "INT64",     "mode": "NULLABLE"},
  {"name": "CUSTOMER_ID_AVG_AMOUNT_7DAY_WINDOW",  "type": "FLOAT64",   "mode": "NULLABLE"},
  {"name": "CUSTOMER_ID_NB_TX_30DAY_WINDOW",      "type": "INT64",     "mode": "NULLABLE"},
  {"name": "CUSTOMER_ID_AVG_AMOUNT_30DAY_WINDOW", "type": "FLOAT64",   "mode": "NULLABLE"},
  {"name": "TERMINAL_ID_NB_TX_1DAY_WINDOW",       "type": "INT64",     "mode": "NULLABLE"},
  {"name": "TERMINAL_ID_RISK_1DAY_WINDOW",        "type": "FLOAT64",   "mode": "NULLABLE"},
  {"name": "TERMINAL_ID_NB_TX_7DAY_WINDOW",       "type": "INT64",     "mode": "NULLABLE"},
  {"name": "TERMINAL_ID_RISK_7DAY_WINDOW",        "type": "FLOAT64",   "mode": "NULLABLE"},
  {"name": "TERMINAL_ID_NB_TX_30DAY_WINDOW",      "type": "INT64",     "mode": "NULLABLE"},
  {"name": "TERMINAL_ID_RISK_30DAY_WINDOW",       "type": "FLOAT64",   "mode": "NULLABLE"},
  {"name": "FRAUD_PROBABILITY",                   "type": "FLOAT64",   "mode": "REQUIRED"},
  {"name": "IS_FRAUD_PREDICTED",                  "type": "INT64",     "mode": "REQUIRED"},
  {"name": "PREDICTED_AT",                        "type": "TIMESTAMP", "mode": "REQUIRED"},
  {"name": "MODEL_VERSION",                       "type": "STRING",    "mode": "NULLABLE"}
]
EOF
}

# ── Table: ground_truth_labels ────────────────────────────────────────
resource "google_bigquery_table" "ground_truth_labels" {
  dataset_id = google_bigquery_dataset.fraud_detection.dataset_id
  table_id   = "ground_truth_labels"
  project    = var.project_id

  description = "Ground-truth fraud labels uploaded with 7-day delay (simulating chargebacks)"

  time_partitioning {
    type  = "DAY"
    field = "LABEL_RECEIVED_AT"
  }

  schema = <<EOF
[
  {"name": "TRANSACTION_ID",    "type": "INT64",     "mode": "REQUIRED"},
  {"name": "TX_FRAUD",          "type": "INT64",     "mode": "REQUIRED"},
  {"name": "TX_FRAUD_SCENARIO", "type": "INT64",     "mode": "REQUIRED"},
  {"name": "LABEL_RECEIVED_AT", "type": "TIMESTAMP", "mode": "NULLABLE"}
]
EOF
}

# ── Materialized view: mlops_training_view ────────────────────────────
resource "google_bigquery_table" "mlops_training_view" {
  dataset_id          = google_bigquery_dataset.fraud_detection.dataset_id
  table_id            = "mlops_training_view"
  project             = var.project_id
  deletion_protection = false

  description = "Materialized view joining predictions with ground-truth labels for retraining"

  view {
    query          = <<-SQL
      SELECT
        sp.*,
        gt.TX_FRAUD,
        gt.TX_FRAUD_SCENARIO
      FROM `${var.project_id}.fraud_detection.streaming_predictions` sp
      LEFT JOIN `${var.project_id}.fraud_detection.ground_truth_labels` gt
        ON sp.TRANSACTION_ID = gt.TRANSACTION_ID
      WHERE gt.TX_FRAUD IS NOT NULL
    SQL
    use_legacy_sql = false
  }

  depends_on = [
    google_bigquery_table.streaming_predictions,
    google_bigquery_table.ground_truth_labels,
  ]
}
