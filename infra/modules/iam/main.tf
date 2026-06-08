# ── Transaction Simulator SA ──────────────────────────────────────────
locals {
  sa_prefix = var.prefix
}

resource "google_service_account" "generator" {
  account_id   = "${local.sa_prefix}-generator"
  display_name = "Fraud Transaction Simulator"
  project      = var.project_id
}

resource "google_project_iam_member" "generator_kafka" {
  project = var.project_id
  role    = "roles/managedkafka.client"
  member  = "serviceAccount:${google_service_account.generator.email}"
}

resource "google_bigquery_dataset_iam_member" "generator_bq_viewer" {
  project    = var.project_id
  dataset_id = var.bq_dataset_name
  role       = "roles/bigquery.dataViewer"
  member     = "serviceAccount:${google_service_account.generator.email}"
}

resource "google_bigquery_table_iam_member" "generator_bq_editor" {
  project    = var.project_id
  dataset_id = var.bq_dataset_name
  table_id   = "ground_truth_labels"
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.generator.email}"
}

# ── Streaming Consumer SA ────────────────────────────────────────────
resource "google_service_account" "streaming" {
  account_id   = "${local.sa_prefix}-streaming"
  display_name = "Fraud Streaming Consumer"
  project      = var.project_id
}

resource "google_project_iam_member" "streaming_kafka" {
  project = var.project_id
  role    = "roles/managedkafka.client"
  member  = "serviceAccount:${google_service_account.streaming.email}"
}

resource "google_project_iam_member" "streaming_bq" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_service_account.streaming.email}"

  condition {
    title       = "predictions_only"
    description = "Only allow writes to streaming_predictions table"
    expression  = "resource.name.startsWith('projects/${var.project_id}/datasets/fraud_detection/tables/streaming_predictions')"
  }
}

resource "google_project_iam_member" "streaming_bq_read_session" {
  project = var.project_id
  role    = "roles/bigquery.readSessionUser"
  member  = "serviceAccount:${google_service_account.streaming.email}"
}

resource "google_project_iam_member" "streaming_storage_viewer" {
  project = var.project_id
  role    = "roles/storage.objectViewer"
  member  = "serviceAccount:${google_service_account.streaming.email}"
}

resource "google_project_iam_member" "streaming_storage_admin" {
  project = var.project_id
  role    = "roles/storage.admin"
  member  = "serviceAccount:${google_service_account.streaming.email}"

  condition {
    title       = "checkpoints_and_staging_only"
    description = "Only allow access to checkpoints and staging buckets"
    expression  = "resource.name.startsWith('projects/${var.project_id}/buckets/${var.project_id}-${local.sa_prefix}-checkpoints') || resource.name.startsWith('projects/${var.project_id}/buckets/${var.project_id}-${local.sa_prefix}-staging')"
  }
}

# ── Training SA ──────────────────────────────────────────────────────
resource "google_service_account" "training" {
  account_id   = "${local.sa_prefix}-training"
  display_name = "Fraud Model Training"
  project      = var.project_id
}

resource "google_project_iam_member" "training_dataproc" {
  project = var.project_id
  role    = "roles/dataproc.worker"
  member  = "serviceAccount:${google_service_account.training.email}"
}

resource "google_project_iam_member" "training_bq_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.training.email}"
}

resource "google_project_iam_member" "training_bq_read_session" {
  project = var.project_id
  role    = "roles/bigquery.readSessionUser"
  member  = "serviceAccount:${google_service_account.training.email}"
}

resource "google_project_iam_member" "training_bq_viewer" {
  project = var.project_id
  role    = "roles/bigquery.dataViewer"
  member  = "serviceAccount:${google_service_account.training.email}"

  condition {
    title       = "training_view_only"
    description = "Only allow reads from fraud_detection dataset"
    expression  = "resource.name.startsWith('projects/${var.project_id}/datasets/fraud_detection')"
  }
}

resource "google_project_iam_member" "training_storage_object_admin" {
  project = var.project_id
  role    = "roles/storage.objectAdmin"
  member  = "serviceAccount:${google_service_account.training.email}"

  condition {
    title       = "model_registry_only"
    description = "Only allow access to model registry bucket"
    expression  = "resource.name.startsWith('projects/${var.project_id}/buckets/${var.project_id}-${local.sa_prefix}-model-registry')"
  }
}
