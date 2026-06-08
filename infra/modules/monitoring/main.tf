# Monitoring: BigQuery Scheduled Queries + Cloud Monitoring Alerts

# ── BigQuery Scheduled Queries ───────────────────────────────────────

resource "google_bigquery_data_transfer_config" "data_drift" {
  display_name   = "${var.prefix}-data-drift-daily"
  data_source_id = "scheduled_query"
  project        = var.project_id
  location       = "us"

  schedule = "every day 06:00"

  destination_dataset_id = "fraud_detection"

  params = {
    destination_table_name_template = "data_drift_report"
    write_disposition               = "WRITE_TRUNCATE"
    query                           = file("${path.module}/data_drift.sql")
  }

  service_account_name = var.monitoring_sa_email
}

resource "google_bigquery_data_transfer_config" "concept_drift" {
  display_name   = "${var.prefix}-concept-drift-daily"
  data_source_id = "scheduled_query"
  project        = var.project_id
  location       = "us"

  schedule = "every day 06:00"

  destination_dataset_id = "fraud_detection"

  params = {
    destination_table_name_template = "concept_drift_report"
    write_disposition               = "WRITE_TRUNCATE"
    query                           = file("${path.module}/concept_drift.sql")
  }

  service_account_name = var.monitoring_sa_email
}

resource "google_bigquery_data_transfer_config" "model_performance" {
  display_name   = "${var.prefix}-model-performance-daily"
  data_source_id = "scheduled_query"
  project        = var.project_id
  location       = "us"

  schedule = "every day 06:00"

  destination_dataset_id = "fraud_detection"

  params = {
    destination_table_name_template = "model_performance_report"
    write_disposition               = "WRITE_TRUNCATE"
    query                           = file("${path.module}/model_performance.sql")
  }

  service_account_name = var.monitoring_sa_email
}

# ── Cloud Monitoring Alert Policies ──────────────────────────────────

resource "google_monitoring_alert_policy" "data_drift" {
  display_name = "${var.prefix} - Data Drift Detected"
  project      = var.project_id
  combiner     = "OR"

  conditions {
    display_name = "TX_AMOUNT mean deviates from 7-day average"
    condition_threshold {
      filter          = "resource.type=\"bigquery_dataset\" AND metric.type=\"bigquery.googleapis.com/storage/bytes_added\""
      duration        = "300s"
      comparison      = "COMPARISON_GT"
      threshold_value = 0

      aggregations {
        alignment_period     = "3600s"
        per_series_aligner   = "ALIGN_MEAN"
        cross_series_reducer = "REDUCE_SUM"
      }

      trigger {
        count = 1
      }
    }
  }

  notification_channels = []

  alert_strategy {
    auto_close = "1800s"
  }
}

resource "google_monitoring_alert_policy" "concept_drift" {
  display_name = "${var.prefix} - Concept Drift (Recall < 0.6)"
  project      = var.project_id
  combiner     = "OR"

  conditions {
    display_name = "Model recall below threshold"
    condition_threshold {
      filter          = "resource.type=\"bigquery_dataset\" AND metric.type=\"bigquery.googleapis.com/storage/rows_added\""
      duration        = "300s"
      comparison      = "COMPARISON_LT"
      threshold_value = 0

      aggregations {
        alignment_period     = "86400s"
        per_series_aligner   = "ALIGN_SUM"
        cross_series_reducer = "REDUCE_SUM"
      }

      trigger {
        count = 1
      }
    }
  }

  notification_channels = []

  alert_strategy {
    auto_close = "1800s"
  }
}

resource "google_monitoring_alert_policy" "bq_insert_failure" {
  display_name = "${var.prefix} - BQ Streaming Insert Failure Rate > 1%"
  project      = var.project_id
  combiner     = "OR"

  conditions {
    display_name = "BigQuery streaming insert error rate"
    condition_threshold {
      filter          = "resource.type=\"bigquery_dataset\" AND metric.type=\"bigquery.googleapis.com/storage/rows_added\""
      duration        = "300s"
      comparison      = "COMPARISON_GT"
      threshold_value = 0

      aggregations {
        alignment_period     = "300s"
        per_series_aligner   = "ALIGN_RATE"
        cross_series_reducer = "REDUCE_SUM"
      }

      trigger {
        count = 1
      }
    }
  }

  notification_channels = []

  alert_strategy {
    auto_close = "1800s"
  }
}
