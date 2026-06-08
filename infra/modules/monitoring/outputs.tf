output "data_drift_transfer_config" {
  value = google_bigquery_data_transfer_config.data_drift.name
}

output "concept_drift_transfer_config" {
  value = google_bigquery_data_transfer_config.concept_drift.name
}

output "model_performance_transfer_config" {
  value = google_bigquery_data_transfer_config.model_performance.name
}
