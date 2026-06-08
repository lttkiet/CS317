output "dataset_id" {
  value = google_bigquery_dataset.fraud_detection.dataset_id
}

output "dataset_name" {
  value = google_bigquery_dataset.fraud_detection.dataset_id
}

output "streaming_predictions_table" {
  value = "${var.project_id}.${google_bigquery_dataset.fraud_detection.dataset_id}.${google_bigquery_table.streaming_predictions.table_id}"
}

output "ground_truth_labels_table" {
  value = "${var.project_id}.${google_bigquery_dataset.fraud_detection.dataset_id}.${google_bigquery_table.ground_truth_labels.table_id}"
}

output "mlops_training_view" {
  value = "${var.project_id}.${google_bigquery_dataset.fraud_detection.dataset_id}.${google_bigquery_table.mlops_training_view.table_id}"
}
