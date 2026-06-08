output "checkpoints_bucket" {
  value = google_storage_bucket.checkpoints.name
}

output "model_registry_bucket" {
  value = google_storage_bucket.model_registry.name
}

output "staging_bucket" {
  value = google_storage_bucket.staging.name
}

output "pipeline_root_bucket" {
  value = google_storage_bucket.pipeline_root.name
}
