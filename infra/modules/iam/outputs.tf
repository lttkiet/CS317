output "generator_sa_email" {
  value = google_service_account.generator.email
}

output "streaming_sa_email" {
  value = google_service_account.streaming.email
}

output "training_sa_email" {
  value = google_service_account.training.email
}
