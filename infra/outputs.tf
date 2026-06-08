output "vpc_id" {
  description = "VPC network ID"
  value       = module.network.vpc_id
}

output "subnet_id" {
  description = "Subnet ID"
  value       = module.network.subnet_id
}

output "kafka_cluster_id" {
  description = "Kafka cluster ID"
  value       = module.kafka.cluster_id
}

output "kafka_cluster_name" {
  description = "Kafka cluster resource name"
  value       = module.kafka.cluster_name
}

output "kafka_topic" {
  description = "Kafka topic name"
  value       = module.kafka.topic_id
}

output "checkpoints_bucket" {
  description = "GCS checkpoints bucket"
  value       = module.storage.checkpoints_bucket
}

output "model_registry_bucket" {
  description = "GCS model registry bucket"
  value       = module.storage.model_registry_bucket
}

output "staging_bucket" {
  description = "GCS staging bucket"
  value       = module.storage.staging_bucket
}

output "bq_dataset" {
  description = "BigQuery dataset ID"
  value       = module.bigquery.dataset_id
}

output "streaming_predictions_table" {
  description = "BigQuery streaming predictions table"
  value       = module.bigquery.streaming_predictions_table
}

output "ground_truth_labels_table" {
  description = "BigQuery ground truth labels table"
  value       = module.bigquery.ground_truth_labels_table
}

output "mlops_training_view" {
  description = "BigQuery mlops training view"
  value       = module.bigquery.mlops_training_view
}

output "generator_sa_email" {
  description = "Simulator service account email"
  value       = module.iam.generator_sa_email
}

output "streaming_sa_email" {
  description = "Streaming service account email"
  value       = module.iam.streaming_sa_email
}

output "training_sa_email" {
  description = "Training service account email"
  value       = module.iam.training_sa_email
}

output "pipeline_root_bucket" {
  description = "GCS pipeline root bucket"
  value       = module.storage.pipeline_root_bucket
}
