output "cluster_id" {
  value = google_managed_kafka_cluster.fraud_cluster.cluster_id
}

output "cluster_name" {
  value = google_managed_kafka_cluster.fraud_cluster.name
}

output "topic_id" {
  value = google_managed_kafka_topic.transactions_topic.topic_id
}
