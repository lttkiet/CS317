terraform {
  required_providers {
    google-beta = {
      source = "hashicorp/google-beta"
    }
  }
}

resource "google_managed_kafka_cluster" "fraud_cluster" {
  provider   = google-beta
  cluster_id = "${var.prefix}-kafka-cluster"
  location   = var.region
  project    = var.project_id

  capacity_config {
    vcpu_count   = var.vcpu
    memory_bytes = var.memory_gb * 1073741824
  }

  gcp_config {
    access_config {
      network_configs {
        subnet = "projects/${var.project_id}/regions/${var.region}/subnetworks/${var.subnet_name}"
      }
    }
  }

  labels = {
    env = "demo"
  }
}

resource "google_managed_kafka_topic" "transactions_topic" {
  provider   = google-beta
  cluster    = "${var.prefix}-kafka-cluster"
  topic_id   = "transactions_topic"
  location   = var.region
  project    = var.project_id
  partition_count   = var.partitions
  replication_factor = var.replication_factor

  configs = {
    "retention.ms"        = "604800000"
    "cleanup.policy"      = "delete"
    "min.insync.replicas" = tostring(max(1, var.replication_factor - 1))
    "segment.ms"          = "3600000"
    "delete.retention.ms" = "86400000"
  }
}
