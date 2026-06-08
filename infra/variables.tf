variable "project_id" {
  description = "GCP project ID"
  type        = string
}

variable "region" {
  description = "GCP region"
  type        = string
  default     = "us-central1"
}

variable "prefix" {
  description = "Resource name prefix"
  type        = string
  default     = "fraud"
}

variable "kafka_vcpu" {
  description = "Kafka cluster vCPU count"
  type        = number
  default     = 3
}

variable "kafka_memory_gb" {
  description = "Kafka cluster memory in GiB"
  type        = number
  default     = 6
}

variable "kafka_partitions" {
  description = "Kafka topic partition count"
  type        = number
  default     = 10
}

variable "kafka_replication_factor" {
  description = "Kafka topic replication factor"
  type        = number
  default     = 3
}
