variable "project_id" {
  type = string
}

variable "prefix" {
  type = string
}

variable "region" {
  type = string
}

variable "vpc_id" {
  type = string
}

variable "subnet_id" {
  type = string
}

variable "subnet_name" {
  type = string
}

variable "vcpu" {
  type    = number
  default = 3
}

variable "memory_gb" {
  type    = number
  default = 6
}

variable "partitions" {
  type    = number
  default = 10
}

variable "replication_factor" {
  type    = number
  default = 3
}

variable "kafka_sa" {
  type = string
}
