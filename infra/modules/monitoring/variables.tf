variable "project_id" {
  type = string
}

variable "region" {
  type    = string
  default = "us-central1"
}

variable "prefix" {
  type = string
}

variable "monitoring_sa_email" {
  type        = string
  description = "Service account for running scheduled queries"
}
