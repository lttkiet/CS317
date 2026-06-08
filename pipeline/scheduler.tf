# Cloud Scheduler + Vertex AI Pipeline trigger (Terraform)

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

variable "pipeline_bucket" {
  type = string
}

variable "training_sa_email" {
  type = string
}

# ── Cloud Scheduler → Vertex AI Pipeline trigger ─────────────────────
resource "google_cloud_scheduler_job" "weekly_retraining" {
  name      = "${var.prefix}-weekly-retraining"
  region    = var.region
  project   = var.project_id
  schedule  = "0 2 * * 1"  # Every Monday at 2:00 AM UTC
  time_zone = "UTC"

  http_target {
    uri         = "https://us-central1-aiplatform.googleapis.com/v1/projects/${var.project_id}/locations/${var.region}/pipelines:run"
    http_method = "POST"

    oauth_token {
      service_account_email = var.training_sa_email
      scope                 = "https://www.googleapis.com/auth/cloud-platform"
    }

    body = base64encode(jsonencode({
      pipelineSpec = {
        pipelineInfo = {
          name    = "fraud-model-retraining"
          version = "1.0"
        }
        root = {
          dag = {
            tasks = {
              training-task = {
                taskInfo = {
                  name = "fraud-retraining"
                }
                componentRef = {
                  name = "training-component"
                }
              }
            }
          }
        }
      }
    }))
  }

  retry_config {
    retry_count          = 3
    max_retry_duration   = "600s"
    min_backoff_duration = "10s"
    max_backoff_duration = "300s"
  }
}

# ── Alert policy for pipeline failures ────────────────────────────────
resource "google_monitoring_alert_policy" "pipeline_failure" {
  display_name = "${var.prefix} - Retraining Pipeline Failure"
  project      = var.project_id
  combiner     = "OR"

  conditions {
    display_name = "Vertex AI Pipeline failed"
    condition_threshold {
      filter          = "resource.type=\"aiplatform.googleapis.com/PipelineJob\" AND metric.type=\"aiplatform.googleapis.com/pipeline/job_state\" AND metric.labels.state=\"PIPELINE_STATE_FAILED\""
      duration        = "60s"
      comparison      = "COMPARISON_GT"
      threshold_value = 0

      aggregations {
        alignment_period     = "60s"
        per_series_aligner   = "ALIGN_COUNT"
        cross_series_reducer = "REDUCE_SUM"
      }

      trigger {
        count = 1
      }
    }
  }

  notification_channels = []

  alert_strategy {
    auto_close = "1800s"
  }
}
