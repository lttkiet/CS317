resource "google_storage_bucket" "checkpoints" {
  name          = "${var.project_id}-${var.prefix}-checkpoints"
  project       = var.project_id
  location      = var.region
  storage_class = "STANDARD"
  uniform_bucket_level_access = true

  lifecycle_rule {
    condition {
      age = 7
    }
    action {
      type = "Delete"
    }
  }

  versioning {
    enabled = true
  }
}

resource "google_storage_bucket" "model_registry" {
  name          = "${var.project_id}-${var.prefix}-model-registry"
  project       = var.project_id
  location      = var.region
  storage_class = "STANDARD"
  uniform_bucket_level_access = true

  versioning {
    enabled = true
  }
}

resource "google_storage_bucket" "staging" {
  name          = "${var.project_id}-${var.prefix}-staging"
  project       = var.project_id
  location      = var.region
  storage_class = "STANDARD"
  uniform_bucket_level_access = true

  lifecycle_rule {
    condition {
      age = 30
    }
    action {
      type = "Delete"
    }
  }
}

resource "google_storage_bucket" "pipeline_root" {
  name          = "${var.project_id}-${var.prefix}-pipeline-root"
  project       = var.project_id
  location      = var.region
  storage_class = "STANDARD"
  uniform_bucket_level_access = true
}
