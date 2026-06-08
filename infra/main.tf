provider "google" {
  project = var.project_id
  region  = var.region
}

provider "google-beta" {
  project = var.project_id
  region  = var.region
}

locals {
  name_prefix = var.prefix
}

# ── Enable required APIs ──────────────────────────────────────────────
resource "google_project_service" "required" {
  for_each = toset([
    "compute.googleapis.com",
    "managedkafka.googleapis.com",
    "dataproc.googleapis.com",
    "bigquery.googleapis.com",
    "storage.googleapis.com",
    "aiplatform.googleapis.com",
    "cloudscheduler.googleapis.com",
    "monitoring.googleapis.com",
    "logging.googleapis.com",
  ])

  project            = var.project_id
  service            = each.value
  disable_on_destroy = false
}

# ── Network ───────────────────────────────────────────────────────────
module "network" {
  source     = "./modules/network"
  project_id = var.project_id
  prefix     = local.name_prefix
  region     = var.region
}

# ── IAM ───────────────────────────────────────────────────────────────
module "iam" {
  source          = "./modules/iam"
  project_id      = var.project_id
  prefix          = local.name_prefix
  bq_dataset_name = module.bigquery.dataset_name

  depends_on = [module.bigquery]
}

# ── Storage ───────────────────────────────────────────────────────────
module "storage" {
  source     = "./modules/storage"
  project_id = var.project_id
  prefix     = local.name_prefix
  region     = var.region
}

# ── BigQuery ──────────────────────────────────────────────────────────
module "bigquery" {
  source     = "./modules/bigquery"
  project_id = var.project_id
  prefix     = local.name_prefix
}

# ── Kafka ─────────────────────────────────────────────────────────────
module "kafka" {
  source              = "./modules/kafka"
  project_id          = var.project_id
  prefix              = local.name_prefix
  region              = var.region
  vpc_id              = module.network.vpc_id
  subnet_id           = module.network.subnet_id
  subnet_name         = module.network.subnet_name
  vcpu                = var.kafka_vcpu
  memory_gb           = var.kafka_memory_gb
  partitions          = var.kafka_partitions
  replication_factor  = var.kafka_replication_factor
  kafka_sa            = module.iam.streaming_sa_email
}

# ── Monitoring ───────────────────────────────────────────────────────
module "monitoring" {
  source              = "./modules/monitoring"
  project_id          = var.project_id
  prefix              = local.name_prefix
  region              = var.region
  monitoring_sa_email = module.iam.training_sa_email
}
