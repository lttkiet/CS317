# Real-Time Fraud Detection Pipeline

## Prerequisites

- Docker & Docker Compose v2
- 8 GB RAM minimum (16 GB for heavy profile)

## Quick Start (Local Docker)

```bash
make up          # Start Kafka + Spark + PostgreSQL + MinIO + Grafana
make init        # Create topic, schema, buckets (one-shot)
make simulator   # Foreground: transactions → Kafka (Ctrl-C to stop)
make training    # One-shot: train RF model
make streaming   # Foreground: Kafka → predict → PostgreSQL
make dashboard   # Streamlit at http://localhost:8501
```

**Order matters** — each step depends on the prior one.

### Data Rate Profiles

| Profile | TPS | Spark | Command |
|---|---|---|---|
| Light | 2–10 | 1g, 1 executor | `make light` |
| Normal | 10–50 | 2g, 2 executors | `make normal` (default) |
| Heavy | 200 (fixed) | 4g, 4 executors | `make heavy` |

### Other Commands

```bash
make psql          # docker exec psql -U fraud -d fraud_detection
make kafka-topics  # List Kafka topics
make kafka-lag     # Check consumer group lag
make logs          # Tail all logs
make health        # Service status table
make down          # Stop everything (⚠️ destroys volumes)
make report        # Build docs/report.pdf + docs/report.docx
```

## GCP Deployment

1. **Provision infrastructure** with Terraform:
   ```bash
   cd infra
   terraform init
   terraform apply -var="project_id=$PROJECT_ID"
   ```

2. **Submit streaming job** (always-on):
   ```bash
   ./submit_streaming.sh $PROJECT_ID
   ```

3. **Submit training job** (one-shot or scheduled):
   ```bash
   ./submit_training.sh $PROJECT_ID
   ```

The same Python code runs identically --- `PROJECT_ID=local` switches to local Docker mode.

## Folder Structure

```
├── simulator/        # Transaction generator → Kafka
├── streaming/        # Spark Structured Streaming inference
├── training/         # Spark ML Random Forest training
├── dashboard/        # Streamlit UI (live feed, charts, tester)
├── docker/           # Dockerfiles, init scripts, DB schema, Grafana
├── infra/            # Terraform for GCP deployment
├── docs/             # Technical report (md, pdf, docx) + presentation
├── monitoring/       # BigQuery scheduled query templates
├── pipeline/         # Vertex AI pipeline definition
├── models/           # Mounted model storage (local)
├── checkpoints/      # Mounted streaming checkpoints (local)
├── seed_model.py     # Creates baseline model for cold start
├── submit_streaming.sh  # GCP Dataproc streaming submission
├── submit_training.sh   # GCP Dataproc training submission
└── Makefile          # All commands
```
