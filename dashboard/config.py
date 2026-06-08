import os
from dataclasses import dataclass, field


@dataclass
class Config:
    mode: str = field(init=False)
    project_id: str = ""
    region: str = "us-central1"
    bq_dataset: str = "fraud_detection"
    bq_predictions_table: str = "streaming_predictions"
    bq_labels_table: str = "ground_truth_labels"
    bq_model_versions_table: str = "model_versions"

    kafka_topic: str = "transactions_topic"
    kafka_bootstrap_servers: str = ""
    kafka_sa_email: str = ""

    pg_host: str = "localhost"
    pg_port: int = 5432
    pg_db: str = "fraud_detection"
    pg_user: str = "fraud"
    pg_password: str = "fraud_local_2024"

    def __post_init__(self):
        self.project_id = os.environ.get("PROJECT_ID", "")
        self.mode = "local" if self.project_id == "local" else "gcp"

        if self.mode == "local":
            self.pg_host = os.environ.get("POSTGRES_HOST", "localhost")
            self.pg_port = int(os.environ.get("POSTGRES_PORT", "5432"))
            self.pg_db = os.environ.get("POSTGRES_DB", "fraud_detection")
            self.pg_user = os.environ.get("POSTGRES_USER", "fraud")
            self.pg_password = os.environ.get("POSTGRES_PASSWORD", "fraud_local_2024")
            self.kafka_bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        else:
            self.kafka_bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "")
            self.kafka_sa_email = os.environ.get("KAFKA_SA_EMAIL", "")
            self.project_id = os.environ.get("PROJECT_ID", "")
            self.region = os.environ.get("GCP_REGION", "us-central1")

    @property
    def kafka_config(self) -> dict:
        base = {
            "bootstrap.servers": self.kafka_bootstrap_servers,
            "acks": "all",
            "linger.ms": 10,
        }
        if self.mode == "gcp" and self.kafka_sa_email:
            try:
                import google.auth
                from google.auth.transport.requests import Request
                creds, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
                creds.refresh(Request())
                base.update({
                    "security.protocol": "SASL_SSL",
                    "sasl.mechanism": "PLAIN",
                    "sasl.username": self.kafka_sa_email,
                    "sasl.password": creds.token,
                })
            except Exception:
                pass
        return base
