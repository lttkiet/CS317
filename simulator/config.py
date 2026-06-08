import os
import json
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any


def _is_local() -> bool:
    return os.environ.get("PROJECT_ID", "") == "local"


# ── Data Rate Profiles ──────────────────────────────────────────────
# Set DATA_RATE=light|normal|heavy to auto-configure all simulation params.
# Individual overrides (MIN_TPS, MAX_TPS, etc.) take precedence if set.

DATA_RATE_PROFILES = {
    "light": {
        "description": "Low resource usage — laptops, small demos",
        "min_tps": 2,
        "max_tps": 10,
        "num_customers": 200,
        "num_terminals": 1000,
        "spark_executor_memory": "1g",
        "spark_executor_instances": "1",
        "spark_shuffle_partitions": "20",
    },
    "normal": {
        "description": "Medium load — desktops, full demo",
        "min_tps": 10,
        "max_tps": 50,
        "num_customers": 1000,
        "num_terminals": 10000,
        "spark_executor_memory": "2g",
        "spark_executor_instances": "2",
        "spark_shuffle_partitions": "100",
    },
    "heavy": {
        "description": "High throughput — servers, stress testing",
        "min_tps": 200,
        "max_tps": 200,
        "num_customers": 10000,
        "num_terminals": 100000,
        "spark_executor_memory": "4g",
        "spark_executor_instances": "4",
        "spark_shuffle_partitions": "200",
    },
}


def _get_data_rate() -> str:
    """Get DATA_RATE from env, default to 'normal'."""
    rate = os.environ.get("DATA_RATE", "normal").lower()
    if rate not in DATA_RATE_PROFILES:
        print(f"[config] Unknown DATA_RATE '{rate}', using 'normal'")
        rate = "normal"
    return rate


def _resolve_param(env_key: str, default) -> Any:
    """Resolve a parameter: env var > default."""
    env_val = os.environ.get(env_key)
    if env_val is not None:
        return env_val
    return default


@dataclass
class Config:
    """Central configuration loaded from environment variables with sensible defaults.

    Set DATA_RATE=light|normal|heavy to auto-configure all simulation params.
    Individual overrides (MIN_TPS, MAX_TPS, etc.) take precedence if set.
    """

    # GCP
    project_id: str = os.environ.get("PROJECT_ID", "")
    region: str = os.environ.get("GCP_REGION", "us-central1")
    local_mode: bool = field(default=False, init=False)

    # Kafka
    bootstrap_servers: str = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "")
    kafka_topic: str = os.environ.get("KAFKA_TOPIC", "transactions_topic")
    kafka_sa_email: str = os.environ.get("KAFKA_SA_EMAIL", "")

    # BigQuery / PostgreSQL
    bq_dataset: str = os.environ.get("BQ_DATASET", "fraud_detection")
    bq_ground_truth_table: str = os.environ.get("BQ_GROUND_TRUTH_TABLE", "ground_truth_labels")

    # Local PostgreSQL
    pg_host: str = os.environ.get("POSTGRES_HOST", "localhost")
    pg_port: int = int(os.environ.get("POSTGRES_PORT", "5432"))
    pg_db: str = os.environ.get("POSTGRES_DB", "fraud_detection")
    pg_user: str = os.environ.get("POSTGRES_USER", "fraud")
    pg_password: str = os.environ.get("POSTGRES_PASSWORD", "fraud_local_2024")

    # Simulation parameters — resolved via DATA_RATE profile + env overrides
    num_customers: int = field(default=0, init=False)
    num_terminals: int = field(default=0, init=False)
    fraud_rate: float = field(default=0.0, init=False)
    min_tps: int = field(default=0, init=False)
    max_tps: int = field(default=0, init=False)
    label_delay_days: int = field(default=0, init=False)

    # Spark config (for local mode)
    spark_executor_memory: str = field(default="", init=False)
    spark_executor_instances: str = field(default="", init=False)
    spark_shuffle_partitions: str = field(default="", init=False)

    # Data rate profile
    data_rate: str = field(default="", init=False)
    data_rate_profile: dict = field(default_factory=dict, init=False)

    # Fraud scenario probabilities (must sum to 1.0)
    # Scenario 0 = legitimate, 1 = stolen card, 2 = counterfeit, 3 = card-not-present
    fraud_scenario_weights: list = field(default_factory=lambda: [0.66, 0.1133, 0.1133, 0.1134])

    def __post_init__(self):
        self.local_mode = _is_local()

        # Load data rate profile
        self.data_rate = _get_data_rate()
        self.data_rate_profile = DATA_RATE_PROFILES[self.data_rate]

        # Resolve simulation params: env var > profile > default
        self.min_tps = int(_resolve_param("MIN_TPS", self.data_rate_profile["min_tps"]))
        self.max_tps = int(_resolve_param("MAX_TPS", self.data_rate_profile["max_tps"]))
        self.num_customers = int(_resolve_param("NUM_CUSTOMERS", self.data_rate_profile["num_customers"]))
        self.num_terminals = int(_resolve_param("NUM_TERMINALS", self.data_rate_profile["num_terminals"]))
        self.fraud_rate = float(_resolve_param("FRAUD_RATE", 0.34))
        self.label_delay_days = int(_resolve_param("LABEL_DELAY_DAYS", 0 if self.local_mode else 7))

        # Spark config
        self.spark_executor_memory = _resolve_param("SPARK_EXECUTOR_MEMORY", self.data_rate_profile["spark_executor_memory"])
        self.spark_executor_instances = _resolve_param("SPARK_EXECUTOR_INSTANCES", self.data_rate_profile["spark_executor_instances"])
        self.spark_shuffle_partitions = _resolve_param("SPARK_SHUFFLE_PARTITIONS", self.data_rate_profile["spark_shuffle_partitions"])

        # Log config
        print(f"[config] DATA_RATE={self.data_rate} | TPS={self.min_tps}-{self.max_tps} | "
              f"customers={self.num_customers} | terminals={self.num_terminals}")

    def get_kafka_config(self) -> dict:
        """Return Kafka producer config. Uses SASL on GCP, plaintext locally."""
        base = {
            "bootstrap.servers": self.bootstrap_servers,
            "acks": "all",
            "linger.ms": 10,
            "batch.size": 65536,
            "compression.type": "lz4",
            "enable.idempotence": True,
        }

        if self.local_mode:
            return base

        # GCP Managed Kafka — SASL/PLAIN with static initial token (refreshed by producer)
        import google.auth
        from google.auth.transport.requests import Request

        token_credentials, _ = google.auth.default(
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        token_credentials.refresh(Request())

        base.update({
            "security.protocol": "SASL_SSL",
            "sasl.mechanism": "PLAIN",
            "sasl.username": self.kafka_sa_email,
            "sasl.password": token_credentials.token,
        })
        return base

    def get_bq_table_id(self) -> str:
        if self.local_mode:
            return self.bq_ground_truth_table
        return f"{self.project_id}.{self.bq_dataset}.{self.bq_ground_truth_table}"

    def load_simulation_state(self, state_path: str) -> dict:
        if os.path.exists(state_path):
            with open(state_path, "r") as f:
                return json.load(f)
        return {
            "transaction_counter": 0,
            "current_day": 0,
            "current_second": 0,
            "start_time": datetime.now(timezone.utc).isoformat(),
        }

    def save_simulation_state(self, state_path: str, state: dict):
        os.makedirs(os.path.dirname(state_path), exist_ok=True)
        with open(state_path, "w") as f:
            json.dump(state, f, indent=2)

    @staticmethod
    def print_profiles():
        """Print available data rate profiles."""
        print("\nAvailable DATA_RATE profiles:")
        print("-" * 60)
        for name, profile in DATA_RATE_PROFILES.items():
            print(f"  {name:10} | {profile['description']}")
            print(f"             | TPS: {profile['min_tps']}-{profile['max_tps']}")
            print(f"             | Customers: {profile['num_customers']}, Terminals: {profile['num_terminals']}")
            print(f"             | Spark: {profile['spark_executor_memory']} memory, {profile['spark_executor_instances']} executors")
        print("-" * 60)
