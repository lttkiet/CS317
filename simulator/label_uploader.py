import logging
from datetime import datetime, timedelta, timezone

from simulator.config import Config

logger = logging.getLogger(__name__)


class LabelUploader:
    """Uploads ground-truth fraud labels to storage with simulated delay.

    In GCP mode: uses BigQuery client.
    In local mode: uses PostgreSQL via psycopg2.
    """

    def __init__(self, config: Config):
        self.config = config
        self._table_id = config.get_bq_table_id()
        self._delay_days = config.label_delay_days

        if config.local_mode:
            import psycopg2
            import psycopg2.extras
            self._conn = psycopg2.connect(
                host=config.pg_host,
                port=config.pg_port,
                dbname=config.pg_db,
                user=config.pg_user,
                password=config.pg_password,
            )
        else:
            from google.cloud import bigquery
            self._bq_client = bigquery.Client(project=config.project_id)

    def upload_labels(self, transactions: list[dict], current_time: datetime):
        """Upload fraud labels for transactions older than the delay window."""
        if self._delay_days > 0:
            cutoff = current_time - timedelta(days=self._delay_days)
        else:
            cutoff = None  # no delay → upload immediately

        rows_to_insert = []
        for tx in transactions:
            if cutoff is not None:
                tx_time = datetime.fromisoformat(
                    tx["TX_DATETIME"].replace("Z", "+00:00")
                )
                if tx_time > cutoff:
                    continue
            label = tx["_fraud_label"]
            rows_to_insert.append({
                    "TRANSACTION_ID": tx["TRANSACTION_ID"],
                    "TX_FRAUD": label["TX_FRAUD"],
                    "TX_FRAUD_SCENARIO": label["TX_FRAUD_SCENARIO"],
                    "LABEL_RECEIVED_AT": current_time.isoformat(),
                })

        if not rows_to_insert:
            return 0

        if self.config.local_mode:
            return self._insert_postgres(rows_to_insert)
        else:
            return self._insert_bigquery(rows_to_insert)

    def _insert_postgres(self, rows: list[dict]) -> int:
        import psycopg2.extras
        cur = self._conn.cursor()
        cols = list(rows[0].keys())
        placeholders = ", ".join(["%s"] * len(cols))
        col_names = ", ".join([c.lower() for c in cols])
        sql = f"INSERT INTO ground_truth_labels ({col_names}) VALUES ({placeholders})"
        values = [tuple(row[c] for c in cols) for row in rows]
        psycopg2.extras.execute_batch(cur, sql, values, page_size=500)
        self._conn.commit()
        cur.close()
        logger.info(f"Uploaded {len(rows)} labels to PostgreSQL")
        return len(rows)

    def _insert_bigquery(self, rows: list[dict]) -> int:
        errors = self._bq_client.insert_rows_json(
            self._table_id,
            rows,
            row_ids=[None] * len(rows),
        )
        if errors:
            logger.error(f"BigQuery insert errors: {errors}")
            raise RuntimeError(f"Failed to insert {len(errors)} rows")
        logger.info(f"Uploaded {len(rows)} labels to BigQuery")
        return len(rows)

    def close(self):
        if self.config.local_mode and hasattr(self, "_conn"):
            self._conn.close()
