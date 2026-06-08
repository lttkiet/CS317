import logging
import time
from datetime import datetime, timedelta, timezone

import pandas as pd

logger = logging.getLogger(__name__)


class BigQuerySource:
    def __init__(self, config):
        from google.cloud import bigquery
        self.client = bigquery.Client(project=config.project_id)
        self.config = config

    def _query(self, sql: str) -> pd.DataFrame:
        df = self.client.query(sql).result().to_dataframe()
        df.columns = [c.lower() for c in df.columns]
        return df

    @property
    def _full_table(self) -> str:
        return f"{self.config.project_id}.{self.config.bq_dataset}.{self.config.bq_predictions_table}"

    @property
    def _model_table(self) -> str:
        return f"{self.config.project_id}.{self.config.bq_dataset}.{self.config.bq_model_versions_table}"

    def get_latest_predictions(self, limit: int = 100) -> pd.DataFrame:
        sql = f"""
            SELECT *
            FROM `{self._full_table}`
            ORDER BY predicted_at DESC
            LIMIT {limit}
        """
        return self._query(sql)

    def search_transactions(
        self,
        customer_id: int | None = None,
        terminal_id: int | None = None,
        tx_id: int | None = None,
        hours: int = 24,
    ) -> pd.DataFrame:
        conditions = []
        if customer_id is not None:
            conditions.append(f"customer_id = {customer_id}")
        if terminal_id is not None:
            conditions.append(f"terminal_id = {terminal_id}")
        if tx_id is not None:
            conditions.append(f"transaction_id = {tx_id}")
        if not conditions:
            conditions.append(f"predicted_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {hours} HOUR)")
        where = " AND ".join(conditions)
        sql = f"""
            SELECT *
            FROM `{self._full_table}`
            WHERE {where}
            ORDER BY predicted_at DESC
            LIMIT 500
        """
        return self._query(sql)

    def get_fraud_rate_trend(self, interval: str = "hour", hours: int = 24) -> pd.DataFrame:
        sql = f"""
            SELECT
                TIMESTAMP_TRUNC(predicted_at, {interval.upper()}) AS bucket,
                COUNT(*) AS total,
                SUM(is_fraud_predicted) AS fraud_count,
                ROUND(SAFE_DIVIDE(SUM(is_fraud_predicted), COUNT(*)) * 100, 2) AS fraud_rate
            FROM `{self._full_table}`
            WHERE predicted_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {hours} HOUR)
            GROUP BY bucket
            ORDER BY bucket
        """
        return self._query(sql)

    def get_probability_distribution(self, hours: int = 24, buckets: int = 20) -> pd.DataFrame:
        sql = f"""
            SELECT
                ROUND(fraud_probability * {buckets}) / {buckets} AS prob_bucket,
                COUNT(*) AS count
            FROM `{self._full_table}`
            WHERE predicted_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {hours} HOUR)
            GROUP BY prob_bucket
            ORDER BY prob_bucket
        """
        return self._query(sql)

    def get_top_terminals(self, limit: int = 10, hours: int = 24) -> pd.DataFrame:
        sql = f"""
            SELECT
                terminal_id,
                COUNT(*) AS total,
                SUM(is_fraud_predicted) AS fraud_count,
                ROUND(AVG(fraud_probability) * 100, 2) AS avg_probability
            FROM `{self._full_table}`
            WHERE predicted_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {hours} HOUR)
            GROUP BY terminal_id
            ORDER BY fraud_count DESC
            LIMIT {limit}
        """
        return self._query(sql)

    def get_total_count(self) -> int:
        sql = f"SELECT COUNT(*) AS cnt FROM `{self._full_table}`"
        df = self._query(sql)
        return int(df["cnt"].iloc[0]) if not df.empty else 0

    def get_fraud_count(self, hours: int | None = None) -> int:
        if hours:
            sql = f"""
                SELECT COUNT(*) AS cnt FROM `{self._full_table}`
                WHERE is_fraud_predicted = 1
                AND predicted_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {hours} HOUR)
            """
        else:
            sql = f"SELECT COUNT(*) AS cnt FROM `{self._full_table}` WHERE is_fraud_predicted = 1"
        df = self._query(sql)
        return int(df["cnt"].iloc[0]) if not df.empty else 0

    def get_last_update_time(self) -> datetime | None:
        sql = f"SELECT MAX(predicted_at) AS last FROM `{self._full_table}`"
        df = self._query(sql)
        return df["last"].iloc[0] if not df.empty and not df["last"].isna().iloc[0] else None

    def get_model_versions(self) -> pd.DataFrame:
        sql = f"""
            SELECT *
            FROM `{self._model_table}`
            ORDER BY version DESC
            LIMIT 20
        """
        return self._query(sql)

    def wait_for_transaction(self, tx_id: int, timeout: int = 60, interval: int = 3) -> pd.DataFrame | None:
        deadline = time.time() + timeout
        while time.time() < deadline:
            df = self.search_transactions(tx_id=tx_id)
            if not df.empty:
                return df
            time.sleep(interval)
        return None


class PostgresSource:
    def __init__(self, config):
        from sqlalchemy import create_engine
        conn_str = f"postgresql://{config.pg_user}:{config.pg_password}@{config.pg_host}:{config.pg_port}/{config.pg_db}"
        self.engine = create_engine(conn_str)
        self.config = config

    def _query(self, sql: str) -> pd.DataFrame:
        return pd.read_sql(sql, self.engine)

    def get_latest_predictions(self, limit: int = 100) -> pd.DataFrame:
        sql = f"""
            SELECT *
            FROM streaming_predictions
            ORDER BY predicted_at DESC
            LIMIT {limit}
        """
        return self._query(sql)

    def search_transactions(
        self,
        customer_id: int | None = None,
        terminal_id: int | None = None,
        tx_id: int | None = None,
        hours: int = 24,
    ) -> pd.DataFrame:
        conditions = []
        if customer_id is not None:
            conditions.append(f"customer_id = {customer_id}")
        if terminal_id is not None:
            conditions.append(f"terminal_id = {terminal_id}")
        if tx_id is not None:
            conditions.append(f"transaction_id = {tx_id}")
        if not conditions:
            conditions.append(f"predicted_at >= NOW() - INTERVAL '{hours} hours'")
        where = " AND ".join(conditions)
        sql = f"""
            SELECT *
            FROM streaming_predictions
            WHERE {where}
            ORDER BY predicted_at DESC
            LIMIT 500
        """
        return self._query(sql)

    def get_fraud_rate_trend(self, interval: str = "hour", hours: int = 24) -> pd.DataFrame:
        trunc = "hour" if interval.lower() == "hour" else "day"
        sql = f"""
            SELECT
                DATE_TRUNC('{trunc}', predicted_at) AS bucket,
                COUNT(*) AS total,
                SUM(is_fraud_predicted::int) AS fraud_count,
                ROUND(CAST(SUM(is_fraud_predicted::int) AS numeric) / NULLIF(COUNT(*), 0) * 100, 2) AS fraud_rate
            FROM streaming_predictions
            WHERE predicted_at >= NOW() - INTERVAL '{hours} hours'
            GROUP BY bucket
            ORDER BY bucket
        """
        return self._query(sql)

    def get_probability_distribution(self, hours: int = 24, buckets: int = 20) -> pd.DataFrame:
        sql = f"""
            SELECT
                ROUND(fraud_probability * {buckets}) / {buckets} AS prob_bucket,
                COUNT(*) AS count
            FROM streaming_predictions
            WHERE predicted_at >= NOW() - INTERVAL '{hours} hours'
            GROUP BY prob_bucket
            ORDER BY prob_bucket
        """
        return self._query(sql)

    def get_top_terminals(self, limit: int = 10, hours: int = 24) -> pd.DataFrame:
        sql = f"""
            SELECT
                terminal_id,
                COUNT(*) AS total,
                SUM(is_fraud_predicted::int) AS fraud_count,
                ROUND(AVG(fraud_probability) * 100, 2) AS avg_probability
            FROM streaming_predictions
            WHERE predicted_at >= NOW() - INTERVAL '{hours} hours'
            GROUP BY terminal_id
            ORDER BY fraud_count DESC
            LIMIT {limit}
        """
        return self._query(sql)

    def get_total_count(self) -> int:
        sql = "SELECT COUNT(*) AS cnt FROM streaming_predictions"
        df = self._query(sql)
        return int(df["cnt"].iloc[0]) if not df.empty else 0

    def get_fraud_count(self, hours: int | None = None) -> int:
        if hours:
            sql = f"""
                SELECT COUNT(*) AS cnt FROM streaming_predictions
                WHERE is_fraud_predicted = 1
                AND predicted_at >= NOW() - INTERVAL '{hours} hours'
            """
        else:
            sql = "SELECT COUNT(*) AS cnt FROM streaming_predictions WHERE is_fraud_predicted = 1"
        df = self._query(sql)
        return int(df["cnt"].iloc[0]) if not df.empty else 0

    def get_last_update_time(self) -> datetime | None:
        sql = "SELECT MAX(predicted_at) AS last FROM streaming_predictions"
        df = self._query(sql)
        return df["last"].iloc[0] if not df.empty and pd.notna(df["last"].iloc[0]) else None

    def get_model_versions(self) -> pd.DataFrame:
        sql = """
            SELECT *
            FROM model_versions
            ORDER BY version DESC
            LIMIT 20
        """
        return self._query(sql)

    def wait_for_transaction(self, tx_id: int, timeout: int = 60, interval: int = 3) -> pd.DataFrame | None:
        deadline = time.time() + timeout
        while time.time() < deadline:
            df = self.search_transactions(tx_id=tx_id)
            if not df.empty:
                return df
            time.sleep(interval)
        return None


def create_datasource(config):
    logger.info(f"Creating DataSource for mode: {config.mode}")
    if config.mode == "gcp":
        return BigQuerySource(config)
    return PostgresSource(config)
