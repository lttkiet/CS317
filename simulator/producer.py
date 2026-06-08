import json
import logging
import threading
import time

from confluent_kafka import Producer as ConfluentProducer

from simulator.config import Config

logger = logging.getLogger(__name__)

_REFRESH_INTERVAL_SEC = 3300  # 55 min (tokens expire after 60 min)


def _get_fresh_token(sa_email: str) -> tuple[str, str]:
    import google.auth
    from google.auth.transport.requests import Request
    creds, _ = google.auth.default(
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    creds.refresh(Request())
    return sa_email, creds.token


class KafkaProducer:
    """Wrapper around confluent-kafka Producer for transaction publishing.

    On GCP, periodically refreshes the SASL access token before expiry.
    """

    def __init__(self, config: Config):
        self.config = config
        self._conf = config.get_kafka_config()
        self._producer = ConfluentProducer(self._conf)
        self._topic = config.kafka_topic

        # Start token refresh thread if using SASL on GCP
        self._token_refresher = None
        if not config.local_mode:
            self._stop_refresh = threading.Event()
            self._token_refresher = threading.Thread(
                target=self._refresh_loop, daemon=True
            )
            self._token_refresher.start()

    def _refresh_loop(self):
        """Periodically refresh SASL token before expiry."""
        while not self._stop_refresh.is_set():
            if self._stop_refresh.wait(timeout=_REFRESH_INTERVAL_SEC):
                break
            try:
                sa_email, token = _get_fresh_token(self.config.kafka_sa_email)
                self._producer.set_sasl_credentials(sa_email, token)
                logger.info("SASL token refreshed successfully")
            except Exception as e:
                logger.warning(f"SASL token refresh failed: {e}")

    def produce_batch(self, transactions: list[dict], topic: str | None = None):
        """Produce a batch of transactions to Kafka.

        Each message key is TRANSACTION_ID. Fraud labels are stripped
        from the payload — only the 7 real-time fields are published.
        """
        target_topic = topic or self._topic

        for tx in transactions:
            payload = {
                "TRANSACTION_ID": tx["TRANSACTION_ID"],
                "TX_DATETIME": tx["TX_DATETIME"],
                "CUSTOMER_ID": tx["CUSTOMER_ID"],
                "TERMINAL_ID": tx["TERMINAL_ID"],
                "TX_AMOUNT": tx["TX_AMOUNT"],
                "TX_TIME_SECONDS": tx["TX_TIME_SECONDS"],
                "TX_TIME_DAYS": tx["TX_TIME_DAYS"],
            }

            key = str(tx["TRANSACTION_ID"]).encode("utf-8")
            value = json.dumps(payload).encode("utf-8")

            try:
                self._producer.produce(
                    target_topic,
                    key=key,
                    value=value,
                    callback=self._delivery_callback,
                )
            except Exception as e:
                logger.error(f"Failed to produce message {tx['TRANSACTION_ID']}: {e}")

        # Flush is intentionally NOT called per-batch to preserve
        # linger.ms batching efficiency. The delivery callback handles
        # async error detection. Full flush occurs on close().

    @staticmethod
    def _delivery_callback(err, msg):
        if err:
            logger.error(f"Message delivery failed: {err}")
        else:
            logger.debug(
                f"Delivered to {msg.topic()}[{msg.partition()}] @ offset {msg.offset()}"
            )

    def close(self):
        if self._token_refresher is not None:
            self._stop_refresh.set()
            self._token_refresher.join(timeout=5)
        self._producer.flush(timeout=10)
