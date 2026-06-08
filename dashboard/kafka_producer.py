import json
import logging

from confluent_kafka import Producer as ConfluentProducer

logger = logging.getLogger(__name__)


class DashboardKafkaProducer:
    def __init__(self, config):
        self._producer = ConfluentProducer(config.kafka_config)
        self._topic = config.kafka_topic

    def produce_transaction(self, tx_data: dict) -> None:
        payload = {
            "TRANSACTION_ID": tx_data["TRANSACTION_ID"],
            "TX_DATETIME": tx_data["TX_DATETIME"],
            "CUSTOMER_ID": tx_data["CUSTOMER_ID"],
            "TERMINAL_ID": tx_data["TERMINAL_ID"],
            "TX_AMOUNT": tx_data["TX_AMOUNT"],
            "TX_TIME_SECONDS": tx_data["TX_TIME_SECONDS"],
            "TX_TIME_DAYS": tx_data["TX_TIME_DAYS"],
        }
        key = str(tx_data["TRANSACTION_ID"]).encode("utf-8")
        value = json.dumps(payload).encode("utf-8")
        self._producer.produce(self._topic, key=key, value=value)
        self._producer.flush(timeout=5)
        logger.info(f"Produced transaction {tx_data['TRANSACTION_ID']} to {self._topic}")
