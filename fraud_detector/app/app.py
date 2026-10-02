import json
import logging
import os
import sys

import pandas as pd
from confluent_kafka import Consumer, Producer

sys.path.append(os.path.abspath("./src"))

from features import prepare_features
from scorer import make_pred

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("/app/logs/service.log"),
        logging.StreamHandler(),
    ],
)

logger = logging.getLogger(__name__)


KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "kafka:9092",
)

TRANSACTIONS_TOPIC = os.getenv(
    "KAFKA_TRANSACTIONS_TOPIC",
    "transactions",
)

SCORING_TOPIC = os.getenv(
    "KAFKA_SCORING_TOPIC",
    "scoring",
)


class ProcessingService:
    def __init__(self):
        consumer_config = {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": "ml-scorer",
            "auto.offset.reset": "earliest",
        }

        producer_config = {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        }

        self.consumer = Consumer(consumer_config)
        self.consumer.subscribe([TRANSACTIONS_TOPIC])

        self.producer = Producer(producer_config)

        logger.info(
            "Subscribed to Kafka topic: %s",
            TRANSACTIONS_TOPIC,
        )

    def process_messages(self):
        while True:
            message = self.consumer.poll(1.0)

            if message is None:
                continue

            if message.error():
                logger.error(
                    "Kafka error: %s",
                    message.error(),
                )
                continue

            try:
                message_data = json.loads(
                    message.value().decode("utf-8")
                )

                transaction_id = message_data["transaction_id"]
                transaction = message_data["data"]

                input_data = pd.DataFrame([transaction])
                prepared_data = prepare_features(input_data)
                prediction = make_pred(prepared_data)

                result = {
                    "transaction_id": str(transaction_id),
                    "score": float(prediction.loc[0, "score"]),
                    "fraud_flag": int(
                        prediction.loc[0, "fraud_flag"]
                    ),
                }

                self.producer.produce(
                    SCORING_TOPIC,
                    key=str(transaction_id),
                    value=json.dumps(result).encode("utf-8"),
                )

                self.producer.flush()

                logger.info(
                    "Transaction scored successfully: %s",
                    result,
                )

            except Exception:
                logger.exception(
                    "Error while processing Kafka message"
                )

    def close(self):
        self.producer.flush()
        self.consumer.close()


if __name__ == "__main__":
    logger.info("Starting Kafka ML scoring service...")

    service = ProcessingService()

    try:
        service.process_messages()
    except KeyboardInterrupt:
        logger.info("Service stopped by user")
    finally:
        service.close()