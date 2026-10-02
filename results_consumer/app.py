import json
import logging
import os
import time

import psycopg2
from confluent_kafka import Consumer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)

logger = logging.getLogger(__name__)


KAFKA_BOOTSTRAP_SERVERS = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "kafka:9092",
)

KAFKA_SCORES_TOPIC = os.getenv(
    "KAFKA_SCORES_TOPIC",
    "scores",
)

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "fraud_db")
POSTGRES_USER = os.getenv("POSTGRES_USER", "fraud_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "fraud_password")


def connect_to_postgres():
    """Подключается к PostgreSQL с повторными попытками."""

    while True:
        try:
            connection = psycopg2.connect(
                host=POSTGRES_HOST,
                port=POSTGRES_PORT,
                database=POSTGRES_DB,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD,
            )

            logger.info("Connected to PostgreSQL")
            return connection

        except psycopg2.OperationalError as error:
            logger.warning(
                "PostgreSQL is not ready: %s",
                error,
            )
            time.sleep(3)


def main():
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": "scores-to-postgres",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })

    consumer.subscribe([KAFKA_SCORES_TOPIC])
    connection = connect_to_postgres()

    logger.info(
        "Subscribed to Kafka topic: %s",
        KAFKA_SCORES_TOPIC,
    )

    try:
        while True:
            message = consumer.poll(1.0)

            if message is None:
                continue

            if message.error():
                logger.error(
                    "Kafka error: %s",
                    message.error(),
                )
                continue

            try:
                result = json.loads(
                    message.value().decode("utf-8")
                )

                with connection.cursor() as cursor:
                    cursor.execute(
                        """
                        INSERT INTO transaction_scores (
                            transaction_id,
                            score,
                            fraud_flag
                        )
                        VALUES (%s, %s, %s)
                        ON CONFLICT (transaction_id)
                        DO UPDATE SET
                            score = EXCLUDED.score,
                            fraud_flag = EXCLUDED.fraud_flag,
                            created_at = CURRENT_TIMESTAMP
                        """,
                        (
                            result["transaction_id"],
                            result["score"],
                            result["fraud_flag"],
                        ),
                    )

                connection.commit()

                consumer.commit(
                    message=message,
                    asynchronous=False,
                )

                logger.info(
                    "Saved transaction %s to PostgreSQL",
                    result["transaction_id"],
                )

            except Exception:
                connection.rollback()
                logger.exception(
                    "Failed to save scoring result"
                )

    except KeyboardInterrupt:
        logger.info("Results consumer stopped")

    finally:
        consumer.close()
        connection.close()


if __name__ == "__main__":
    main()