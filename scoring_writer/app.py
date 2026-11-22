import json
import logging
import os
from time import perf_counter

import psycopg2
from confluent_kafka import Consumer
from prometheus_client import Counter, Histogram, start_http_server

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

WRITTEN = Counter("fraud_written_total", "Scores committed to PostgreSQL")
FLAGGED = Counter("fraud_written_flagged_total", "Flagged scores committed to PostgreSQL")
DURATION = Histogram("fraud_write_seconds", "PostgreSQL insert and commit time")
SCORES = Histogram(
    "fraud_written_score", "Scores committed to PostgreSQL",
    buckets=tuple(i / 20 for i in range(21)),
)


def get_db_config():
    return {
        "host": os.getenv("POSTGRES_HOST", "postgres"),
        "port": os.getenv("POSTGRES_PORT", "5432"),
        "dbname": os.environ["POSTGRES_DB"],
        "user": os.environ["POSTGRES_USER"],
        "password": os.environ["POSTGRES_PASSWORD"],
    }


def insert_score(conn, data):
    started = perf_counter()
    with conn, conn.cursor() as cur:
        cur.execute("""
            INSERT INTO scores (
                transaction_id, score, fraud_flag, model_version,
                us_state, merch, cat_id, amount
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (transaction_id) DO NOTHING
        """, (
            data["transaction_id"], data["score"], data["fraud_flag"],
            data.get("model_version"), data.get("us_state"), data.get("merch"),
            data.get("cat_id"), data.get("amount"),
        ))
        inserted = cur.rowcount
    if inserted:
        WRITTEN.inc()
        FLAGGED.inc(data["fraud_flag"])
        SCORES.observe(data["score"])
        DURATION.observe(perf_counter() - started)


def main():
    start_http_server(int(os.getenv("METRICS_PORT", "8001")))
    consumer = Consumer({
        "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
        "group.id": "scoring-writer",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([os.getenv("KAFKA_SCORING_TOPIC", "scoring")])
    conn = psycopg2.connect(**get_db_config())
    try:
        while True:
            msg = consumer.poll(1)
            if msg is None:
                continue
            if msg.error():
                logger.warning("Kafka: %s", msg.error())
                continue

            try:
                scores = json.loads(msg.value())
                if not isinstance(scores, list):
                    raise ValueError("Ожидался список скоров")
                for score in scores:
                    insert_score(conn, score)
            except (ValueError, KeyError, TypeError):
                logger.exception("Некорректный результат скоринга")

            consumer.commit(message=msg, asynchronous=False)
    finally:
        consumer.close()
        conn.close()


if __name__ == "__main__":
    main()
