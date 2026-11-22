import json
import logging
import os
import sys
from time import perf_counter

import pandas as pd
from confluent_kafka import Consumer, Producer
from prometheus_client import start_http_server

sys.path.append(os.path.abspath("./src"))
from metrics import DURATION, ERRORS, FLAGGED, FRAUD_RATIO, PREPROCESSING, SCORED, SCORES
from preprocessing import run_preproc
from scorer import ensure_bootstrap, make_pred

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BROKERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TRANSACTIONS_TOPIC = os.getenv("KAFKA_TRANSACTIONS_TOPIC", "transactions")
SCORING_TOPIC = os.getenv("KAFKA_SCORING_TOPIC", "scoring")


def process_message(message, producer):
    started = perf_counter()
    transaction = json.loads(message.value())
    data = transaction["data"]
    with PREPROCESSING.time():
        features = run_preproc(pd.DataFrame([data]))
    submission, _, version = make_pred(features)
    result = submission.to_dict(orient="records")[0]
    result.update(
        transaction_id=transaction["transaction_id"],
        model_version=version,
        us_state=data.get("us_state"),
        merch=data.get("merch"),
        cat_id=data.get("cat_id"),
        amount=float(features["amount"].iat[0]),
    )

    delivery_errors = []

    def on_delivery(error, _message):
        if error is not None:
            delivery_errors.append(error)

    producer.produce(SCORING_TOPIC, value=json.dumps([result]), on_delivery=on_delivery)
    producer.flush()
    if delivery_errors:
        raise RuntimeError(f"Kafka: {delivery_errors[0]}")

    SCORED.inc()
    FLAGGED.inc(result["fraud_flag"])
    SCORES.observe(result["score"])
    DURATION.observe(perf_counter() - started)
    return result["fraud_flag"]


def main():
    ensure_bootstrap()
    start_http_server(int(os.getenv("METRICS_PORT", "8000")))
    consumer = Consumer({
        "bootstrap.servers": BROKERS,
        "group.id": "ml-scorer",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    producer = Producer({
        "bootstrap.servers": BROKERS,
        "enable.idempotence": True,
        "message.timeout.ms": 10000,
    })
    consumer.subscribe([TRANSACTIONS_TOPIC])
    processed = flagged = 0
    try:
        while True:
            message = consumer.poll(1)
            if message is None:
                continue
            if message.error():
                logger.warning("Kafka: %s", message.error())
                continue
            try:
                fraud_flag = process_message(message, producer)
            except (ValueError, TypeError, KeyError):
                ERRORS.inc()
                logger.exception("Некорректная транзакция")
            except Exception:
                ERRORS.inc()
                raise
            else:
                processed += 1
                flagged += fraud_flag
                FRAUD_RATIO.set(flagged / processed)
            consumer.commit(message=message, asynchronous=False)
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
