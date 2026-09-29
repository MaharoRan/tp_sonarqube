import json
import os
import shlex
import subprocess
import time
import uuid

import pytest

RUN_INTEGRATION = os.getenv("RUN_INTEGRATION_TESTS", "false").lower() == "true"

pytestmark = pytest.mark.skipif(
    not RUN_INTEGRATION,
    reason="Integration tests disabled. Set RUN_INTEGRATION_TESTS=true.",
)


def _find_processed_order(order_id):
    query = (
        "SELECT order_id, customer_id, product_id, quantity, unit_price, "
        "total_amount FROM processed_orders "
        f"WHERE order_id = '{order_id}';"
    )
    result = subprocess.run(
        [
            *shlex.split(os.getenv("COMPOSE_COMMAND", "docker compose")),
            "exec",
            "-T",
            "postgres",
            "psql",
            "-U",
            os.getenv("POSTGRES_USER", "sales"),
            "-d",
            os.getenv("POSTGRES_DB", "sales"),
            "-At",
            "-F",
            "\t",
            "-c",
            query,
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    rows = [line.split("\t") for line in result.stdout.splitlines() if line]
    return rows[0] if rows else None


def test_kafka_event_is_processed_by_spark_and_saved_to_postgres():
    from kafka import KafkaProducer

    order_id = f"INT-{uuid.uuid4().hex[:10].upper()}"
    event = {
        "order_id": order_id,
        "customer_id": "INT-CUSTOMER",
        "product_id": "P001",
        "quantity": 3,
        "unit_price": 25.50,
        "total_amount": 76.50,
        "timestamp": "2026-09-29T12:00:00+00:00",
    }
    producer = KafkaProducer(
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )
    processed_order = None

    try:
        producer.send(
            os.getenv("KAFKA_TOPIC", "sales.orders"),
            value=event,
        ).get(timeout=10)
        producer.flush()

        deadline = time.monotonic() + float(
            os.getenv("SPARK_TIMEOUT_SECONDS", "90")
        )
        while time.monotonic() < deadline and processed_order is None:
            processed_order = _find_processed_order(order_id)
            if processed_order is None:
                time.sleep(1)
    finally:
        producer.close()

    assert processed_order == [
        order_id,
        "INT-CUSTOMER",
        "P001",
        "3",
        "25.50",
        "76.50",
    ]