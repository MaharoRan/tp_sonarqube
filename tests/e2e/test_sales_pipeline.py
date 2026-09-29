import os
import shlex
import subprocess
import time

import httpx
import pytest

RUN_E2E = os.getenv("RUN_E2E_TESTS", "false").lower() == "true"

pytestmark = pytest.mark.skipif(
    not RUN_E2E,
    reason="E2E tests disabled. Set RUN_E2E_TESTS=true."
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


def test_kafka_spark_postgres_pipeline():
    api_url = os.getenv("API_BASE_URL", "http://localhost:8000")
    order = {
        "customer_id": "E2E-CUSTOMER",
        "product_id": "P001",
        "quantity": 3,
        "unit_price": 100.0,
    }

    with httpx.Client(base_url=api_url, timeout=20.0) as client:
        response = client.post("/api/orders", json=order)

    assert response.status_code == 201
    event = response.json()
    order_id = event["order_id"]
    assert event["total_amount"] == 300.0

    deadline = time.monotonic() + float(os.getenv("E2E_TIMEOUT_SECONDS", "60"))
    processed_order = None
    while time.monotonic() < deadline:
        processed_order = _find_processed_order(order_id)
        if processed_order is not None:
            break
        time.sleep(0.5)

    assert processed_order is not None
    assert processed_order == [
        order_id,
        "E2E-CUSTOMER",
        "P001",
        "3",
        "100.00",
        "300.00",
    ]
